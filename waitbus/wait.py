"""``waitbus wait`` -- block until any source event matches a predicate.

A thin, exit-coded adapter over the shared ``await_predicate`` egress
engine (``_broadcast_sub``) plus the source-agnostic predicate engine
(``_predicate``). The contract is dumb and script-friendly
(the ripgrep / fzf model -- one composed predicate, one overall
deadline, exit code carries the verdict):

    waitbus wait --sha <SHA> [--repo owner/repo] [--timeout 30s] [--since <cursor>]
    waitbus wait --source <s> --match <dotted.key>=<json_literal>... [--timeout 30s]
    waitbus wait --cond <named-condition> [--timeout 30s]
    waitbus wait --all-of '<source>:<key>=<json>' --all-of '<source>:<key>=<json>' ...
    waitbus wait --first-of '<source>:<key>=<json>' --first-of '<source>:<key>=<json>' ...

Three input shapes, one canonical decision path internally: all reduce
to an AND-composed :class:`waitbus._predicate.Predicate`.
``--sha X`` is sugar for ``--source github`` plus a git-style PREFIX
match on ``fields.head_sha`` (X must be >=7 hex chars): GitHub stores the
full 40-char SHA, but the 7-char abbreviation people actually type
resolves, mirroring ``git show <prefix>``. The generic
``--match fields.head_sha=`` path stays an exact match. The
universal-egress capability the public copy already promises ("wait on
anything -- CI, tests, builds, deploys") is real in code.

Exit codes:

    0    matched (any source) AND, for GitHub frames, conclusion is
         ``success``; for non-GitHub frames any match is exit 0
    1    matched a GitHub frame whose conclusion is terminal
         ``failure`` / ``cancelled`` / ``timed_out``
    124  overall ``--timeout`` elapsed with no match (coreutils
         ``timeout`` convention)
    130  SIGINT (Ctrl-C) -- clean socket/cursor teardown, no
         spurious match (128 + SIGINT(2))
    2    startup failure (daemon down, token required, bad --repo,
         malformed --match, evaluator extra not installed, no
         predicate supplied)

For GitHub matches, ``skipped`` / ``neutral`` / ``action_required`` /
``stale`` are non-terminal and the wait keeps streaming. There are
no ``--treat-<x>=`` override knobs (closed bucketing
table; add only behind a stated real-consumer trigger).

Cross-source composition (``--all-of`` / ``--first-of``) takes repeatable
``source:key=json_literal`` clauses -- each clause is the existing
per-source predicate grammar scoped to that source. ``--all-of`` is a
STICKY conjunction over the event stream: clauses may be satisfied by
different events over time, a clause once satisfied stays satisfied, and
the wait returns when the last outstanding clause matches. ``--first-of``
is a single-event disjunction: return on the first event matching any
clause. Composition mode does NOT apply the GitHub conclusion-bucketing
ladder (a github clause states its terminal condition explicitly) and
``--exit-status`` has no effect in it; a satisfied composition exits 0.

Layer-2 expressive predicates (``--match-cel``, ``--match-jmespath``)
route through :func:`waitbus._predicate.resolve_evaluator`; the
relevant evaluator is provided by an opt-in extras package and is NOT
in the MIT core. Without the extras installed, the flag raises a
startup-time error pointing at the install command.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Callable
from typing import Annotated, Any, NoReturn

import typer

from . import _compose, _predicate
from ._broadcast_sub import (
    BroadcastConnectionError,
    FrameDecision,
    WaitOutcome,
    await_predicate,
    open_subscriber,
)
from ._duration import parse_duration
from ._secrets import SecretNotConfigured
from ._terminal import FAILURE_CONCLUSIONS, SUCCESS_CONCLUSION
from .cli._shared import _exit_with_error, run_typer_app
from .read_events import detect_repo

# coreutils `timeout` uses 124 for "the command timed out"; the shell
# reports a signal-killed child as 128+signum, so SIGINT(2) -> 130.
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
EXIT_STARTUP = 2
EXIT_TIMEOUT = 124
EXIT_SIGINT = 130

_app = typer.Typer(
    name="wait",
    help="Block until any source's event matches a predicate.",
    no_args_is_help=False,
    add_completion=False,
)


def _exit_code_if_terminal(conclusion: str | None) -> int | None:
    """Map a GitHub ``conclusion`` to an exit code, or ``None`` if the
    conclusion is non-terminal (the wait must keep going).

    The buckets come from ``_terminal`` -- the single source of truth
    shared with ``AGG_SQL``; this function does NOT introduce a parallel
    map. A missing / empty / not-yet-set conclusion is non-terminal (the
    run is still in flight).
    """
    if not conclusion:
        return None
    if conclusion == SUCCESS_CONCLUSION:
        return EXIT_SUCCESS
    if conclusion in FAILURE_CONCLUSIONS:
        return EXIT_FAILURE
    # skipped / neutral / action_required / stale -> keep waiting.
    return None


def _build_predicate(
    *,
    sha: str | None,
    match: list[str] | None,
    cond: list[str] | None,
    match_cel: str | None,
    match_jmespath: str | None,
    source: str | None,
    verb: str = "wait",
) -> tuple[_predicate.Predicate, str | None]:
    """Compose every CLI predicate input into one AND-Predicate.

    ``--sha X`` is sugar for ``--source github`` plus a git-style PREFIX
    match on ``fields.head_sha`` (lowered to a dedicated prefix predicate,
    NOT an exact ``fields.head_sha=`` spec, so the 7-char SHAs people type
    resolve against the full SHA GitHub stores). Returns the composed
    predicate plus the effective source (set explicitly or inferred from
    --sha). Exits at startup (code 2) on any predicate-shape error --
    garbage in is never a silent never-matches.

    ``verb`` names the calling command in the "requires at least one
    predicate" startup error, so the wait-family verbs (``wait``, ``on``)
    that share this builder each surface their own name.
    """
    match_specs: list[str] = list(match or [])
    effective_source = source
    if sha is not None:
        if not sha.strip():
            _exit_with_error("--sha must be non-empty", code=EXIT_STARTUP)
        if effective_source is None:
            effective_source = "github"
        elif effective_source != "github":
            _exit_with_error(
                f"--sha implies --source github; got --source {effective_source!r}",
                hint="Drop --sha or pass --source github.",
                code=EXIT_STARTUP,
            )
        # NB: --sha is lowered to a git-style prefix predicate below (inside
        # the try, so its hex/length ValueError maps to exit-2), not an exact
        # fields.head_sha= match_spec.

    # --source narrows the match table too -- consistent with --match (a
    # predicate, not just a daemon subscription filter); makes `--source X`
    # match the user's mental model regardless of how the daemon multiplexes
    # sources today.
    if effective_source is not None:
        match_specs.append(f"fields.source={json.dumps(effective_source)}")

    cond_names = list(cond or [])
    if sha is None and not match_specs and not cond_names and match_cel is None and match_jmespath is None:
        _exit_with_error(
            f"waitbus {verb} requires at least one of --sha / --match / --cond / --match-cel / --match-jmespath",
            hint=(f"Example: `waitbus {verb} --source docker --match 'fields.action=\"die\"' --timeout 30s`."),
            code=EXIT_STARTUP,
        )

    try:
        predicates: list[_predicate.Predicate] = [_predicate.parse_match(match_specs)]
        if sha is not None:
            predicates.append(_predicate.head_sha_prefix(sha))
        for name in cond_names:
            predicates.append(_predicate.resolve_condition(name))
        if match_cel is not None:
            predicates.append(_predicate.resolve_evaluator("cel", match_cel))
        if match_jmespath is not None:
            predicates.append(_predicate.resolve_evaluator("jmespath", match_jmespath))
    except (ValueError, KeyError, _predicate.EvaluatorUnavailableError) as exc:
        # KeyError.__str__ wraps in extra quotes; .args[0] is the raw message.
        msg = exc.args[0] if isinstance(exc, KeyError) else str(exc)
        _exit_with_error(msg, code=EXIT_STARTUP)

    return _predicate.compose(*predicates), effective_source


def _resolve_repo(
    *,
    repo: str | None,
    effective_source: str | None,
    sha: str | None,
) -> str | None:
    """Pick the subscriber's ``filters=[owner/repo]`` or None for wildcard.

    GitHub paths keep the existing ``detect_repo()`` contract: the
    helpful exit-2 surfaces when an operator typed ``waitbus wait --sha X``
    in a directory whose origin is not github. Non-GitHub sources skip
    ``detect_repo()`` entirely (third leg of the historical
    GitHub-only triple lockout) and subscribe with no repo filter --
    the composed predicate is the matcher.
    """
    is_github_path = effective_source in (None, "github")
    if repo is not None:
        if not is_github_path:
            print(
                f"note: --repo {repo!r} ignored for --source {effective_source!r} (repo filter is GitHub-only)",
                file=sys.stderr,
                flush=True,
            )
            return None
        if repo.count("/") != 1 or not all(repo.split("/")):
            _exit_with_error(
                f"--repo must be owner/repo, got {repo!r}",
                code=EXIT_STARTUP,
            )
        return repo
    if is_github_path and (sha is not None or effective_source == "github"):
        detected = detect_repo()
        if detected is None:
            _exit_with_error(
                "--repo not given and the current directory's git origin is not a github.com remote",
                hint=("Pass --repo owner/repo, or run inside a clone whose origin is github.com/<owner>/<repo>."),
                code=EXIT_STARTUP,
            )
        return f"{detected[0]}/{detected[1]}"
    # Universal subscribe: source is non-GitHub or unspecified, no --repo
    # given, no --sha to require a GitHub remote.
    return None


def _build_decide(
    composed: _predicate.Predicate,
    matched_exit: dict[str, int],
    matched_source: dict[str, str | None],
) -> Callable[[dict[str, Any]], FrameDecision]:
    """Wrap the bool Predicate in the FrameDecision ladder.

    The composed predicate decides match-or-not; this closure adds the
    source-aware exit-code mapping (GitHub: conclusion-bucket; non-
    GitHub: any-match-is-exit-0) and captures the result for the caller
    to map to ``typer.Exit`` after ``await_predicate`` returns.
    """

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        fields = frame.get("fields")
        if not isinstance(fields, dict):
            return FrameDecision.CONTINUE
        if not composed(frame):
            return FrameDecision.CONTINUE
        frame_source = fields.get("source")
        if frame_source == "github":
            code = _exit_code_if_terminal(fields.get("conclusion"))
            if code is None:
                # Non-terminal GitHub conclusion for our matcher: keep waiting.
                return FrameDecision.CONTINUE
            matched_exit["code"] = code
        else:
            # Non-GitHub source: no conclusion vocabulary; any match exits 0.
            matched_exit["code"] = EXIT_SUCCESS
        matched_source["source"] = frame_source if isinstance(frame_source, str) else None
        return FrameDecision.MATCHED

    return _decide


def _print_matched(
    *,
    sha: str | None,
    effective_repo: str | None,
    matched_source: dict[str, str | None],
    code: int,
) -> None:
    """Stderr-print the human-readable verdict for the matched outcome."""
    if matched_source.get("source") == "github":
        verdict = "success" if code == EXIT_SUCCESS else "failure"
        scope = effective_repo or "*"
        label = f"{sha} on {scope}" if sha is not None else f"match on {scope}"
        print(f"{label}: {verdict}", file=sys.stderr, flush=True)
    else:
        src = matched_source.get("source") or "(unknown)"
        print(f"matched on source={src}", file=sys.stderr, flush=True)


def _dispatch_outcome(
    *,
    outcome: WaitOutcome,
    timeout: str,
    composed_source: str,
    effective_repo: str | None,
    effective_source: str | None,
    sha: str | None,
    matched_exit: dict[str, int],
    matched_source: dict[str, str | None],
    exit_status: bool,
) -> None:
    """Translate the engine's :class:`WaitOutcome` into a ``typer.Exit``."""
    if outcome.cancelled:
        # SIGINT: clean teardown above already ran; no spurious match.
        print("wait interrupted", file=sys.stderr, flush=True)
        raise typer.Exit(EXIT_SIGINT)
    if outcome.timed_out:
        scope = effective_repo or effective_source or "all sources"
        print(
            f"timed out after {timeout} with no match for `{composed_source}` on {scope}",
            file=sys.stderr,
            flush=True,
        )
        raise typer.Exit(EXIT_TIMEOUT)
    if outcome.matched:
        code = matched_exit["code"]
        _print_matched(
            sha=sha,
            effective_repo=effective_repo,
            matched_source=matched_source,
            code=code,
        )
        raise typer.Exit(code if exit_status else EXIT_SUCCESS)
    # peer_closed / framing_error: the daemon went away before a match.
    print("broadcast connection closed before a match", file=sys.stderr, flush=True)
    raise typer.Exit(EXIT_STARTUP)


# --- cross-source composition (--all-of / --first-of) -------------------------


def _check_compose_exclusive(
    *,
    all_of: list[str] | None,
    first_of: list[str] | None,
    sha: str | None,
    match: list[str] | None,
    cond: list[str] | None,
    match_cel: str | None,
    match_jmespath: str | None,
    source: str | None,
    repo: str | None,
) -> None:
    """Reject illegal flag combinations for composition mode (startup exit 2).

    The composition flags are mutually exclusive with each other and with
    the whole single-source predicate flag family plus ``--repo``: clauses
    carry their own source scoping, and a ``--repo`` daemon filter would
    narrow the subscription to GitHub repos and silently starve non-GitHub
    clauses.
    """
    if all_of and first_of:
        _exit_with_error("--all-of and --first-of are mutually exclusive", code=EXIT_STARTUP)
    flag = "--all-of" if all_of else "--first-of"
    conflicting = {
        "--sha": sha,
        "--match": match,
        "--cond": cond,
        "--match-cel": match_cel,
        "--match-jmespath": match_jmespath,
        "--source": source,
        "--repo": repo,
    }
    used = [name for name, value in conflicting.items() if value]
    if used:
        _exit_with_error(
            f"{flag} cannot be combined with {', '.join(used)}",
            hint="Each clause carries its own source scoping (source:key=json_literal).",
            code=EXIT_STARTUP,
        )


def _parse_compose_clauses(specs: list[str], flag: str) -> list[_predicate.Predicate]:
    """Lower the repeated clause flags into per-clause Predicates.

    A malformed clause exits 2 with the offending clause named verbatim,
    before any daemon contact. This catches grammar errors only: a
    charset-valid but typo'd source name (``"pytset"``) parses fine and
    IS a silent never-match until the timeout fires.
    """
    clauses: list[_predicate.Predicate] = []
    for spec in specs:
        try:
            clauses.append(_compose.clause_predicate(_compose.parse_clause(spec)))
        except ValueError as exc:
            _exit_with_error(f"invalid {flag} clause: {exc}", code=EXIT_STARTUP)
    return clauses


def _build_composed_decide(
    clauses: list[_predicate.Predicate],
    conjunction: bool,
    tracker: _compose.AllOfTracker,
    matched_source: dict[str, str | None],
) -> Callable[[dict[str, Any]], FrameDecision]:
    """The FrameDecision closure for composition mode.

    Conjunction folds each frame into the sticky tracker and matches on the
    completing frame; disjunction matches on the first frame any clause
    accepts. No GitHub conclusion bucketing in either form -- a clause's
    predicate states its own terminal condition explicitly.
    """
    disjunction = _predicate.compose_any(*clauses)

    def _decide(frame: dict[str, Any]) -> FrameDecision:
        fields = frame.get("fields")
        if not isinstance(fields, dict):
            return FrameDecision.CONTINUE
        done = tracker.update(frame) if conjunction else disjunction(frame)
        if not done:
            return FrameDecision.CONTINUE
        frame_source = fields.get("source")
        matched_source["source"] = frame_source if isinstance(frame_source, str) else None
        return FrameDecision.MATCHED

    return _decide


def _dispatch_composed_outcome(
    *,
    outcome: WaitOutcome,
    conjunction: bool,
    timeout: str,
    tracker: _compose.AllOfTracker,
    clauses: list[_predicate.Predicate],
    matched_source: dict[str, str | None],
) -> NoReturn:
    """Translate composition mode's :class:`WaitOutcome` into a ``typer.Exit``."""
    if outcome.cancelled:
        print("wait interrupted", file=sys.stderr, flush=True)
        raise typer.Exit(EXIT_SIGINT)
    if outcome.timed_out:
        pending = tracker.outstanding if conjunction else tuple(c.source for c in clauses)
        label = "outstanding clauses" if conjunction else "clauses"
        print(
            f"timed out after {timeout}; {label}: {', '.join(pending)} "
            "(if a clause source name is typo'd it never matches; "
            "re-run with --since to replay recent events)",
            file=sys.stderr,
            flush=True,
        )
        raise typer.Exit(EXIT_TIMEOUT)
    if outcome.matched:
        if conjunction:
            print("all clauses satisfied", file=sys.stderr, flush=True)
        else:
            src = matched_source.get("source") or "(unknown)"
            print(f"matched on source={src}", file=sys.stderr, flush=True)
        raise typer.Exit(EXIT_SUCCESS)
    print("broadcast connection closed before a match", file=sys.stderr, flush=True)
    raise typer.Exit(EXIT_STARTUP)


def _wait_composed(
    *,
    clauses: list[_predicate.Predicate],
    conjunction: bool,
    timeout: str,
    deadline_seconds: float,
    since: str | None,
) -> NoReturn:
    """Run a cross-source composed wait to completion.

    Subscribes wildcard (no repo filter -- the all-source stream already
    reaches every subscriber; clauses do their own source scoping) and
    rides the unchanged ``await_predicate`` engine.
    """
    try:
        sub = open_subscriber(filters=None, since=since)
    except BroadcastConnectionError as exc:
        _exit_with_error(str(exc), hint=exc.remediation, code=EXIT_STARTUP)
    except SecretNotConfigured as exc:
        _exit_with_error(str(exc), code=EXIT_STARTUP)

    tracker = _compose.AllOfTracker(clauses)
    matched_source: dict[str, str | None] = {}
    _decide = _build_composed_decide(clauses, conjunction, tracker, matched_source)

    try:
        outcome: WaitOutcome = await_predicate(
            sub,
            decide=_decide,
            deadline_seconds=deadline_seconds,
            idle_reset=False,
        )
    finally:
        sub.sock.close()

    _dispatch_composed_outcome(
        outcome=outcome,
        conjunction=conjunction,
        timeout=timeout,
        tracker=tracker,
        clauses=clauses,
        matched_source=matched_source,
    )


@_app.command()
def _wait(
    sha: Annotated[
        str | None,
        typer.Option(
            "--sha",
            help=(
                "Wait on a GitHub commit SHA. Git-style prefix match on "
                "head_sha (>=7 hex chars, like `git show <prefix>`), so the "
                "abbreviated SHA you copy from a commit URL resolves against "
                "the full SHA GitHub stores. Implies --source github; rejects "
                "a conflicting --source."
            ),
        ),
    ] = None,
    match: Annotated[
        list[str] | None,
        typer.Option(
            "--match",
            help=(
                "Source-agnostic predicate: 'dotted.key=<json_literal>' "
                '(e.g. fields.head_sha="abc", fields.action="die", '
                "fields.run_id=12345). Repeatable: AND across distinct "
                "keys; OR within a repeated key."
            ),
        ),
    ] = None,
    cond: Annotated[
        list[str] | None,
        typer.Option(
            "--cond",
            help=("Named condition registered via waitbus.register_condition. Repeatable; AND-combined with --match."),
        ),
    ] = None,
    match_cel: Annotated[
        str | None,
        typer.Option(
            "--match-cel",
            help=("Layer-2 CEL expression (requires `pip install waitbus[cel]`). AND-combined with --match / --cond."),
        ),
    ] = None,
    match_jmespath: Annotated[
        str | None,
        typer.Option(
            "--match-jmespath",
            help=(
                "Layer-2 JMESPath expression "
                "(requires `pip install waitbus[jmespath]`). "
                "AND-combined with --match / --cond."
            ),
        ),
    ] = None,
    repo: Annotated[
        str | None,
        typer.Option(
            "--repo",
            help=(
                "owner/repo to scope the subscription (GitHub paths only). "
                "Defaults to the current git checkout's origin remote when "
                "the source is github; ignored with a note for non-GitHub "
                "sources."
            ),
        ),
    ] = None,
    source: Annotated[
        str | None,
        typer.Option(
            "--source",
            help=(
                "Restrict to one event source (github / alertmanager / "
                "pytest / docker / fs / prometheus_watchdog). Default: all."
            ),
        ),
    ] = None,
    all_of: Annotated[
        list[str] | None,
        typer.Option(
            "--all-of",
            help=(
                "Repeatable cross-source clause 'source:key=json_literal'. "
                "The wait returns when EVERY clause has been satisfied; "
                "clauses may be satisfied by DIFFERENT events over time and "
                "stay satisfied once matched (sticky conjunction). No GitHub "
                "conclusion bucketing and --exit-status has no effect: a "
                "satisfied composition exits 0. Mutually exclusive with "
                "--sha/--match/--cond/--match-cel/--match-jmespath/--source/"
                "--repo and --first-of."
            ),
        ),
    ] = None,
    first_of: Annotated[
        list[str] | None,
        typer.Option(
            "--first-of",
            help=(
                "Repeatable cross-source clause 'source:key=json_literal'. "
                "The wait returns on the FIRST event matching ANY clause "
                "(single-event disjunction). No GitHub conclusion bucketing "
                "and --exit-status has no effect: a match exits 0. Mutually "
                "exclusive with --sha/--match/--cond/--match-cel/"
                "--match-jmespath/--source/--repo and --all-of."
            ),
        ),
    ] = None,
    timeout: Annotated[
        str,
        typer.Option(
            "--timeout",
            help=(
                "Single overall deadline. Bare number = seconds; suffixes "
                "s/m/h/d accepted (e.g. 30s, 5m). Exit 124 on expiry."
            ),
        ),
    ] = "300s",
    since: Annotated[
        str | None,
        typer.Option(
            "--since",
            help=("26-char ULID resume cursor: also consider events at/after this id (replay), not just live ones."),
        ),
    ] = None,
    exit_status: Annotated[
        bool,
        typer.Option(
            "--exit-status/--no-exit-status",
            help=(
                "Default ON for GitHub frames: the terminal conclusion "
                "drives the exit code (0 success, 1 failure). "
                "--no-exit-status always exits 0 on a terminal match "
                "(still 124 on timeout, 130 on SIGINT). For non-GitHub "
                "sources a match always exits 0."
            ),
        ),
    ] = True,
) -> None:
    """Block until the composed predicate matches an event frame."""
    try:
        deadline_seconds = parse_duration(timeout)
    except ValueError as exc:
        _exit_with_error(f"invalid --timeout: {exc}", code=EXIT_STARTUP)

    if all_of or first_of:
        _check_compose_exclusive(
            all_of=all_of,
            first_of=first_of,
            sha=sha,
            match=match,
            cond=cond,
            match_cel=match_cel,
            match_jmespath=match_jmespath,
            source=source,
            repo=repo,
        )
        flag = "--all-of" if all_of else "--first-of"
        clauses = _parse_compose_clauses(all_of or first_of or [], flag)
        _wait_composed(
            clauses=clauses,
            conjunction=bool(all_of),
            timeout=timeout,
            deadline_seconds=deadline_seconds,
            since=since,
        )

    composed, effective_source = _build_predicate(
        sha=sha,
        match=match,
        cond=cond,
        match_cel=match_cel,
        match_jmespath=match_jmespath,
        source=source,
    )
    effective_repo = _resolve_repo(repo=repo, effective_source=effective_source, sha=sha)

    try:
        sub = open_subscriber(
            filters=[effective_repo] if effective_repo is not None else None,
            since=since,
        )
    except BroadcastConnectionError as exc:
        _exit_with_error(str(exc), hint=exc.remediation, code=EXIT_STARTUP)
    except SecretNotConfigured as exc:
        _exit_with_error(str(exc), code=EXIT_STARTUP)

    # --- the engine seam: bool Predicate -> FrameDecision ladder ----------
    matched_exit: dict[str, int] = {}
    matched_source: dict[str, str | None] = {}
    _decide = _build_decide(composed, matched_exit, matched_source)

    try:
        outcome: WaitOutcome = await_predicate(
            sub,
            decide=_decide,
            deadline_seconds=deadline_seconds,
            idle_reset=False,
        )
    finally:
        sub.sock.close()

    _dispatch_outcome(
        outcome=outcome,
        timeout=timeout,
        composed_source=composed.source,
        effective_repo=effective_repo,
        effective_source=effective_source,
        sha=sha,
        matched_exit=matched_exit,
        matched_source=matched_source,
        exit_status=exit_status,
    )


def main(argv: list[str] | None = None) -> int:
    """Entry point for ``waitbus wait``."""
    return run_typer_app(_app, argv)
