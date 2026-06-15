"""Schema-aware deterministic PII redactor for GitHub webhook fixture files.

Primary layer: allowlist-driven field-strip of every documented PII-bearing
path in octokit/webhooks-shaped JSON payloads.  See
``tests/data/webhook_fixtures/README.md`` for the dual-layer rationale and
the security considerations that make header-class redaction load-bearing.

All functions are pure (return new dicts, never mutate inputs) and idempotent.
"""

from __future__ import annotations

import copy
from typing import Any

# Placeholder used for redacted scalar values in the payload body.
_REDACTED = "<redacted>"

# Placeholder used for redacted repository identity fields.
_REPO_PLACEHOLDER = "owner/waitbus-demo-fixture"

# Placeholder used for all redacted HTTP headers.
_HEADER_REDACTED = "<redacted-header>"

# Headers that must be scrubbed.  The full X-GitHub-* family is covered
# because X-Hub-Signature-256 / X-Hub-Signature carry webhook secrets and
# the broader X-GitHub-* family carries delivery and installation IDs that
# are correlation vectors in their own right.
_REDACTED_HEADERS: frozenset[str] = frozenset(
    {
        "X-Hub-Signature-256",
        "X-Hub-Signature",
        "X-GitHub-Delivery",
        "X-GitHub-Event",
        "X-GitHub-Hook-ID",
        "X-GitHub-Hook-Installation-Target-ID",
        "X-GitHub-Hook-Installation-Target-Type",
    }
)


def _redact_user(user: Any) -> Any:
    """Scrub login, id, email from a user/sender/actor object."""
    if not isinstance(user, dict):
        return user
    out = dict(user)
    for field in ("login", "id", "email"):
        if field in out:
            out[field] = _REDACTED
    return out


def _redact_commit_identity(commit: Any) -> Any:
    """Scrub author and committer name/email from a commit-shaped object."""
    if not isinstance(commit, dict):
        return commit
    out = dict(commit)
    for role in ("author", "committer"):
        if role in out and isinstance(out[role], dict):
            identity = dict(out[role])
            for field in ("name", "email", "username"):
                if field in identity:
                    identity[field] = _REDACTED
            out[role] = identity
    if "message" in out:
        out["message"] = _REDACTED
    return out


def _redact_repository(repo: Any) -> Any:
    """Scrub identity and URL fields from a repository object."""
    if not isinstance(repo, dict):
        return repo
    out = dict(repo)
    # Replace owner sub-object
    if "owner" in out and isinstance(out["owner"], dict):
        out["owner"] = _redact_user(out["owner"])
    # Identity fields
    if "full_name" in out:
        out["full_name"] = _REPO_PLACEHOLDER
    # URL fields that embed the owner/repo path
    for url_field in ("html_url", "ssh_url", "clone_url", "git_url"):
        if url_field in out:
            out[url_field] = _REDACTED
    return out


def _redact_commits_list(commits: Any) -> Any:
    """Scrub author/committer/message from every entry in a commits array."""
    if not isinstance(commits, list):
        return commits
    return [_redact_commit_identity(c) for c in commits]


def _redact_pull_request(pr: Any) -> Any:
    """Scrub user, body from a pull_request object."""
    if not isinstance(pr, dict):
        return pr
    out = dict(pr)
    if "user" in out:
        out["user"] = _redact_user(out["user"])
    if "body" in out and out["body"] is not None:
        out["body"] = _REDACTED
    return out


def _redact_issue(issue: Any) -> Any:
    """Scrub user and body from an issue object."""
    if not isinstance(issue, dict):
        return issue
    out = dict(issue)
    if "user" in out:
        out["user"] = _redact_user(out["user"])
    if "body" in out and out["body"] is not None:
        out["body"] = _REDACTED
    return out


def _redact_comment(comment: Any) -> Any:
    """Scrub user and body from a comment object."""
    if not isinstance(comment, dict):
        return comment
    out = dict(comment)
    if "user" in out:
        out["user"] = _redact_user(out["user"])
    if "body" in out and out["body"] is not None:
        out["body"] = _REDACTED
    return out


def _redact_release(release: Any) -> Any:
    """Scrub author login/id/email from a release object."""
    if not isinstance(release, dict):
        return release
    out = dict(release)
    if "author" in out:
        out["author"] = _redact_user(out["author"])
    return out


def _redact_installation(installation: Any) -> Any:
    """Scrub installation id."""
    if not isinstance(installation, dict):
        return installation
    out = dict(installation)
    if "id" in out:
        out["id"] = _REDACTED
    return out


# Branch density here mirrors the data shape: per-field rule application
# across deeply nested octokit/webhooks payload structures.  Refactoring into
# a `_REDACTION_RULES` declarative table is a natural next step if a fifth
# top-level field-family is added.
def redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a new dict with PII-bearing fields replaced by redaction placeholders.

    Covers the field allowlist documented in
    ``tests/data/webhook_fixtures/README.md``.  Does not mutate the input;
    safe to call repeatedly (idempotent).

    Args:
        payload: Parsed GitHub webhook JSON body as a Python dict.

    Returns:
        New dict with all documented PII fields replaced by ``'<redacted>'``
        or the repo placeholder string.
    """
    out: dict[str, Any] = copy.deepcopy(payload)

    # sender / actor (top-level user identity)
    if "sender" in out:
        out["sender"] = _redact_user(out["sender"])

    # repository (top-level)
    if "repository" in out:
        out["repository"] = _redact_repository(out["repository"])

    # installation
    if "installation" in out:
        out["installation"] = _redact_installation(out["installation"])

    # workflow_run — contains head_commit + nested repository copies
    if "workflow_run" in out and isinstance(out["workflow_run"], dict):
        wr = dict(out["workflow_run"])
        if "head_commit" in wr and wr["head_commit"] is not None:
            wr["head_commit"] = _redact_commit_identity(wr["head_commit"])
        if "repository" in wr:
            wr["repository"] = _redact_repository(wr["repository"])
        if "head_repository" in wr:
            wr["head_repository"] = _redact_repository(wr["head_repository"])
        if "actor" in wr:
            wr["actor"] = _redact_user(wr["actor"])
        if "triggering_actor" in wr:
            wr["triggering_actor"] = _redact_user(wr["triggering_actor"])
        out["workflow_run"] = wr

    # push / create / delete — commits list + head_commit + pusher
    if "commits" in out:
        out["commits"] = _redact_commits_list(out["commits"])
    if "head_commit" in out and out["head_commit"] is not None:
        out["head_commit"] = _redact_commit_identity(out["head_commit"])
    if "pusher" in out and isinstance(out["pusher"], dict):
        pusher = dict(out["pusher"])
        for field in ("name", "email"):
            if field in pusher:
                pusher[field] = _REDACTED
        out["pusher"] = pusher

    # pull_request
    if "pull_request" in out:
        out["pull_request"] = _redact_pull_request(out["pull_request"])

    # issue
    if "issue" in out:
        out["issue"] = _redact_issue(out["issue"])

    # comment
    if "comment" in out:
        out["comment"] = _redact_comment(out["comment"])

    # release
    if "release" in out:
        out["release"] = _redact_release(out["release"])

    return out


def redact_headers(headers: dict[str, str]) -> dict[str, str]:
    """Return a new dict with GitHub-specific headers replaced by redaction placeholders.

    Covers the documented header allowlist: X-Hub-Signature-256
    and the full X-GitHub-* family.  Case-sensitive matching mirrors the
    canonical casing GitHub uses in its delivery payloads.

    WARNING: do not use this redactor on incoming HTTP request headers.
    RFC 9110 section 5.1 makes HTTP header field names case-insensitive,
    so a real-world request might present ``x-hub-signature-256`` (an
    nginx-normalised lowercased form) that this function would leak
    through unredacted.  The redactor's contract is fixture-author
    dicts written with the canonical casing GitHub emits; if a future
    consumer pipes real request headers through it, switch to a
    lowercase-comparison form first.

    Args:
        headers: HTTP request headers as a plain str-to-str dict.

    Returns:
        New dict with redacted header values.
    """
    return {k: (_HEADER_REDACTED if k in _REDACTED_HEADERS else v) for k, v in headers.items()}
