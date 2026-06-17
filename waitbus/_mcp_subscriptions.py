"""Per-session subscription registry for the waitbus MCP surface.

Each ServerSession that issues a ``resources/subscribe`` request lands
a ``_SessionState`` row in the module-level WeakKeyDictionary. The
registry tracks which URIs the session subscribed to plus a bounded
pre-init notification queue (1000 frames; overflow emits a synthetic
truncated marker so the client knows it has a gap).

Two invariants:

1. ``_SessionState`` never holds a strong reference back to its
   ServerSession. ``tests/test_mcp_session_state.py`` enforces this at
   the dataclass-introspection level so a future field addition that
   references ServerSession trips the lint test rather than leaking
   memory silently.

2. ``ServerSession`` must remain weak-referenceable. The
   WeakKeyDictionary semantics rely on it. The probe assertion in
   ``mcp.build_server()`` covers a hypothetical SDK change that adds
   __slots__ without __weakref__.

URI scheme:

- ``waitbus://current`` — global; matches every frame (subject to the
  daemon-side operator filter).
- ``waitbus://repo/{owner}/{repo}`` — per-repo; supports ``{owner}/*``
  and ``*/*`` wildcards in the path portion.
- ``waitbus://event/{ulid}`` — read-only, NOT subscribable; handed back
  through the read_resource handler instead.
"""

from __future__ import annotations

import collections
import dataclasses
from dataclasses import dataclass, field
from typing import Any

URI_CURRENT: str = "waitbus://current"
URI_REPO_PREFIX: str = "waitbus://repo/"
URI_EVENT_PREFIX: str = "waitbus://event/"
URI_EVENT_RAW_SUFFIX: str = "/raw"
#: Agent-message doorbell prefix. ``waitbus://agent/{name}`` is BOTH
#: subscribable (it fires the agent_message doorbell ping) AND readable
#: (the read returns a short stub directing the client to the
#: read_agent_messages tool -- never the message bodies, which would dump
#: the whole inbox and blow the context window). The doorbell is the only
#: subscribable resource whose read does NOT mirror its subscription
#: payload; the design's hard rule is that the inbox is pulled, not pushed.
URI_AGENT_PREFIX: str = "waitbus://agent/"

_PENDING_QUEUE_MAX: int = 1000


@dataclass(slots=True, kw_only=True)
class _QueuedEmit:
    """One pending notification queued before the initialize handshake.

    Stored verbatim so the post-init flush can replay it through the
    normal session send path. The notification's ``kind`` discriminates
    the two queued shapes:

    - ``"resource_updated"`` (default): a spec-standard
      ``notifications/resources/updated`` carrying the canonical
      ``waitbus://...`` ``uri``. ``payload`` holds the dict the resource
      read path would have synthesised; it is retained for diagnostics.
    - ``"claude_channel"``: an Anthropic-private
      ``notifications/claude/channel`` carrying ``content`` and ``meta``.
      Queued pre-init because the client's negotiated experimental
      capabilities (which gate this emit) are unavailable until the
      initialize handshake populates ``ServerSession.client_params``;
      the post-init flush is where that capability check finally runs.

    A single queue type carries both shapes so the FIFO ordering across
    the two notification methods is preserved exactly as the broadcast
    stream produced them, rather than splitting into two parallel queues
    that would reorder interleaved frames on flush.
    """

    kind: str = "resource_updated"
    uri: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    content: str = ""
    meta: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True, kw_only=True)
class _SessionState:
    """Per-session subscription bag plus pre-init latch.

    Invariant: no field annotation contains ``ServerSession``. The
    registry uses a WeakKeyDictionary keyed on the session; if this
    struct also referenced the session strongly the WeakKey semantics
    would be defeated.
    """

    subscriptions: set[str] = field(default_factory=set)
    initialized: bool = False
    pending: collections.deque[_QueuedEmit] = field(
        default_factory=lambda: collections.deque(maxlen=_PENDING_QUEUE_MAX)
    )
    pending_overflowed: bool = False


def _uri_matches_frame(uri: str, owner: str, repo: str) -> bool:
    """Return True iff the subscribed URI matches a frame for owner/repo.

    Accepts ``waitbus://current`` (matches everything) and
    ``waitbus://repo/{owner}/{repo}`` patterns where each segment may be
    a literal or the wildcard ``*``.

    The matcher is path-portion only; it mirrors the daemon-side
    filter-language semantics in ``_broadcast_sub.py`` so an operator
    config that uses ``owner/*`` on the daemon side renders identically
    on the subscription side.
    """
    if uri == URI_CURRENT:
        return True
    if not uri.startswith(URI_REPO_PREFIX):
        return False
    path = uri[len(URI_REPO_PREFIX) :]
    parts = path.split("/")
    if len(parts) != 2:
        return False
    pat_owner, pat_repo = parts
    if pat_owner != "*" and pat_owner != owner:
        return False
    return pat_repo == "*" or pat_repo == repo


def parse_agent_uri(uri: str) -> str | None:
    """Extract the agent name from a ``waitbus://agent/{name}`` URI, or None.

    Returns the (non-empty) name segment verbatim. The name is a
    self-asserted agent address under the same-UID trust model, so it is
    not validated against any registry here -- a name that does not match
    any committed message simply never receives a doorbell ping.
    """
    if not uri.startswith(URI_AGENT_PREFIX):
        return None
    name = uri[len(URI_AGENT_PREFIX) :]
    return name or None


def is_subscribable_uri(uri: str) -> bool:
    """Return True iff the URI is in the subscribable subset.

    Event URIs (``waitbus://event/{ulid}``) are read-only by design and
    do not participate in the subscription registry. Agent doorbells
    (``waitbus://agent/{name}``) ARE subscribable -- the subscription is
    what arms the doorbell ping.
    """
    if uri == URI_CURRENT:
        return True
    if uri.startswith(URI_REPO_PREFIX):
        path = uri[len(URI_REPO_PREFIX) :]
        parts = path.split("/")
        return len(parts) == 2 and all(parts)
    return parse_agent_uri(uri) is not None


def is_readable_uri(uri: str) -> bool:
    """Return True iff the URI is readable via read_resource.

    All three waitbus URI shapes are readable; subscribability is the
    narrower predicate. Used by the read_resource handler to reject
    unknown schemes early with a clear error rather than letting them
    propagate to the events_query layer.
    """
    # Both waitbus://event/{ulid} and waitbus://event/{ulid}/raw share the
    # URI_EVENT_PREFIX prefix, so the prefix check below admits both.
    # The /raw form remains undiscoverable (absent from list_resources
    # and list_resource_templates) and unsubscribable by design.
    # waitbus://agent/{name} is readable too: its read returns the
    # read_agent_messages stub, NOT the inbox.
    return (
        uri == URI_CURRENT
        or uri.startswith(URI_REPO_PREFIX)
        or uri.startswith(URI_EVENT_PREFIX)
        or uri.startswith(URI_AGENT_PREFIX)
    )


def parse_repo_uri(uri: str) -> tuple[str, str] | None:
    """Extract (owner, repo) from a waitbus://repo/{owner}/{repo} URI.

    Returns None for non-repo URIs. Wildcards are returned verbatim
    so callers that want to feed the matched values into a daemon
    filter can do so without re-parsing.
    """
    if not uri.startswith(URI_REPO_PREFIX):
        return None
    path = uri[len(URI_REPO_PREFIX) :]
    parts = path.split("/")
    if len(parts) != 2 or not all(parts):
        return None
    return parts[0], parts[1]


def parse_event_uri(uri: str) -> str | None:
    """Extract the ULID from a waitbus://event/{ulid} URI, or None.

    Returns None for the sibling waitbus://event/{ulid}/raw form so the
    raw branch in the read_resource handler can match first without
    this helper swallowing the URI as a ULID literally ending in
    ``/raw`` (which would route to the capped branch and return a
    truncation marker pointing at a nonexistent ULID).
    """
    if not uri.startswith(URI_EVENT_PREFIX):
        return None
    tail = uri[len(URI_EVENT_PREFIX) :]
    if not tail or tail.endswith(URI_EVENT_RAW_SUFFIX):
        return None
    return tail


def parse_event_raw_uri(uri: str) -> str | None:
    """Extract the ULID from a waitbus://event/{ulid}/raw URI, or None.

    Returns the ULID iff the URI matches the exact raw shape with a
    non-empty ULID segment. The raw form yields the full fenced
    payload uncapped, opt-in by URI rather than by parameter.
    """
    if not uri.startswith(URI_EVENT_PREFIX):
        return None
    tail = uri[len(URI_EVENT_PREFIX) :]
    if not tail.endswith(URI_EVENT_RAW_SUFFIX):
        return None
    ulid = tail[: -len(URI_EVENT_RAW_SUFFIX)]
    return ulid or None


def agent_doorbell_uri_for_session(
    subscriptions: set[str],
    msg_to: str,
) -> str | None:
    """Return the ONE agent-doorbell URI to ping a session for, or None.

    The dedup contract (SWARM_DESIGN.md "Wildcard fan-out + dedup"): a
    session receives at most one ``resources/updated`` ping per committed
    ``agent_message``, regardless of how many ``waitbus://agent/...``
    subscriptions it holds.

    - A directed message (``msg_to != "*"``) pings the session iff it
      holds ``waitbus://agent/{msg_to}``; that exact URI is returned.
    - A broadcast message (``msg_to == "*"``) pings the session iff it
      holds ANY ``waitbus://agent/...`` subscription; a single
      deterministic URI (the lexicographically smallest such subscription)
      is returned so the same session is pinged exactly once with a stable
      URI rather than once per agent subscription it holds.

    ``waitbus://current`` and ``waitbus://repo/...`` subscriptions are
    deliberately ignored here: an ``agent_message`` is partitioned out of
    the CI stream, so a CI-watching subscriber is never double-pinged.
    """
    agent_subs = sorted(uri for uri in subscriptions if uri.startswith(URI_AGENT_PREFIX))
    if not agent_subs:
        return None
    if msg_to != "*":
        target = f"{URI_AGENT_PREFIX}{msg_to}"
        return target if target in subscriptions else None
    # Broadcast: one stable ping for the whole session.
    return agent_subs[0]


def assert_no_session_back_reference() -> None:
    """Runtime guard re-asserting the no-strong-reference invariant.

    Called from the introspection test; raising here means a future
    field on ``_SessionState`` referenced ``ServerSession`` directly,
    defeating the WeakKeyDictionary semantics. The test imports this
    function so the failure carries an actionable traceback.
    """
    for f in dataclasses.fields(_SessionState):
        annotation = repr(f.type)
        if "ServerSession" in annotation:
            raise AssertionError(
                f"_SessionState field {f.name!r} references ServerSession; "
                "this defeats WeakKeyDictionary semantics and leaks sessions"
            )
