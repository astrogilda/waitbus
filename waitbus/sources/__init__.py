"""Local event-source clients — producers that feed the bus.

Every module in this package observes some local signal (a finished
pytest session, a Docker container that died, a file written to disk),
builds a write-shape :class:`waitbus._types.EventInsert` with
the canonical source name (``"pytest"``, ``"docker"``, ``"fs"``), and
lands it as an event. What they all share is the idempotency
discipline: a deterministic ``delivery_id`` natural key so a retry, a
reconnect, or a coalesced replay collapses to the same row instead of
a duplicate.

How a source reaches the store differs by source, and the difference is
deliberate:

* :mod:`waitbus.sources.docker_watch` and
  :mod:`waitbus.sources.fs_watch` are thin clients of the public
  :func:`waitbus.emit` API — one event per signal, the single
  ingress contract (commit-then-ring, WAL safety) owned by
  :func:`waitbus.emit`.
* :mod:`waitbus.sources.pytest_emit` opens ``waitbus._db``
  directly (``_db.connect``, ``insert_event(commit=False)`` in a loop,
  one ``conn.commit()``, one ``_db._doorbell.ring()``) to batch a whole
  session's results into a single commit + a single doorbell ring. It
  bypasses the public ``emit`` because ``emit`` exposes only a
  single-event seam, with no batch entry point.

These local sources are the framework-neutral counterpart to the GitHub
webhook/poll ingress: any local signal -> the bus.

This module also re-exports the public plugin-registry surface so
third-party plugin authors and operator-side scripts can import everything
from a single seam (``from waitbus.sources import SourceSpec, ...``)
rather than reaching into the private ``_protocol`` / ``_registry`` /
``_attestation`` modules.
"""

from ._attestation import (
    AttestationToolingMissingError,
    AttestationVerificationError,
    VerifiedPublisher,
)
from ._config import AllowlistCorruptError
from ._protocol import (
    SOURCE_PLUGIN_API_VERSION,
    PluginContractError,
    PluginShadowError,
    PluginVersionMismatchError,
    SourcePlugin,
    SourceSpec,
)
from ._registry import (
    ENTRY_POINT_GROUP,
    discover_plugins,
    discover_plugins_once,
    entry_points_by_name,
    event_types_supported,
    is_known_source,
    known_sources,
    plugin_publishers,
    register_plugin,
)

__all__ = [
    "ENTRY_POINT_GROUP",
    "SOURCE_PLUGIN_API_VERSION",
    "AllowlistCorruptError",
    "AttestationToolingMissingError",
    "AttestationVerificationError",
    "PluginContractError",
    "PluginShadowError",
    "PluginVersionMismatchError",
    "SourcePlugin",
    "SourceSpec",
    "VerifiedPublisher",
    "discover_plugins",
    "discover_plugins_once",
    "entry_points_by_name",
    "event_types_supported",
    "is_known_source",
    "known_sources",
    "plugin_publishers",
    "register_plugin",
]
