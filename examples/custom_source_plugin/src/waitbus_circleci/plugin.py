"""CircleCI source plugin -- implements the SourcePlugin Protocol.

This module is intentionally a stub. It demonstrates the waitbus contract shape
for a ``waitbus.sources.v1`` plugin without making any actual network calls.

Real CircleCI integration would:

1. Read an API token from the environment (e.g. ``CIRCLECI_TOKEN``) or from a
   waitbus-managed secrets store -- never hard-code credentials.
2. Poll or subscribe to ``https://circleci.com/api/v2/pipeline`` (v2 REST API)
   using an HTTP client such as ``httpx`` or ``urllib.request``.
3. Emit ``EventInsert`` instances via the public ``waitbus.emit``
   API as each ``pipeline_finished`` event arrives. waitbus does NOT prescribe
   any producer-loop method on the plugin object; the plugin runs its own
   long-lived loop (typically as a systemd / launchd service) and calls
   ``emit()`` directly.

This stub demonstrates ONLY the contract shape: a single ``spec()`` method
returning a typed :class:`SourceSpec`. Operators integrating a real CircleCI
poller add their producer loop in a separate module (or daemon entry-point)
that calls ``emit()`` after pip-installing this plugin.
"""

from __future__ import annotations

from waitbus.sources._protocol import SOURCE_PLUGIN_API_VERSION, SourceSpec


class CircleCISourcePlugin:
    """Implements the :class:`~waitbus.sources._protocol.SourcePlugin` Protocol.

    waitbus does not use ``isinstance`` to validate plugins (the Protocol is not
    ``@runtime_checkable``). Instead, waitbus calls ``inspect.signature`` on
    ``spec`` at registration time and raises ``PluginContractError`` on
    mismatch. This class satisfies the protocol by providing ``spec()`` with
    the expected zero-arg signature.

    Producers emit via the public ``waitbus.emit`` API, not via
    any method on this class. (A previous version of the SourcePlugin
    Protocol declared a ``fetch`` method, but the registry never invoked it
    and the example plugin's ``fetch`` raised ``NotImplementedError``; the
    method was documentation, not contract, and was dropped in an earlier
    cleanup.)
    """

    def spec(self) -> SourceSpec:
        """Return the plugin's typed registration shape.

        Called once by waitbus at daemon startup. The returned :class:`SourceSpec`
        tells waitbus:

        * ``name``: The canonical source name used in ``waitbus emit --source``
          and on the wire. Must be a non-empty lowercase ASCII string matching
          ``[a-z][a-z0-9_]*``. Must also equal the entry-point key declared
          in your ``pyproject.toml`` -- waitbus refuses a silent rename.
        * ``event_types``: Values accepted for ``waitbus emit --event-type``
          when ``--source circleci`` is specified. Each value must match the
          same regex as ``name`` and must not collide with any built-in or
          other-plugin-registered ``event_type`` value.
        * ``payload_schema``: ``None`` here (opt-in). When set to a
          ``msgspec.Struct`` subclass, waitbus validates decoded payloads
          against it at emit time.
        * ``api_version``: Must equal ``SOURCE_PLUGIN_API_VERSION`` (currently
          ``1``). waitbus raises ``PluginVersionMismatchError`` if the value
          diverges.

        Returns:
            A frozen :class:`SourceSpec` instance describing the circleci source.
        """
        return SourceSpec(
            name="circleci",
            event_types=("pipeline_finished",),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        )


__all__ = ["CircleCISourcePlugin"]
