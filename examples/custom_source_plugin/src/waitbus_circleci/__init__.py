"""waitbus-circleci — reference plugin for the ``waitbus.sources.v1`` entry-point group.

This package demonstrates the minimal contract a third-party Python package
must satisfy to register a custom event source with waitbus. It is intentionally
a stub: real CircleCI integration would call the CircleCI v2 REST API at
``https://circleci.com/api/v2/...`` with an operator-provided API token, but
this example demonstrates ONLY the waitbus contract shape.

Operator UX (end-to-end)
------------------------

1. **Install** the plugin into the same environment as waitbus::

       pip install waitbus-circleci

2. **Restart** the waitbus daemon so the new entry-point is discovered::

       systemctl --user restart waitbus-broadcast.service

3. **Verify** registration::

       waitbus source list
       # Should include: circleci  (pipeline_finished)

4. **Emit** a test event to confirm the bus accepts the source::

       waitbus emit --source circleci --event-type pipeline_finished \\
           --payload-json @body.json

   where ``body.json`` is any JSON object. Example::

       echo '{"pipeline_id": "abc123", "status": "success"}' > /tmp/body.json
       waitbus emit --source circleci --event-type pipeline_finished \\
           --payload-json @/tmp/body.json

5. **Subscribe** (in another terminal or script)::

       waitbus wait --source circleci --event-type pipeline_finished

How this plugin is discovered
-----------------------------

waitbus calls ``importlib.metadata.entry_points(group="waitbus.sources.v1")`` at
daemon startup. This package declares the entry-point::

    [project.entry-points."waitbus.sources.v1"]
    circleci = "waitbus_circleci:plugin"

waitbus resolves ``waitbus_circleci:plugin`` — the ``plugin`` object exported by
this module — and calls ``plugin.spec()`` to retrieve the :class:`SourceSpec`
registration shape. If ``spec()`` returns a valid :class:`SourceSpec` whose
``api_version`` matches ``SOURCE_PLUGIN_API_VERSION``, the source is registered
and events with ``source="circleci"`` are accepted by the bus.

API version contract
--------------------

The entry-point group name ``waitbus.sources.v1`` is the primary version
selector. ``SourceSpec.api_version = 1`` is the secondary check that catches
plugins whose group name matches but whose internal contract has drifted (e.g.
a wheel installed against an incompatible waitbus release). Both must match for
registration to succeed.

Authoring your own plugin
--------------------------

See :mod:`waitbus_circleci.plugin` for the class that implements the
:class:`~waitbus.sources._protocol.SourcePlugin` Protocol, and
``README.md`` in this directory for a step-by-step guide.
"""

from __future__ import annotations

from .plugin import CircleCISourcePlugin

#: Singleton plugin instance. waitbus resolves this name via the entry-point
#: declaration ``circleci = "waitbus_circleci:plugin"`` and calls
#: ``plugin.spec()`` at registration time.
plugin: CircleCISourcePlugin = CircleCISourcePlugin()

__all__ = ["plugin"]
