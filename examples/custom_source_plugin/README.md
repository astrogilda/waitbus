# waitbus-circleci: reference plugin for `waitbus.sources.v1`

This directory contains a complete, buildable Python package that demonstrates
how to register a custom event source with [waitbus](https://github.com/astrogilda/waitbus)
via the `waitbus.sources.v1` entry-point group.

The plugin registers the source name `circleci` and declares support for the
`pipeline_finished` event type. The `fetch` implementation is a documented
stub that does not call the CircleCI API, so this example can be read,
installed, and tested without any CircleCI account or credentials.

---

## Demonstration goals

- The minimal `pyproject.toml` shape for a `waitbus.sources.v1` plugin.
- How to implement the `SourcePlugin` Protocol (`spec()` + `fetch()`).
- How `SourceSpec` ties together `name`, `event_types`, `payload_schema`, and
  `api_version`.
- The operator workflow: install → restart daemon → verify → emit.
- Where to put real CircleCI API logic when extending this stub.

---

## Install

Install the plugin into the same Python environment as waitbus:

```sh
pip install /path/to/examples/custom_source_plugin
```

Or, if you have published the wheel to PyPI:

```sh
pip install waitbus-circleci
```

---

## Configure

waitbus discovers plugins at daemon startup via
`importlib.metadata.entry_points(group="waitbus.sources.v1")`.

Plugin discovery is controlled by `$XDG_CONFIG_HOME/waitbus/config.toml`
(typically `~/.config/waitbus/config.toml`):

```toml
[plugins]
autoload = true   # default — all installed waitbus.sources.v1 plugins are loaded
# allow = ["circleci"]   # if autoload=false, explicitly allow specific plugins
# deny = []              # always-applied blocklist
```

After installing the plugin, restart the waitbus daemon:

```sh
systemctl --user restart waitbus-broadcast.service
```

Or, for a quick manual test, run the broadcaster directly:

```sh
waitbus serve --all
```

---

## Verify (`waitbus source list`)

Confirm that the plugin registered correctly:

```sh
waitbus source list
```

Expected output includes:

```
circleci    pipeline_finished
```

To inspect the full registration details:

```sh
waitbus source show circleci
```

---

## Emit an event

Send a `pipeline_finished` event from any shell or script:

```sh
# Write a sample payload
echo '{"pipeline_id": "abc123", "status": "success", "branch": "main"}' \
    > /tmp/body.json

# Emit
waitbus emit --source circleci --event-type pipeline_finished \
    --payload-json @/tmp/body.json
```

Subscribe in a second terminal to confirm delivery:

```sh
waitbus wait --source circleci --event-type pipeline_finished
```

---

## Authoring your own plugin (the 4 steps)

### Step 1: Create a Python package

```
my-waitbus-plugin/
    pyproject.toml
    src/
        my_waitbus_plugin/
            __init__.py
            plugin.py
```

### Step 2: Declare the entry-point in `pyproject.toml`

```toml
[project]
name = "my-waitbus-plugin"
dependencies = ["waitbus>=0.5,<0.6"]

[project.entry-points."waitbus.sources.v1"]
my_source = "my_waitbus_plugin:plugin"
```

The key (`my_source`) becomes the canonical source name. The value
(`my_waitbus_plugin:plugin`) is the Python import path to the plugin instance.

### Step 3: Implement the `SourcePlugin` Protocol

```python
# src/my_waitbus_plugin/plugin.py
from __future__ import annotations
from collections.abc import Iterator
from typing import TYPE_CHECKING
from waitbus.sources._protocol import SOURCE_PLUGIN_API_VERSION, SourceSpec

if TYPE_CHECKING:
    from waitbus._types import EventInsert

class MySourcePlugin:
    def spec(self) -> SourceSpec:
        return SourceSpec(
            name="my_source",
            event_types=("build_finished",),
            payload_schema=None,
            api_version=SOURCE_PLUGIN_API_VERSION,
        )

    def fetch(self, *args: object, **kwargs: object) -> Iterator[EventInsert] | None:
        # Poll your CI system here and yield EventInsert objects,
        # or emit via waitbus's public emit API and return None.
        ...
```

### Step 4: Export a singleton instance from `__init__.py`

```python
# src/my_waitbus_plugin/__init__.py
from my_waitbus_plugin.plugin import MySourcePlugin

plugin: MySourcePlugin = MySourcePlugin()
__all__ = ["plugin"]
```

waitbus resolves the entry-point to the `plugin` object and calls `plugin.spec()`
once at registration time. The `api_version` field must equal
`SOURCE_PLUGIN_API_VERSION` (currently `1`); a mismatch raises
`PluginVersionMismatchError`.

---

## Notes on payload schemas (optional)

`payload_schema=None` tells waitbus to treat the payload as opaque JSON. If your
source emits a known payload shape, you can pass a `msgspec.Struct` subclass:

```python
import msgspec

class PipelinePayload(msgspec.Struct, frozen=True):
    pipeline_id: str
    status: str
    branch: str | None = None

# In spec():
return SourceSpec(
    name="my_source",
    event_types=("build_finished",),
    payload_schema=PipelinePayload,  # waitbus validates decoded payloads
    api_version=SOURCE_PLUGIN_API_VERSION,
)
```

When set, waitbus decodes and validates incoming payloads against the struct type
at emit time, rejecting malformed events before they reach the bus.

---

## See also

- [`../../docs/CUSTOM_SOURCES.md`](../../docs/CUSTOM_SOURCES.md) --
  operator how-to, supply-chain hygiene, and failure-mode reference.
