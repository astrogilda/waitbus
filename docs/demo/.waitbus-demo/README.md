# .waitbus-demo

This directory holds the VHS recording assets for the `waitbus demo`
walkthrough:

- `demo.tape` (committed): the VHS script that records the
  self-contained `waitbus demo` subcommand end-to-end. The subcommand
  itself synthesizes one event per built-in source (github,
  pytest, docker, fs) against a temporary state directory and prints
  one `[event] ...` line per source as the subscriber tap fans them
  out.
- `Makefile` (committed): `make demo` re-renders both `demo.gif`
  (cross-platform link-unfurl fallback) and `demo.mp4` (preferred for
  GitHub's native `<video>` markdown rendering, supported since late
  2024). The Makefile version-checks the installed VHS binary
  against `VHS_MIN_VERSION = 0.10.0` before running.
- `demo.gif` + `demo.mp4` (rendered, committed once produced): the
  artefacts embedded in the main waitbus README. Re-render via
  `make demo` after any waitbus version bump that changes the demo's
  printed output.

Install VHS: <https://github.com/charmbracelet/vhs>

Workflow for the maintainer:

```
$ make demo            # version-checks vhs, then renders demo.gif + demo.mp4
$ git add demo.gif demo.mp4
$ git commit -m 'docs: re-render waitbus demo recording'
```

`vhs publish demo.tape` (separate one-off) mirrors the rendered GIF
to `vhs.charm.sh` for high-resolution embed in remote docs.

The directory is committed via `.gitkeep` so the path exists in a
fresh fork; the GIF / MP4 are typically present in the canonical
`astrogilda/waitbus` clone but a fresh fork may need to run `make demo`
once before publishing demo media.
