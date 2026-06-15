# VHS tape — `waitbus demo` walkthrough.
#
# Render with:
#
#     vhs demo.tape          # produces demo.gif + demo.mp4
#
# Or invoke the Makefile, which version-checks vhs before running:
#
#     make demo
#
# VHS install: https://github.com/charmbracelet/vhs
#
# Pinned VHS version: >= 0.10.0 (struct stability for Output / Set /
# Type / Enter / Sleep). The Makefile asserts this minimum.
# Terminal geometry, font, and theme are pinned to keep the rendered
# output reproducible across machines.

Output demo.gif
Output demo.mp4

Set FontFamily "JetBrains Mono"
Set FontSize 28
Set Width 2800
Set Height 1400
Set Theme "Dracula"
# VHS compresses idle frames, so the rendered duration tracks the count
# of frames where the screen actually changes (and is somewhat noisy run
# to run), not wall-clock. The demo paces its reveal to maximise distinct
# frames; this quarter-speed playback stretches them so each line lands
# readably. Recording-only; the real `waitbus demo` runs at its normal ~8s.
Set PlaybackSpeed 0.25
Set Shell "bash"
Set TypingSpeed 60ms

# Opening: a clean terminal.
Hide
Type "clear"
Enter
Sleep 200ms
Show

# Step 1 — one pitch line, then the command. The opening shows a single
# coherent invocation (`waitbus demo`); the one-command no-install path
# (`uvx waitbus demo`) is promoted in the closing call-to-action, not the
# opening, so the recording never shows `uvx waitbus demo` and then runs a
# different line. On the render box `waitbus demo` is the editable
# working-tree build; the rendered output is identical to what
# `uvx waitbus demo` produces for a viewer.
Type "# waitbus: block on CI/test/docker/fs events instead of polling for them."
Enter
Sleep 800ms

# Step 2 — invoke the demo. The subcommand is self-contained: it spins
# up a temporary broadcast daemon, blocks an agent on `waitbus wait`
# (phase 1, the value prop), then fans the other three sources out to a
# live subscriber (phase 2, breadth).
Type "waitbus demo"
Enter

# The demo paces its own reveal (a line roughly every ~0.7s) so content
# streams steadily over ~9s rather than bursting -- VHS captures active
# output faithfully (it only compresses idle), so a steady reveal yields
# a readable recording without slow-motion playback. Sleep just past the
# demo's wall time plus a short settle on the closing banner.
Sleep 11s
