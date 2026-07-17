.PHONY: demo demo-check font-check waitbus-check hero hero-check hero-smoke clean

VHS_MIN_VERSION := 0.10.0
VHS_BIN := vhs
# Must match `Set FontFamily` in demo.tape / hero.tape. The render falls back
# to a broken-metrics font silently when this is absent, so the check below
# refuses to render rather than ship a garbled recording.
DEMO_FONT := JetBrains Mono

# `make demo` re-renders demo.gif and demo.mp4 from demo.tape.
# The checks below guard against silent format drift across VHS
# versions and against a missing render font (the two ways a render
# silently produces a bad artefact).
demo: demo-check font-check waitbus-check
	$(VHS_BIN) demo.tape

# Refuse to render when `waitbus` does not resolve on PATH. demo.tape types
# `waitbus demo` inside VHS; if the binary is missing, VHS silently records a
# "command not found" frame and ships a broken GIF -- the same silent-bad-render
# failure the VHS-version and render-font guards exist to prevent. (`hero` gets
# this coverage through hero-smoke, which actually runs the orchestrator; the
# `demo` target had no equivalent PATH check until now.)
waitbus-check:
	@command -v waitbus >/dev/null 2>&1 || { \
		echo "error: waitbus is not on PATH; refusing to render (VHS would record a 'command not found' GIF)."; \
		echo "install it (e.g. 'uv tool install waitbus') or activate the venv that provides it, then retry."; \
		exit 1; \
	}
	@echo "waitbus: $$(command -v waitbus)"

# Version-check the installed VHS binary. Enforces a hard floor at
# VHS_MIN_VERSION via lexicographic compare under sort -V: if the
# installed version sorts strictly below the minimum, exit non-zero
# so `make demo` refuses to render against a too-old VHS that may
# silently change the tape-script PlaybackSpeed / TypingSpeed
# semantics. Refusing-to-render is the right failure mode: a stale
# GIF gets committed and ships to readers; a refused render forces
# the maintainer to upgrade VHS and re-run.
demo-check:
	@command -v $(VHS_BIN) >/dev/null 2>&1 || { \
		echo "error: $(VHS_BIN) not on PATH."; \
		echo "install: https://github.com/charmbracelet/vhs"; \
		exit 1; \
	}
	@version=$$($(VHS_BIN) --version 2>&1 | head -1 | awk '{print $$NF}' | sed 's/^v//'); \
	if [ -z "$$version" ]; then \
		echo "error: could not read $(VHS_BIN) --version output; refusing to render."; \
		exit 1; \
	fi; \
	lowest=$$(printf '%s\n%s\n' "$(VHS_MIN_VERSION)" "$$version" | sort -V | head -1); \
	if [ "$$lowest" != "$(VHS_MIN_VERSION)" ]; then \
		echo "error: vhs version $$version is older than minimum $(VHS_MIN_VERSION); upgrade required."; \
		exit 1; \
	fi; \
	echo "vhs version: $$version (minimum supported: $(VHS_MIN_VERSION))"

# Refuse to render when the pinned demo font is not installed.
# VHS renders through a headless browser that silently falls back to
# a font with broken glyph-advance metrics when the requested family
# is missing -- the failure mode that shipped a garbled recording.
# Refusing-to-render forces the maintainer to install the font (e.g.
# JetBrains Mono from https://github.com/JetBrains/JetBrainsMono) and
# re-run, the same posture as the VHS version guard above.
font-check:
	@command -v fc-list >/dev/null 2>&1 || { \
		echo "error: fc-list not on PATH (install fontconfig); cannot verify '$(DEMO_FONT)'."; \
		exit 1; \
	}
	@fc-list | grep -qi "$(DEMO_FONT)" || { \
		echo "error: render font '$(DEMO_FONT)' is not installed; refusing to render."; \
		echo "install it (e.g. https://github.com/JetBrains/JetBrainsMono/releases)"; \
		echo "into ~/.local/share/fonts and run 'fc-cache -f', then retry."; \
		exit 1; \
	}
	@echo "render font: $(DEMO_FONT) (installed)"

# `make hero` re-renders hero.gif and hero.mp4 from hero.tape -- the
# cross-harness swarm demo (two different agent frameworks on one bus, one
# fails, the peer reacts). Shares the VHS version + render-font guards with
# `demo`, and adds a tmux guard because hero.tape drives the multi-process
# orchestration inside a tmux session (`Require tmux` in the tape).
hero: demo-check font-check hero-check hero-smoke
	$(VHS_BIN) hero.tape

# Refuse to render a BROKEN demo. Run the orchestrator headless first and
# require its PROVEN verdict before recording -- this validates the very thing
# we are about to capture AND pre-syncs the venv so the recorded `uv run` is
# quiet. Without this guard a render can silently capture a failed run: a
# wrong-interpreter `ModuleNotFoundError` shipped as the GIF exactly once, which
# is why this gate exists.
hero-smoke:
	@echo "hero-smoke: running the orchestrator headless to verify it works before recording..."
	@cd "$$(git rev-parse --show-toplevel)" && uv run --group agent-recipes python -m examples.hero_swarm 2>&1 \
		| grep -q "PROVEN: two DIFFERENT frameworks woke" || { \
		echo "error: hero demo did not reach its PROVEN verdict; refusing to render a broken recording."; \
		exit 1; \
	}
	@echo "hero-smoke: PROVEN verdict observed; safe to render."

# Refuse to render the hero tape when tmux is absent. hero.tape declares
# `Require tmux` and drives the demo inside a tmux session; without tmux the
# render fails opaquely, so check up front with an actionable message --
# the same refuse-to-render posture as the VHS version and font guards.
hero-check:
	@command -v tmux >/dev/null 2>&1 || { \
		echo "error: tmux not on PATH; hero.tape drives the demo inside tmux."; \
		echo "install tmux (e.g. 'apt install tmux' / 'brew install tmux') and retry."; \
		exit 1; \
	}
	@echo "tmux: $$(tmux -V)"

# Remove rendered artefacts. The tape script is the source of
# truth; the GIF / MP4 are derived outputs.
clean:
	rm -f demo.gif demo.mp4 hero.gif hero.mp4
