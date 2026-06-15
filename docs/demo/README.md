# waitbus-demo

A demonstration repository for [waitbus](https://github.com/astrogilda/waitbus): one fork exercises all four event sources waitbus v0.5.0 supports.

## Status

This directory is the **skeleton** of the eventual `astrogilda/waitbus-demo` repository. It lives inside the waitbus repository so the skeleton stays in lockstep with the protocol surface; the operator pushes it to a new GitHub repo manually when ready. Do not `git init` from inside this directory and push it as part of waitbus itself.

## What this demo covers

| Source | What you do | What waitbus sees |
|--------|-------------|-----------------|
| github | Push, open a PR, or `gh workflow run matrix.yml` | webhook `workflow_run` event |
| pytest | `pytest` (with the `waitbus-emit` plugin loaded) | `pytest_session` event |
| docker | `docker compose up` (containers exit on their own) | `docker_container` event |
| fs     | `touch watched-dir/<anything>`           | `fs_change` event |

## Fork and run

```bash
gh repo fork astrogilda/waitbus-demo --clone
cd waitbus-demo

# Install waitbus (the daemon and the CLI):
pip install waitbus

# Start the broadcast daemon:
waitbus broadcast serve &

# Watch every event arrive:
waitbus broadcast tap &

# Trigger each source in turn:
gh workflow run matrix.yml          # github event
pytest                              # pytest event
docker compose up                   # docker event
touch watched-dir/$(date +%s)       # fs event
```

The companion VHS recording (`.waitbus-demo/demo.gif`) walks through this flow in 30 seconds for readers who would rather not run the commands themselves.

## License

MIT. See `LICENSE`.
