# waitbus demo

A self-contained sample project for [waitbus](https://github.com/astrogilda/waitbus):
one repository that exercises all four built-in event sources.

## What this demo covers

| Source | What you do | What waitbus sees |
|--------|-------------|-----------------|
| github | Push, open a PR, or `gh workflow run matrix.yml` | webhook `workflow_run` event |
| pytest | `pytest` (with the `waitbus-emit` plugin loaded) | `pytest_session` event |
| docker | `docker compose up` (containers exit on their own) | `docker_container` event |
| fs     | `touch watched-dir/<anything>`           | `fs_change` event |

## Run it

Copy this directory into a repository of your own (or run it in place), then:

```bash
# Install waitbus (the daemon and the CLI):
uv tool install waitbus

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

The companion VHS recording (`.waitbus-demo/demo.gif`) walks through this flow in
30 seconds for readers who would rather not run the commands themselves.

## License

MIT. See the repository [LICENSE](../../LICENSE).
