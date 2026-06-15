# waitbus workflow relay (GitHub Action skeleton)

**Default path first:** the built-in `github` source needs no Action at
all. The waitbus daemon's own webhook listener (or its zero-setup etag
poller) already ingests `workflow_run` completions for any repository
you watch. Use this Action only if you have a specific reason to push
from inside the workflow instead.

## Overview

A composite action that synthesizes a minimal `workflow_run`-shaped
completion payload from the runner context, signs it with your webhook
secret (HMAC-SHA256, `X-Hub-Signature-256`), and POSTs it to
`<listener-url>/webhook` — exactly the headers and payload shape the
waitbus listener validates.

## The constraint

The bus is **workstation-local by design**. GitHub's runners cannot
reach your listener unless you deliberately expose it through a relay
you own — a tailscale funnel, `cloudflared`, or an `ssh -R` tunnel.
`listener-url` is that relay's hostname, not anything waitbus hosts for
you. If you do not want to run a relay, stop here and use the built-in
`github` source.

## Usage

```yaml
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: make build
      - name: Tell the workstation bus
        if: always()
        uses: ./examples/emitters/github_action
        with:
          listener-url: ${{ secrets.WAITBUS_RELAY_URL }}
          webhook-secret: ${{ secrets.WAITBUS_WEBHOOK_SECRET }}
          conclusion: ${{ job.status }}
```

The `X-GitHub-Delivery` id is `gha-relay-<run id>-<run attempt>`
(plus the optional `delivery-suffix`), so re-delivered runs dedup via
the listener's normal `delivery_id` idempotency.

## Status

This directory is a **skeleton**, validated structurally by
`tests/test_emitter_examples.py` (action shape, listener header
contract, shellcheck of the run script). It is not published to the
Actions Marketplace.
