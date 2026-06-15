# Vendored `@octokit/webhooks` payload templates

These JSON schemas and example payloads are vendored verbatim from the
`octokit/webhooks` repository and used as field-shape generator substrates
by `benchmarks/gen_corpus.py`.

- Upstream: <https://github.com/octokit/webhooks>
- Upstream commit SHA: `76f8deb2d40c05aa72a8281eb0113dbe5e6a8495`
- Subdirectories vendored:
  - `payload-schemas/api.github.com/workflow_run/` (3 files)
  - `payload-examples/api.github.com/workflow_run/` (4 files)
- Upstream license: MIT (Copyright (c) 2018 Gregor Martynus)

Vendored content is kept as-is to preserve byte-identity with the upstream
schema; the waitbus generator parses and reshapes at runtime rather than
mutating the vendored files.

Refresh procedure (maintainer-side):

    git -C ~/Documents/git-clones/octokit-webhooks pull
    cp ~/Documents/git-clones/octokit-webhooks/payload-schemas/api.github.com/workflow_run/{completed,in_progress,requested}.schema.json benchmarks/data/payload_templates/schemas/
    cp ~/Documents/git-clones/octokit-webhooks/payload-examples/api.github.com/workflow_run/*.payload.json benchmarks/data/payload_templates/examples/
    # update the commit SHA above to the new HEAD
