# waitbus container image — SKELETON (first publish at v0.4.1, not v0.4.0).
#
# Runtime contract: run with `--read-only --tmpfs /tmp`. The daemons
# write only to the XDG state/runtime dirs (mount those as named
# volumes) and to /tmp (tmpfs). Nothing else is written.
#
# Image tag scheme (set by the publish workflow, not here):
#   ghcr.io/astrogilda/waitbus:vN.N.N
#   ghcr.io/astrogilda/waitbus:vN.N
#   ghcr.io/astrogilda/waitbus:vN
#   ghcr.io/astrogilda/waitbus:latest
#   ghcr.io/astrogilda/waitbus:sha-<shortsha>
#
# Multi-arch: build linux/amd64 + linux/arm64 via `docker buildx`.

# ---- Stage 1: build the wheel -------------------------------------------
FROM python:3.13-slim AS wheel
WORKDIR /src
COPY . .
RUN pip install --no-cache-dir build \
 && python -m build --wheel --outdir /dist

# ---- Stage 2: install into a clean prefix -------------------------------
FROM python:3.13-slim AS install
COPY --from=wheel /dist/*.whl /tmp/
RUN pip install --no-cache-dir --prefix=/install /tmp/*.whl

# ---- Stage 3: distroless runtime ----------------------------------------
# nonroot tag runs as UID 65532; no shell, no package manager.
FROM gcr.io/distroless/python3-debian12:nonroot
COPY --from=install /install /usr/local
ENV PYTHONUNBUFFERED=1 \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8
# `waitbus` is the console-script entry point installed by the wheel.
ENTRYPOINT ["/usr/local/bin/waitbus"]
CMD ["--help"]
