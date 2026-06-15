# waitbus container image

**Status:** skeleton. First publish at v0.4.1, not v0.4.0.

## Build

```bash
docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f packaging/docker/Dockerfile \
  -t ghcr.io/astrogilda/waitbus:dev .
```

The build is three-stage: build the wheel, install it into a clean
prefix, then copy the prefix into a distroless `:nonroot` runtime
(UID 65532, no shell, no package manager).

## Run

The image is read-only-safe. Run with:

```bash
docker run --rm \
  --read-only --tmpfs /tmp \
  -v waitbus-state:/home/nonroot/.local/state/waitbus \
  -v waitbus-runtime:/run/waitbus \
  ghcr.io/astrogilda/waitbus:vX.Y.Z listener serve
```

Daemons write only to the XDG state/runtime dirs (mount as named
volumes) and `/tmp` (tmpfs). The root filesystem stays read-only.

## Tags (set by the publish workflow)

```
ghcr.io/astrogilda/waitbus:vN.N.N
ghcr.io/astrogilda/waitbus:vN.N
ghcr.io/astrogilda/waitbus:vN
ghcr.io/astrogilda/waitbus:latest
ghcr.io/astrogilda/waitbus:sha-<shortsha>
```

Publish carries an SBOM (`syft` CycloneDX) and build-provenance
attestation pushed to the registry.
