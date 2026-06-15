# A trivial single-stage image used by the docker-compose.yml demo.
# The image runs a no-op so a `docker compose up` produces a clean
# container-exit event with conclusion=success.
#
# alpine:3.20 is intentionally tag-pinned, NOT digest-pinned, because
# this Dockerfile ships as a demonstration skeleton operators COPY
# into their own demo repo before customising. Forcing a specific
# digest here would route every fork through the demo author's
# alpine mirror; tag-pinning is the right scope-handoff for a
# skeleton. Operators who fork this Dockerfile for a production
# workload SHOULD digest-pin (e.g. ``FROM alpine:3.20@sha256:...``);
# fetch the current digest via ``docker image inspect alpine:3.20
# --format='{{index .RepoDigests 0}}'`` after a pristine pull, and
# update the line in the operator's fork of this file.
FROM alpine:3.20

CMD ["sh", "-c", "echo 'worker ran'; exit 0"]
