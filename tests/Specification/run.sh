#!/usr/bin/env bash
#
# Runs the Autobahn TestSuite against the ws package and prints the verdict.
#
# It starts the echo server on the host, drives the official fuzzing client at
# it from a container, collects the reports and exits non-zero if any case came
# back as anything but OK or NON-STRICT.
#
#   ./tests/Specification/run.sh
#
# Nothing else in the repository depends on this. `go build ./...` and
# `go test ./...` do not run it and do not need Docker; the two Go commands
# under this directory compile on their own so the harness cannot rot unnoticed.
#
# It is a category of its own and not a unit, feature or end-to-end suite: the
# runner is somebody else's, and what it checks the code against is a document
# that is not ours.
#
# # The target, and what is excluded
#
# Autobahn 18 -- draft 18 is RFC 6455 -- minus groups 12 and 13, which are
# permessage-deflate. The transport implements that extension and the echo
# server does not negotiate it, so those groups measure nothing here. Turning
# EnableCompression on in the echo server is what would bring the two groups
# into the run.
#
# # Two settings that are not tuning
#
# failByDrop is false in fuzzingclient.json. It is the setting every reference
# implementation runs with, and what it turns off is failing a case purely
# because the TCP connection was dropped after the closing handshake rather than
# lingering. What is being measured is the protocol -- did the right close code
# go out, at the right moment -- and not how long a socket stays half-open.
#
# The container image is amd64 only and runs under emulation on arm64, which is
# why --platform is passed explicitly rather than left to a warning. It makes
# group 9, which pushes 16 MiB per case, take minutes rather than seconds.
#
# # Environment
#
#   AUTOBAHN_PORT     port the echo server listens on          (default 9001)
#   AUTOBAHN_TARGET   host:port the container dials    (default host.docker.internal:$AUTOBAHN_PORT)
#   AUTOBAHN_IMAGE    the suite image                  (default crossbario/autobahn-testsuite:latest)
#   AUTOBAHN_PLATFORM container platform                       (default linux/amd64)
#
# AUTOBAHN_TARGET exists because reaching the host from a container is the one
# part of this that is not the same everywhere: host.docker.internal is right on
# Docker Desktop, on colima and on Rancher Desktop, and on a plain Linux daemon
# it needs the host's bridge address instead.

set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "${here}/../.." && pwd)"

port="${AUTOBAHN_PORT:-9001}"
target="${AUTOBAHN_TARGET:-host.docker.internal:${port}}"
image="${AUTOBAHN_IMAGE:-crossbario/autobahn-testsuite:latest}"
platform="${AUTOBAHN_PLATFORM:-linux/amd64}"

work="${here}/.work"
reports="${here}/reports"
container="joaju-autobahn-$$"
echo_pid=""

log() { printf '==> %s\n' "$*"; }
fail() { printf 'run.sh: %s\n' "$*" >&2; exit 2; }

cleanup() {
    if [ -n "${echo_pid}" ] && kill -0 "${echo_pid}" 2>/dev/null; then
        kill "${echo_pid}" 2>/dev/null || true
        wait "${echo_pid}" 2>/dev/null || true
    fi
    docker rm -f "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || fail "docker is not installed; the suite runs in a container"
docker version >/dev/null 2>&1 || fail "the docker daemon is not reachable; start it and try again"
command -v go >/dev/null 2>&1 || fail "go is not installed"

rm -rf "${work}" "${reports}"
mkdir -p "${work}" "${reports}"

# A built binary rather than `go run`: `go run` execs the program as a child and
# killing the parent leaves the child holding the port, which turns the next run
# into a mystery.
log "building the echo server"
( cd "${root}" && go build -o "${work}/echo" ./tests/Specification/echo )

log "starting the echo server on :${port}"
"${work}/echo" -addr ":${port}" >"${work}/echo.log" 2>&1 &
echo_pid=$!

# The server prints its resolved address once net.Listen has returned, so the
# line is proof the port is bound rather than a guess about how long that takes.
for _ in $(seq 1 100); do
    if grep -q 'listening on' "${work}/echo.log" 2>/dev/null; then
        break
    fi
    if ! kill -0 "${echo_pid}" 2>/dev/null; then
        cat "${work}/echo.log" >&2
        fail "the echo server exited before it was listening"
    fi
    sleep 0.1
done
grep -q 'listening on' "${work}/echo.log" 2>/dev/null || {
    cat "${work}/echo.log" >&2
    fail "the echo server did not come up"
}

# The committed spec is the canonical one and names host.docker.internal. The
# only thing substituted is the address, so a run on a daemon that reaches the
# host differently is still running the same case list.
sed "s|ws://host.docker.internal:9001|ws://${target}|" \
    "${here}/fuzzingclient.json" >"${work}/fuzzingclient.json"

log "running the suite against ws://${target}"

# docker cp rather than a bind mount. A bind mount needs the daemon to share a
# filesystem with this directory, which is false for a remote daemon and false
# for a VM started without mounts; cp works in both.
docker create --platform "${platform}" --name "${container}" "${image}" \
    wstest --mode fuzzingclient --spec /config/fuzzingclient.json >/dev/null

docker cp "${work}/fuzzingclient.json" "${container}:/config/fuzzingclient.json"

suite_status=0
docker start --attach "${container}" || suite_status=$?

if ! docker cp "${container}:/reports/." "${reports}/" 2>/dev/null; then
    fail "the suite produced no reports (container exited ${suite_status})"
fi

log "summarising"

# The number of cases Autobahn 18 runs once 12.* and 13.* are excluded. It is
# checked rather than trusted: the suite exits zero when it gives up on a
# connection halfway through, and every case it did reach can be green, so a
# truncated run is indistinguishable from a clean one without it.
#
# The suite announces the same number on the line "Ok, will run N test cases",
# which is where to look if a version bump moves it.
expected_cases=301

context="Autobahn TestSuite against the ws package of github.com/arandu-io/joaju
run at:     $(date -u '+%Y-%m-%d %H:%M:%S UTC')
target:     ws://${target}
image:      ${image} (${platform})
suite:      Autobahn 18, all cases except 12.* and 13.* (permessage-deflate,
            not negotiated by this harness)
pass rule:  OK or NON-STRICT, and a closing handshake of OK or INFORMATIONAL

"

( cd "${root}" && go run ./tests/Specification/report \
    -reports "${reports}" -out "${here}/REPORT.txt" \
    -min-cases "${expected_cases}" -context "${context}" )
