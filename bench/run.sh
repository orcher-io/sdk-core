#!/usr/bin/env bash
# Runs the benchmark suite against an isolated engine and Postgres.
#
#   bench/run.sh LABEL [--engine-version V | --engine-image IMAGE] [--only a,b]
#                      [--upstream mirror|shared]
#
# Starts Postgres 17 (with pg_stat_statements) and the engine image
# ghcr.io/orcher-io/orcher:V (default: latest, the latest release) in Docker,
# or IMAGE, an engine image already on this machine (an unreleased build), as is,
# on a network of their own and on free localhost ports, so no existing
# database or engine is touched. Builds the worker from this checkout, runs
# every scenario (or those named with --only), writes
# bench/results/LABEL.json and bench/results/LABEL.md (and the engine's log
# as bench/results/LABEL.engine.log, not committed), and removes the
# containers, their volumes and the network on the way out.
#
# BENCH_ENGINE_LOG_LEVEL sets the engine's log level (default warn), for
# tracing a run through its log, e.g. "info,orcher_dispatch=info".
#
# Needs docker, curl and cargo. See bench/README.md.

set -euo pipefail

usage() {
  sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

[ $# -ge 1 ] || usage
LABEL=$1
shift
case "$LABEL" in -*) usage ;; esac

ENGINE_VERSION=latest
ENGINE_IMAGE=
SUITE_ARGS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --engine-version) ENGINE_VERSION=$2; shift 2 ;;
    --engine-image) ENGINE_IMAGE=$2; shift 2 ;;
    --only) SUITE_ARGS+=(--only "$2"); shift 2 ;;
    --upstream) SUITE_ARGS+=(--upstream "$2"); shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

ROOT=$(cd "$(dirname "$0")/.." && pwd)
IMAGE=${ENGINE_IMAGE:-ghcr.io/orcher-io/orcher:$ENGINE_VERSION}
PG_IMAGE=postgres:17-alpine
RUN=orcher-bench-$(date +%s)-$$
NET=$RUN
PG=$RUN-pg
ENGINE=$RUN-engine
# Credentials of a database that exists only for this run.
DB_USER=bench
DB_PASSWORD=bench
DB_NAME=orcher_bench

cleanup() {
  # The engine's log (warnings and errors only), to see why a run did not end.
  mkdir -p "$ROOT/bench/results"
  docker logs "$ENGINE" >"$ROOT/bench/results/$LABEL.engine.log" 2>&1 || true
  docker rm -f -v "$ENGINE" "$PG" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

port_of() { docker port "$1" "$2" | head -n1 | sed 's/.*://'; }

if [ -n "$ENGINE_IMAGE" ]; then
  # A local build, not a release: used as it is, never pulled.
  docker image inspect "$IMAGE" >/dev/null 2>&1 \
    || { echo "bench: no image $IMAGE on this machine" >&2; exit 2; }
  echo "bench: using the local image $IMAGE; pulling $PG_IMAGE"
else
  echo "bench: pulling $IMAGE and $PG_IMAGE"
  docker pull -q "$IMAGE" >/dev/null
fi
docker pull -q "$PG_IMAGE" >/dev/null

docker network create "$NET" >/dev/null

echo "bench: starting Postgres"
docker run -d --name "$PG" --network "$NET" -p 127.0.0.1::5432 \
  -e POSTGRES_USER="$DB_USER" -e POSTGRES_PASSWORD="$DB_PASSWORD" -e POSTGRES_DB="$DB_NAME" \
  "$PG_IMAGE" \
  -c shared_preload_libraries=pg_stat_statements \
  -c pg_stat_statements.max=10000 \
  -c pg_stat_statements.track=top \
  -c track_counts=on \
  -c max_connections=200 >/dev/null
for _ in $(seq 1 60); do
  # The image restarts the server once after initialising: wait for the
  # final one, which listens on TCP.
  if docker exec "$PG" pg_isready -h 127.0.0.1 -U "$DB_USER" -d "$DB_NAME" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
docker exec "$PG" pg_isready -h 127.0.0.1 -U "$DB_USER" -d "$DB_NAME" >/dev/null
PG_PORT=$(port_of "$PG" 5432/tcp)

echo "bench: starting the engine"
# Every database setting is given explicitly, so nothing falls back to a
# default that could point at another database.
# ORCHER_INSECURE: the ports are published on this machine's loopback only,
# and an engine that refuses to serve without authentication on an address
# other machines could reach needs to be told that is intended.
docker run -d --name "$ENGINE" --network "$NET" \
  -p 127.0.0.1::50051 -p 127.0.0.1::8080 \
  -e ORCHER_INSECURE=true \
  -e DATABASE_URL="postgresql://$DB_USER:$DB_PASSWORD@$PG:5432/$DB_NAME" \
  -e PGHOST="$PG" -e PGPORT=5432 -e PGUSER="$DB_USER" -e PGPASSWORD="$DB_PASSWORD" -e PGDATABASE="$DB_NAME" \
  -e ORCHER_PG_MAX_CONNECTIONS=20 -e ORCHER_PG_MIN_CONNECTIONS=2 \
  -e ORCHER_AUTO_MIGRATE=true -e ORCHER_DB_WAIT_SECS=120 \
  -e GRPC_BIND_ADDR=0.0.0.0:50051 -e HTTP_BIND_ADDR=0.0.0.0:8080 \
  -e ORCHER_GRPC_MAX_MESSAGE_BYTES=33554432 -e ORCHER_PAYLOAD_MAX_BYTES=8388608 \
  -e LOG_LEVEL="${BENCH_ENGINE_LOG_LEVEL:-warn}" -e LOG_FORMAT=compact \
  "$IMAGE" >/dev/null
GRPC_PORT=$(port_of "$ENGINE" 50051/tcp)
HTTP_PORT=$(port_of "$ENGINE" 8080/tcp)
ready=
for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:$HTTP_PORT/health/ready" >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ -z "$ready" ]; then
  echo "bench: the engine did not get ready; its log:" >&2
  docker logs --tail 50 "$ENGINE" >&2
  exit 1
fi

label() { docker image inspect "$IMAGE" --format "{{ index .Config.Labels \"$1\" }}"; }
ENGINE_RELEASE=$(label org.opencontainers.image.version)
ENGINE_REVISION=$(label org.opencontainers.image.revision)
CORE_REVISION=$(git -C "$ROOT" rev-parse HEAD)
if [ -n "$(git -C "$ROOT" status --porcelain -- src host bench/src Cargo.toml)" ]; then
  CORE_REVISION="$CORE_REVISION+changes"
fi

echo "bench: building the worker"
cargo build --release --manifest-path "$ROOT/Cargo.toml" -p orcher-bench
TARGET_DIR=$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --format-version 1 --no-deps |
  sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p')

echo "bench: engine $ENGINE_RELEASE ($ENGINE_REVISION), sdk-core $CORE_REVISION"
"$TARGET_DIR/release/orcher-bench" suite \
  --engine "127.0.0.1:$GRPC_PORT" \
  --pg "postgresql://$DB_USER:$DB_PASSWORD@127.0.0.1:$PG_PORT/$DB_NAME" \
  --label "$LABEL" \
  --out "$ROOT/bench/results" \
  --meta "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --meta "engine_image=$IMAGE" \
  --meta "engine_release=$ENGINE_RELEASE" \
  --meta "engine_revision=$ENGINE_REVISION" \
  --meta "sdk_core_revision=$CORE_REVISION" \
  --meta "postgres_image=$PG_IMAGE" \
  --meta "docker_cpus=$(docker info --format '{{.NCPU}}')" \
  --meta "docker_memory_bytes=$(docker info --format '{{.MemTotal}}')" \
  --meta "load_average_at_start=$(uptime | sed 's/.*load average[s]*: *//')" \
  ${SUITE_ARGS[@]+"${SUITE_ARGS[@]}"}
