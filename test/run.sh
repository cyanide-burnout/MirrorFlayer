#!/bin/sh
# Docker test: MariaDB with a row-based binlog and Tarantool 3 running
# test/scenario.lua in phases. Containers are named mirrorflayer-* and
# removed afterwards, the work directory too. The TT_* variables of the
# official image switch Tarantool to its cluster config, so they are unset.
set -eu

cd "$(dirname "$0")/.."
SOURCE=$(pwd)
WORK=${WORK:-$SOURCE/.work}
MARIADB_IMAGE=${MARIADB_IMAGE:-mariadb:11.4}
TARANTOOL_IMAGE=${TARANTOOL_IMAGE:-tarantool/tarantool:3}
NETWORK=mirrorflayer-net
MARIADB=mirrorflayer-mariadb
TARANTOOL=mirrorflayer-tarantool

cleanup() {
  docker rm -f "$MARIADB" "$TARANTOOL" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}

cleanup
trap cleanup EXIT INT TERM
mkdir -p "$WORK/data" "$WORK/legacy"

docker network create "$NETWORK" >/dev/null
docker run -d --name "$MARIADB" --network "$NETWORK" -e MARIADB_ROOT_PASSWORD=root "$MARIADB_IMAGE" \
  --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --binlog-checksum=CRC32 >/dev/null

echo "waiting for $MARIADB_IMAGE"
for attempt in $(seq 1 90); do
  if docker exec "$MARIADB" mariadb -h127.0.0.1 -uroot -proot -e 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" = 90 ]; then
    docker logs "$MARIADB" 2>&1 | tail -30
    exit 1
  fi
  sleep 1
done
docker exec "$MARIADB" mariadb -h127.0.0.1 -uroot -proot -N -e \
  "SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_checksum"

phase() {
  echo "=== phase $1"
  docker run --rm --name "$TARANTOOL" --network "$NETWORK" \
    --user "$(id -u):$(id -g)" -e HOME=/tmp -e MIRRORFLAYER_DATA=/data \
    -v "$SOURCE:/src:ro" -v "$WORK/$2:/data" -w /src \
    --entrypoint env "$TARANTOOL_IMAGE" -u TT_APP_NAME -u TT_INSTANCE_NAME \
    tarantool test/scenario.lua "$1"
}

docker run --rm --entrypoint tarantool "$TARANTOOL_IMAGE" --version | head -1
phase load data
phase resume data
phase purge data
phase legacy legacy
echo "all phases passed"
