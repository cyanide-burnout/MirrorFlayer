#!/bin/sh
# Docker test: MariaDB or MySQL with a row-based binlog and Tarantool 3 running
# Tests/MirrorFlayerTest.lua in phases.
#
#   SERVER=mariadb|mysql   source (images mariadb:11.4, mysql:8.4; SERVER_IMAGE overrides)
#   TLS=off|on|ca          replicator connection: plain, TLS without verification,
#                          TLS verified against the CA of the MySQL server
#
# TLS.so is built in a container of the Tarantool image; Source/ is mounted
# where make install puts the Lua modules and TLS.so. Containers are named
# mirrorflayer-* and removed afterwards, the work directory too. The TT_*
# variables of the official image switch Tarantool to its cluster config, so
# they are unset.
set -eu

cd "$(dirname "$0")/.."
SOURCE=$(pwd)
WORK=${WORK:-$SOURCE/.work}
SERVER=${SERVER:-mariadb}
TLS=${TLS:-off}
TARANTOOL_IMAGE=${TARANTOOL_IMAGE:-tarantool/tarantool:3}
NETWORK=mirrorflayer-net
DATABASE=mirrorflayer-server
TARANTOOL=mirrorflayer-tarantool
BUILD=mirrorflayer-build

case "$SERVER" in
  mariadb)
    SERVER_IMAGE=${SERVER_IMAGE:-mariadb:11.4}
    CLIENT=mariadb
    set -- -e MARIADB_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --binlog-checksum=CRC32
    ;;
  mysql)
    SERVER_IMAGE=${SERVER_IMAGE:-mysql:8.4}
    CLIENT=mysql
    set -- -e MYSQL_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-row-image=FULL --binlog-checksum=CRC32
    ;;
  *)
    echo "SERVER must be mariadb or mysql" >&2
    exit 2
    ;;
esac

cleanup() {
  docker rm -f -v "$DATABASE" "$TARANTOOL" "$BUILD" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}

cleanup
trap cleanup EXIT INT TERM
mkdir -p "$WORK/data" "$WORK/late" "$WORK/certs"

echo "building TLS.so"
docker run --rm --name "$BUILD" -v "$SOURCE:/src" -w /src --entrypoint sh "$TARANTOOL_IMAGE" -c \
  "apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq gcc make libssl-dev pkg-config >/dev/null 2>&1; make TLS=1 && chown $(id -u):$(id -g) Source/TLS.so"

docker network create "$NETWORK" >/dev/null
docker run -d --name "$DATABASE" --network "$NETWORK" "$@" >/dev/null

echo "waiting for $SERVER_IMAGE"
for attempt in $(seq 1 120); do
  if docker exec "$DATABASE" $CLIENT -h127.0.0.1 -uroot -proot -e 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" = 120 ]; then
    docker logs "$DATABASE" 2>&1 | tail -30
    exit 1
  fi
  sleep 1
done
docker exec "$DATABASE" $CLIENT -h127.0.0.1 -uroot -proot -N -e \
  "SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_checksum" 2>/dev/null

if [ "$TLS" = ca ]; then
  docker exec "$DATABASE" cat /var/lib/mysql/ca.pem > "$WORK/certs/ca.pem"
fi

phase() {
  echo "=== phase $1 ($SERVER, TLS $TLS)"
  docker run --rm --name "$TARANTOOL" --network "$NETWORK" \
    --user "$(id -u):$(id -g)" -e HOME=/tmp -e MIRRORFLAYER_DATA=/data \
    -e SERVER="$SERVER" -e TLS="$TLS" -e SERVER_HOST="$DATABASE" \
    -v "$SOURCE:/src:ro" -v "$WORK/$2:/data" -v "$WORK/certs:/certs:ro" -w /src \
    -v "$SOURCE/Source:/usr/share/tarantool/MirrorFlayer:ro" \
    -v "$SOURCE/Source:/usr/lib/x86_64-linux-gnu/tarantool/MirrorFlayer:ro" \
    --entrypoint env "$TARANTOOL_IMAGE" -u TT_APP_NAME -u TT_INSTANCE_NAME \
    tarantool Tests/MirrorFlayerTest.lua "$1"
}

docker run --rm --entrypoint tarantool "$TARANTOOL_IMAGE" --version | head -1
phase load data
phase resume data
phase purge data
phase late late
echo "all phases passed ($SERVER_IMAGE, TLS $TLS)"
