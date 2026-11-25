#!/bin/sh
# Docker test: MariaDB or MySQL with a row-based binlog, or PostgreSQL with
# wal_level=logical, and Tarantool 3 running Tests/MirrorFlayerTest.lua or
# Tests/PostgreSQLTest.lua in phases.
#
#   SERVER=mariadb|galera|mysql|postgresql
#                          source (images mariadb:11.4, the same as a node of
#                          Galera, mysql:8.4, postgres:17; SERVER_IMAGE overrides)
#   THREAD=1               the replicators run their source side in an application
#                          thread (needs TARANTOOL_IMAGE of Tarantool 3.9 or later)
#   TLS=off|on|ca          replicator connection: plain, TLS without verification,
#                          TLS verified against the CA of the server
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
THREAD=${THREAD:-0}
TARANTOOL_IMAGE=${TARANTOOL_IMAGE:-tarantool/tarantool:3}
NETWORK=mirrorflayer-net
DATABASE=mirrorflayer-server
TARANTOOL=mirrorflayer-tarantool
BUILD=mirrorflayer-build

case "$SERVER" in
  mariadb)
    SERVER_IMAGE=${SERVER_IMAGE:-mariadb:11.4}
    CLIENT=mariadb
    TEST=Tests/MirrorFlayerTest.lua
    set -- -e MARIADB_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --binlog-checksum=CRC32
    ;;
  galera)
    SERVER_IMAGE=${SERVER_IMAGE:-mariadb:11.4}
    CLIENT=mariadb
    TEST=Tests/MirrorFlayerTest.lua
    # A cluster of one node, bootstrapped by itself
    set -- -e MARIADB_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL --binlog-checksum=CRC32 \
      --log-slave-updates --innodb-autoinc-lock-mode=2 --wsrep-on=ON --wsrep-provider=/usr/lib/galera/libgalera_smm.so \
      --wsrep-cluster-address=gcomm:// --wsrep-cluster-name=mirrorflayer
    ;;
  mysql)
    SERVER_IMAGE=${SERVER_IMAGE:-mysql:8.4}
    CLIENT=mysql
    TEST=Tests/MirrorFlayerTest.lua
    set -- -e MYSQL_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --gtid-mode=ON --enforce-gtid-consistency=ON --binlog-row-image=FULL --binlog-checksum=CRC32
    ;;
  postgresql)
    SERVER_IMAGE=${SERVER_IMAGE:-postgres:17}
    CLIENT=psql
    TEST=Tests/PostgreSQLTest.lua
    # A CA and a server certificate of its own, made before the server starts
    set -- -e POSTGRES_PASSWORD=root --entrypoint sh "$SERVER_IMAGE" -c '
      set -e
      mkdir -p /certs && cd /certs
      openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=mirrorflayer-ca -keyout ca.key -out ca.pem 2>/dev/null
      openssl req -newkey rsa:2048 -nodes -subj /CN=mirrorflayer-server -keyout server.key -out server.csr 2>/dev/null
      openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -days 2 -out server.pem 2>/dev/null
      chown postgres:postgres server.key server.pem && chmod 600 server.key
      exec docker-entrypoint.sh postgres -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 \
        -c ssl=on -c ssl_cert_file=/certs/server.pem -c ssl_key_file=/certs/server.key'
    ;;
  *)
    echo "SERVER must be mariadb, galera, mysql or postgresql" >&2
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

# A query over TCP: the temporary server of the first start of an image
# listens on the Unix socket only
sql() {
  if [ "$SERVER" = postgresql ]; then
    docker exec -e PGPASSWORD=root "$DATABASE" psql -h 127.0.0.1 -U postgres -At -c "$1"
  else
    docker exec "$DATABASE" $CLIENT -h127.0.0.1 -uroot -proot -N -e "$1"
  fi
}

echo "waiting for $SERVER_IMAGE"
for attempt in $(seq 1 120); do
  if sql 'SELECT 1' >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" = 120 ]; then
    docker logs "$DATABASE" 2>&1 | tail -30
    exit 1
  fi
  sleep 1
done
if [ "$SERVER" = postgresql ]; then
  sql "SELECT version(), current_setting('wal_level'), current_setting('ssl')"
  docker exec "$DATABASE" cat /certs/ca.pem > "$WORK/certs/ca.pem"
else
  sql "SELECT VERSION(), @@log_bin, @@binlog_format, @@binlog_row_image, @@binlog_checksum" 2>/dev/null
  if [ "$TLS" = ca ]; then
    docker exec "$DATABASE" cat /var/lib/mysql/ca.pem > "$WORK/certs/ca.pem"
  fi
fi

phase() {
  echo "=== phase $1 ($SERVER, TLS $TLS, thread $THREAD)"
  docker run --rm --name "$TARANTOOL" --network "$NETWORK" \
    --user "$(id -u):$(id -g)" -e HOME=/tmp -e MIRRORFLAYER_DATA=/data \
    -e SERVER="$SERVER" -e TLS="$TLS" -e THREAD="$THREAD" -e SERVER_HOST="$DATABASE" \
    -v "$SOURCE:/src:ro" -v "$WORK/$2:/data" -v "$WORK/certs:/certs:ro" -w /src \
    -v "$SOURCE/Source:/usr/share/tarantool/MirrorFlayer:ro" \
    -v "$SOURCE/Source:/usr/lib/x86_64-linux-gnu/tarantool/MirrorFlayer:ro" \
    --entrypoint env "$TARANTOOL_IMAGE" -u TT_APP_NAME -u TT_INSTANCE_NAME \
    tarantool "$TEST" "$1"
}

docker run --rm --entrypoint tarantool "$TARANTOOL_IMAGE" --version | head -1
phase load data
phase resume data
phase purge data
phase late late
echo "all phases passed ($SERVER_IMAGE, TLS $TLS, thread $THREAD)"
