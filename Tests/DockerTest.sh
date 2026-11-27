#!/bin/sh
# Docker test: MariaDB or MySQL with a row-based binlog, or PostgreSQL with
# wal_level=logical, and Tarantool 3 running Tests/MirrorFlayerTest.lua or
# Tests/PostgreSQLTest.lua in phases.
#
#   SERVER=mariadb|galera|mysql|postgresql
#                          source (images mariadb:11.8, the same as a node of
#                          Galera, mysql:8.4, postgres:17; SERVER_IMAGE overrides)
#   THREAD=1               the replicators run their source side in an application
#                          thread (needs TARANTOOL_IMAGE of Tarantool 3.9 or later)
#   TLS=off|on|ca          replicator connection: plain, TLS without verification,
#                          TLS verified against the CA of the server
#   COMPRESS=0             no compressed protocol of MySQL and MariaDB (on by default)
#   BINLOG_COMPRESS=1      MariaDB compresses its binlog events (log_bin_compress),
#                          MySQL its transactions (binlog_transaction_compression)
#   ROW_IMAGE=FULL|MINIMAL|NOBLOB|FULL_NODUP
#                          binlog_row_image of the source (FULL by default,
#                          FULL_NODUP is MariaDB only)
#   PARTIAL_JSON=1         MySQL logs JSON updates as diffs (binlog_row_value_options)
#   GTID=0                 MySQL without GTIDs: the position is a binlog file and offset
#   CLEARTEXT=1            the PAM user of MariaDB logs in with mysql_clear_password
#                          instead of dialog
#
# MariaDB gets users of the authentication plugins ed25519, parsec (11.6 and
# later) and pam (pam_unix with a system user of the same name), password
# secret; the test logs in with each of them (AUTH)
#
# Support.so is built in a container of the Tarantool image; Source/ is mounted
# where make install puts the Lua modules and Support.so. Containers are named
# mirrorflayer-* and removed afterwards, the work directory too. The TT_*
# variables of the official image switch Tarantool to its cluster config, so
# they are unset.
set -eu

cd "$(dirname "$0")/.."
SOURCE=$(pwd)
WORK=${WORK:-$SOURCE/.work}
SERVER=${SERVER:-mariadb}
TLS=${TLS:-off}
COMPRESS=${COMPRESS:-1}
BINLOG_COMPRESS=${BINLOG_COMPRESS:-0}
CLEARTEXT=${CLEARTEXT:-0}
ROW_IMAGE=${ROW_IMAGE:-FULL}
PARTIAL_JSON=${PARTIAL_JSON:-0}
GTID=${GTID:-1}
AUTH=
THREAD=${THREAD:-0}
TARANTOOL_IMAGE=${TARANTOOL_IMAGE:-tarantool/tarantool:3}
NETWORK=mirrorflayer-net
DATABASE=mirrorflayer-server
TARANTOOL=mirrorflayer-tarantool
BUILD=mirrorflayer-build

# MariaDB compresses its events of 10 bytes and more
COMPRESSION=
if [ "$BINLOG_COMPRESS" = 1 ]; then
  COMPRESSION="--log-bin-compress=ON --log-bin-compress-min-len=10"
fi

# PAM of MariaDB: dialog, or mysql_clear_password with CLEARTEXT=1
PAM="--plugin-load-add=auth_pam --pam-use-cleartext-plugin=OFF"
if [ "$CLEARTEXT" = 1 ]; then
  PAM="--plugin-load-add=auth_pam --pam-use-cleartext-plugin=ON"
fi

case "$SERVER" in
  mariadb)
    SERVER_IMAGE=${SERVER_IMAGE:-mariadb:11.8}
    CLIENT=mariadb
    TEST=Tests/MirrorFlayerTest.lua
    set -- -e MARIADB_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=$ROW_IMAGE --binlog-checksum=CRC32 $COMPRESSION $PAM
    ;;
  galera)
    SERVER_IMAGE=${SERVER_IMAGE:-mariadb:11.8}
    CLIENT=mariadb
    TEST=Tests/MirrorFlayerTest.lua
    # A cluster of one node, bootstrapped by itself
    set -- -e MARIADB_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=$ROW_IMAGE --binlog-checksum=CRC32 $COMPRESSION $PAM \
      --log-slave-updates --innodb-autoinc-lock-mode=2 --wsrep-on=ON --wsrep-provider=/usr/lib/galera/libgalera_smm.so \
      --wsrep-cluster-address=gcomm:// --wsrep-cluster-name=mirrorflayer
    ;;
  mysql)
    SERVER_IMAGE=${SERVER_IMAGE:-mysql:8.4}
    CLIENT=mysql
    TEST=Tests/MirrorFlayerTest.lua
    if [ "$BINLOG_COMPRESS" = 1 ]; then
      COMPRESSION=--binlog-transaction-compression=ON
    fi
    if [ "$PARTIAL_JSON" = 1 ]; then
      COMPRESSION="$COMPRESSION --binlog-row-value-options=PARTIAL_JSON"
    fi
    MODE="--gtid-mode=ON --enforce-gtid-consistency=ON"
    if [ "$GTID" = 0 ]; then
      MODE="--gtid-mode=OFF --enforce-gtid-consistency=OFF"
    fi
    set -- -e MYSQL_ROOT_PASSWORD=root "$SERVER_IMAGE" \
      --server-id=1 $MODE --binlog-row-image=$ROW_IMAGE --binlog-checksum=CRC32 \
      $COMPRESSION
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
        -c logical_decoding_work_mem=64kB \
        -c ssl=on -c ssl_cert_file=/certs/server.pem -c ssl_key_file=/certs/server.key'
    ;;
  *)
    echo "SERVER must be mariadb, galera, mysql or postgresql" >&2
    exit 2
    ;;
esac

# The image the test runs in: the one of Tarantool with the libraries
# Support.so needs beyond it (libsodium)
RUN_IMAGE=mirrorflayer-run:$$

cleanup() {
  docker rm -f -v "$DATABASE" "$TARANTOOL" "$BUILD" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  docker rmi "$RUN_IMAGE" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}

cleanup
trap cleanup EXIT INT TERM
mkdir -p "$WORK/data" "$WORK/late" "$WORK/certs"

echo "building Support.so"
printf 'FROM %s\nRUN apt-get update -qq >/dev/null && apt-get install -y -qq libsodium23 >/dev/null && rm -rf /var/lib/apt/lists/*\n' \
  "$TARANTOOL_IMAGE" | docker build -q -t "$RUN_IMAGE" - >/dev/null

docker run --rm --name "$BUILD" -v "$SOURCE:/src" -w /src --entrypoint sh "$TARANTOOL_IMAGE" -c \
  "apt-get update -qq >/dev/null 2>&1; apt-get install -y -qq gcc make libssl-dev zlib1g-dev libzstd-dev libsodium-dev pkg-config >/dev/null 2>&1; make SUPPORT=1 && chown $(id -u):$(id -g) Source/Support.so"

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
  if [ "$BINLOG_COMPRESS" = 1 ] && [ "$CLIENT" = mariadb ]; then
    sql "SELECT @@log_bin_compress, @@log_bin_compress_min_len" 2>/dev/null
  elif [ "$BINLOG_COMPRESS" = 1 ]; then
    sql "SELECT @@binlog_transaction_compression, @@binlog_transaction_compression_level_zstd" 2>/dev/null
  fi
  if [ "$CLIENT" = mariadb ]; then
    docker exec "$DATABASE" sh -c 'useradd -M auth_pam && echo auth_pam:secret | chpasswd &&
      printf "auth required pam_unix.so\naccount required pam_unix.so\n" > /etc/pam.d/mariadb'
    sql "INSTALL SONAME 'auth_ed25519'" 2>/dev/null
    sql "CREATE USER auth_ed25519 IDENTIFIED VIA ed25519 USING PASSWORD('secret')" 2>/dev/null
    sql "CREATE USER auth_pam IDENTIFIED VIA pam USING 'mariadb'" 2>/dev/null
    AUTH=ed25519,pam
    if sql "INSTALL SONAME 'auth_parsec'" 2>/dev/null; then
      sql "CREATE USER auth_parsec IDENTIFIED VIA parsec USING PASSWORD('secret')" 2>/dev/null
      AUTH=$AUTH,parsec
    fi
    echo "users of the authentication plugins: $AUTH"
  fi
  if [ "$TLS" = ca ]; then
    docker exec "$DATABASE" cat /var/lib/mysql/ca.pem > "$WORK/certs/ca.pem"
  fi
fi

phase() {
  echo "=== phase $1 ($SERVER, TLS $TLS, thread $THREAD, compress $COMPRESS, binlog compress $BINLOG_COMPRESS, row image $ROW_IMAGE, partial JSON $PARTIAL_JSON, GTID $GTID)"
  docker run --rm --name "$TARANTOOL" --network "$NETWORK" \
    --user "$(id -u):$(id -g)" -e HOME=/tmp -e MIRRORFLAYER_DATA=/data \
    -e SERVER="$SERVER" -e TLS="$TLS" -e THREAD="$THREAD" -e COMPRESS="$COMPRESS" -e GTID="$GTID" -e AUTH="$AUTH" -e CLEARTEXT="$CLEARTEXT" -e SERVER_HOST="$DATABASE" \
    -v "$SOURCE:/src:ro" -v "$WORK/$2:/data" -v "$WORK/certs:/certs:ro" -w /src \
    -v "$SOURCE/Source:/usr/share/tarantool/MirrorFlayer:ro" \
    -v "$SOURCE/Source:/usr/lib/x86_64-linux-gnu/tarantool/MirrorFlayer:ro" \
    --entrypoint env "$RUN_IMAGE" -u TT_APP_NAME -u TT_INSTANCE_NAME \
    tarantool "$TEST" "$1"
}

docker run --rm --entrypoint tarantool "$TARANTOOL_IMAGE" --version | head -1
phase load data
phase resume data
phase purge data
phase late late
echo "all phases passed ($SERVER_IMAGE, TLS $TLS, thread $THREAD, compress $COMPRESS, binlog compress $BINLOG_COMPRESS, row image $ROW_IMAGE, partial JSON $PARTIAL_JSON, GTID $GTID)"
