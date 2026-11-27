#!/bin/sh
# Copies the working tree to a Docker host and runs Tests/DockerTest.sh there:
#   Tests/RemoteTest.sh host
# SSH overrides the ssh command; SERVER, SERVER_IMAGE, TLS, THREAD, COMPRESS, BINLOG_COMPRESS, CLEARTEXT,
# ROW_IMAGE, PARTIAL_JSON, GTID, GSS and TARANTOOL_IMAGE go to DockerTest.sh
set -eu

HOST=${1:?usage: Tests/RemoteTest.sh host}
SSH=${SSH:-ssh}
REMOTE=mirrorflayer-test

cd "$(dirname "$0")/.."
$SSH "$HOST" "rm -rf $REMOTE/src && mkdir -p $REMOTE/src"
COPYFILE_DISABLE=1 tar --no-xattrs --exclude=.git --exclude=.work --exclude=Source/Support.so -cf - . | $SSH "$HOST" "tar -xf - -C $REMOTE/src"
status=0
$SSH "$HOST" "cd $REMOTE/src && SERVER=${SERVER:-mariadb} SERVER_IMAGE=${SERVER_IMAGE:-} TLS=${TLS:-off} THREAD=${THREAD:-0} COMPRESS=${COMPRESS:-1} BINLOG_COMPRESS=${BINLOG_COMPRESS:-0} CLEARTEXT=${CLEARTEXT:-0} ROW_IMAGE=${ROW_IMAGE:-FULL} PARTIAL_JSON=${PARTIAL_JSON:-0} GTID=${GTID:-1} GSS=${GSS:-0} TARANTOOL_IMAGE=${TARANTOOL_IMAGE:-tarantool/tarantool:3} WORK=\$HOME/$REMOTE/work sh Tests/DockerTest.sh" || status=$?
$SSH "$HOST" "rm -rf $REMOTE"
exit $status
