#!/bin/sh
# Copies the working tree to a Docker host and runs Tests/DockerTest.sh there:
#   Tests/RemoteTest.sh [host]    (SSH overrides the ssh command, MARIADB_IMAGE the server)
set -eu

HOST=${1:?usage: $0 host}
SSH=${SSH:-ssh}
REMOTE=mirrorflayer-test

cd "$(dirname "$0")/.."
$SSH "$HOST" "rm -rf $REMOTE/src && mkdir -p $REMOTE/src"
COPYFILE_DISABLE=1 tar --no-xattrs --exclude=.git --exclude=.work -cf - . | $SSH "$HOST" "tar -xf - -C $REMOTE/src"
status=0
$SSH "$HOST" "cd $REMOTE/src && MARIADB_IMAGE=${MARIADB_IMAGE:-mariadb:11.4} WORK=\$HOME/$REMOTE/work sh Tests/DockerTest.sh" || status=$?
$SSH "$HOST" "rm -rf $REMOTE"
exit $status
