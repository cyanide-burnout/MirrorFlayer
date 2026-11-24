#!/bin/sh
# Copies the working tree to a Docker host and runs Tests/DockerTest.sh there:
#   Tests/RemoteTest.sh [host]
# SSH overrides the ssh command; SERVER, SERVER_IMAGE and TLS go to DockerTest.sh
set -eu

HOST=${1:?usage: Tests/RemoteTest.sh host}
SSH=${SSH:-ssh}
REMOTE=mirrorflayer-test

cd "$(dirname "$0")/.."
$SSH "$HOST" "rm -rf $REMOTE/src && mkdir -p $REMOTE/src"
COPYFILE_DISABLE=1 tar --no-xattrs --exclude=.git --exclude=.work --exclude=Source/TLS.so -cf - . | $SSH "$HOST" "tar -xf - -C $REMOTE/src"
status=0
$SSH "$HOST" "cd $REMOTE/src && SERVER=${SERVER:-mariadb} SERVER_IMAGE=${SERVER_IMAGE:-} TLS=${TLS:-off} WORK=\$HOME/$REMOTE/work sh Tests/DockerTest.sh" || status=$?
$SSH "$HOST" "rm -rf $REMOTE"
exit $status
