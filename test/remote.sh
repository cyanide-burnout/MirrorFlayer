#!/bin/sh
# Copies the working tree to a Docker host and runs test/run.sh there:
#   test/remote.sh [host]    (SSH overrides the ssh command)
set -eu

HOST=${1:?usage: $0 host}
SSH=${SSH:-ssh}
REMOTE=mirrorflayer-test

cd "$(dirname "$0")/.."
$SSH "$HOST" "rm -rf $REMOTE/src && mkdir -p $REMOTE/src"
COPYFILE_DISABLE=1 tar --exclude=.git --exclude=.work -cf - . | $SSH "$HOST" "tar -xf - -C $REMOTE/src"
status=0
$SSH "$HOST" "cd $REMOTE/src && WORK=\$HOME/$REMOTE/work sh test/run.sh" || status=$?
$SSH "$HOST" "rm -rf $REMOTE"
exit $status
