#! /bin/bash
set -e

q="${1:-timeout}"
echo $q
./requeue.py -l | awk '{print $1}' > jobfiles/to
wc -l jobfiles/to
./requeue.py -J jobfiles/to - -q $q --commit
