#! /bin/sh
set -e

for i in `python manager.py listhosts`; do
    cmd="python manager.py setup $i";
    echo $cmd;
    $cmd;
done
