#! /bin/sh

cd /homes/gws/darioush/t/
source ./env/bin/activate
./aws-setup.py
for m in `python aws-list-all.py hosts.json`; do
    python manager.py spawn $m 5;
done;
