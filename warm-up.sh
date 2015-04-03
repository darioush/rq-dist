for m in `python aws-list-all.py hosts.json `; do python manager.py spawn $m 10; done
