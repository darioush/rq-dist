
for host in `python manager.py listhosts` ; do python manager.py killall $host; done
