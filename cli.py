#! /usr/bin/env python

import click
import time

from cvgmeasure.common import connect_to_redis
from cvgmeasure.conf import REDIS_URL_RQ
from rq import Connection, Queue
from rq.utils import current_timestamp
from rq.registry import StartedJobRegistry as Wip


def refresh(interval, func, *args):
    while True:
        if interval:
            click.clear()
        func(*args)
        if interval:
            time.sleep(interval)
        else:
            break

def main(r):
    refresh(3, printinfo, r)


def is_local(addr):
    return any(map(addr.startswith, [
        '[::1]',
        '[2607:4000:200:13::3e]',
        '127.0.0.1',
        ]))

def printinfo(r):
    timestamp = current_timestamp()
    click.echo('current time {0}'.format(timestamp))
    client_list = r.client_list()
    local_clients = [cl for cl in client_list if is_local(cl['addr'])]
    remote_clients = [cl for cl in client_list if not is_local(cl['addr'])]
    click.echo('Number of active Redis connections: {remote} (+ {local} local)'.format(
        remote=len(remote_clients),
        local=len(local_clients))
    )
    print "-------------"
    qs = Queue.all()
    for q in qs:
        if q.count == 0:
            continue
        wip_key = 'rq:wip:{q.name}'.format(q=q)
        expired_count = r.zcount(wip_key, 0, timestamp-1)
        wip_count = r.zcount(wip_key, timestamp, '+inf')
        line = "{q.name:20} {q.count:>10} {wip_count:>10} {expired_count:>10}".format(
            q=q,
            wip_count=wip_count,
            expired_count=expired_count,
        )

        click.echo(line)


if __name__ == "__main__":
    with connect_to_redis(REDIS_URL_RQ) as r:
        with Connection(connection=r):
            main(r)

