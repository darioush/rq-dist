#! /usr/bin/env python

from flask import Flask, render_template
app = Flask(__name__)

import time

from rq import Connection, Queue, Worker
from rq.utils import current_timestamp
from rq.registry import StartedJobRegistry as Wip
from collections import defaultdict

from cvgmeasure.common import connect_to_redis
from cvgmeasure.conf import REDIS_URL_RQ

def is_local(addr):
    return any(map(addr.startswith, [
        '[::1]',
        '[2607:4000:200:13::3e]',
        '127.0.0.1',
        ]))


def get_queue_info(r, q_2_w, timestamp):
    def get_q_info(q):
        wip_key = 'rq:wip:{q.name}'.format(q=q)
        expired_count = r.zcount(wip_key, 0, timestamp-1)
        wip_count = r.zcount(wip_key, timestamp, '+inf')
        return {
                'q': q,
                'workers': q_2_w[q.name],
                'wip_count': wip_count,
                'expired_count': expired_count,
                'skip': all(x == 0 for x in (q.count, q_2_w[q.name], wip_count, expired_count)),
        }
    return [get_q_info(q) for q in Queue.all()]

@app.route("/")
@app.route("/info")
def hello():
    with connect_to_redis(REDIS_URL_RQ) as r:
        with Connection(connection=r):
            ### redis info
            client_list = r.client_list()
            local_clients = [cl for cl in client_list if is_local(cl['addr'])]
            remote_clients = [cl for cl in client_list if not is_local(cl['addr'])]

            ws = Worker.all()
            q_2_w = defaultdict(int)
            for w in ws:
                for qn in w.queue_names():
                    q_2_w[qn] += 1

            timestamp = current_timestamp()

            return render_template(
                'home.html',
                queues=get_queue_info(r, q_2_w, timestamp),
                info={
                    'timestamp': timestamp,
                    'local_clients': len(local_clients),
                    'remote_clients': len(remote_clients),
                    'workers': len(ws)
                },
            )

def run():
    app.debug = True
    app.run(host='0.0.0.0')


