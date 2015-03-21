#! /usr/bin/env python
import json
import boto.ec2.cloudwatch as cw

from datetime import datetime, timedelta
from rq import Connection, Worker
from redis import StrictRedis

from cvgmeasure.conf import REDIS_URL_RQ

def is_local(m, worker):
    machine, _, _ = worker.partition('.')
    return machine == m

def main():
    mycw = cw.connect_to_region('us-west-2')
    with open('hosts.json') as f:
        hosts = json.loads(f.read())
    now = datetime.utcnow()
    all_workers = Worker.all()
    busy_workers = [w for w in all_workers if w.get_state() == 'busy']
    idle_workers = [w for w in all_workers if w.get_state() == 'idle']

    for host in hosts:
        myl = sorted(mycw.get_metric_statistics(
            period=5*60,
            metric_name='CPUUtilization',
            namespace='AWS/EC2',
            statistics=['Average'],
            dimensions={"InstanceId": host['id']},
            unit='Percent',
            start_time=now-timedelta(hours=1),
            end_time=now,
            ),
            key=lambda x: x['Timestamp'],
            reverse=True)
        cpu_usage = '  '.join('%2d' % int(info['Average']) for info in myl)
        host_name, _, _ = host['private'].partition('.')
        id = host['id']
        busy_workers_local = [w for w in busy_workers if is_local(host_name, w.name)]
        idle_workers_local = [w for w in idle_workers if is_local(host_name, w.name)]
        newest_busy_worker = sorted(busy_workers_local, key=lambda w: w.birth_date, reverse=True)[:1]
        newest_time = [(datetime.utcnow()-w.birth_date).total_seconds() for w in newest_busy_worker]
        newest_time_fmt = ['{mins}'.format(mins=int(dt/60.0)) for dt in newest_time]
        print '{host_name} {idle} {busy} {newest} {newest_lt_10}\t{cpu_usage}'.format(
                idle=len(idle_workers_local),
                busy=len(busy_workers_local),
                host_name=host_name,
                newest=''.join(newest_time_fmt),
                newest_lt_10='*' if [t for t in newest_time if t < 10*60] else '-',
                cpu_usage=cpu_usage
            )


if __name__ == "__main__":
    with Connection(connection=StrictRedis.from_url(REDIS_URL_RQ)):
        main()

