#! /usr/bin/env python

import click
import time
import json
import boto
import boto.ec2.cloudwatch as cw
import traceback

from datetime import datetime, timedelta
from rq import Connection, Worker
from redis import StrictRedis
from plumbum import SshMachine

from cvgmeasure.common import connect_to_redis
from cvgmeasure.conf import REDIS_URL_RQ, KEYFILE

SCALE_DELTA = 10
CPU_AUTOSCALE_UP = 70
WORKERS_MAX = 27

SSH_OPTS=('-o', 'StrictHostKeyChecking=no')

def is_local(m, worker):
    machine, _, _ = worker.partition('.')
    return machine == m

def refresh(interval, func, *args):
    while True:
        if interval:
            click.clear()
        try:
            func(*args)
        except:
            print traceback.format_exc()

        if interval:
            time.sleep(interval)
        else:
            break

def main(r, mycw):
    refresh(60, printinfo, r, mycw)

def run_more_instances(machine, count, queues=['high', 'default', 'low']):
    rem = SshMachine(machine, ssh_opts=SSH_OPTS, keyfile=KEYFILE, user='ec2-user')
    dir = rem.path('/home/ec2-user/rq')

    with rem.cwd(dir):
        for i in xrange(0, count):
            rem["./worker.sh"](' '.join(queues))
            print "Worker spawned"


ec2 = boto.ec2.connect_to_region('us-west-2')
def write_hosts_json():
    instances = ec2.get_only_instances(filters={'instance-state-name':'running'}) #filters={'tag:runner': "true"})
    with open('hosts.json', 'w') as f:
        f.write(json.dumps([{"id": instance.id,
        "public": instance.public_dns_name,
        "private": instance.private_dns_name,
        "placement": instance.placement,
        "ip": instance.ip_address,
        } for instance in instances], indent=1))

def printinfo(r, mycw):
    write_hosts_json()
    with open('hosts.json') as f:
        hosts = json.loads(f.read())
    now = datetime.utcnow()
    all_workers = Worker.all()
    busy_workers = [w for w in all_workers if w.get_state() == 'busy']
    idle_workers = [w for w in all_workers if w.get_state() in ['idle', 'suspended']]


    for i, host in enumerate(hosts):
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
        cpu_usages = [int(info['Average']) for info in myl]
        cpu_usage = ' '.join(['%2d' % avg for avg in cpu_usages])
        host_name, _, _ = host['private'].partition('.')
        id = host['id']
        busy_workers_local = [w for w in busy_workers if is_local(host_name, w.name)]
        idle_workers_local = [w for w in idle_workers if is_local(host_name, w.name)]
        newest_busy_worker = sorted(busy_workers_local, key=lambda w: w.birth_date if w.birth_date is not None
                else datetime(year=1980, month=1, day=1), reverse=True)[:1]
        newest_time = [(datetime.utcnow()-(w.birth_date if w.birth_date is not None
            else datetime(year=1980, month=1, day=1))).total_seconds() for w in newest_busy_worker]
        newest_time_fmt = ['{mins}'.format(mins=min(int(dt/60.0),999)) for dt in newest_time]
        newest_lt_10 = len([t for t in newest_time if t < 10*60]) > 0

        should_autoscale_up_conditions = (
            not newest_lt_10,                             # not if newly spawned a worker
            len(busy_workers_local) > 0,                  # not if everything is idle
            len(idle_workers_local) < SCALE_DELTA,        # or a bunch of idle workers already laying around
            len(cpu_usages) >= 2,                         # some history exists
            len([cpu for cpu in cpu_usages[:1] if cpu < CPU_AUTOSCALE_UP]) > 0, # cpu usage below threshold
            len(busy_workers_local + idle_workers_local) < WORKERS_MAX, # below capacity of total wokers
        )
        should_autoscale_up = all(should_autoscale_up_conditions)


        print '{idx:>2} {host_name:20} {idle:>2} {busy:>2} {newest:>3} {newest_lt_10} {scale} : {cpu_usage:2}'.format(
                idx=i,
                idle=len(idle_workers_local),
                busy=len(busy_workers_local),
                host_name=host_name,
                newest=''.join(newest_time_fmt),
                newest_lt_10='*' if newest_lt_10 else '-',
                cpu_usage=cpu_usage,
                scale='^' if should_autoscale_up else ' ',
            )

        if should_autoscale_up:
            current = len(busy_workers_local + idle_workers_local)
            new_max = min(current + SCALE_DELTA, WORKERS_MAX)
            new = max(new_max - current, 0)
            run_more_instances(host['public'], new)
    print '---'

if __name__ == "__main__":
    with connect_to_redis(REDIS_URL_RQ) as r:
        with Connection(connection=r):
            mycw = cw.connect_to_region('us-west-2')
            main(r, mycw)

