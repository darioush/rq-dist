import json
import socket
import os

REDIS_PREFIX = 'results'
DATA_PREFIX = 'data'
TMP_PREFIX = 'temp'
KEYFILE = '/homes/gws/darioush/mykeypair.pem'

#REDIS_URL_RQ = 'redis://monarch.cs.washington.edu:6379/0'
REDIS_URL_RQ = 'redis://nest.cs.washington.edu:6379/0'
REDIS_URL_TG = 'redis://monarch.cs.washington.edu:6379/2'

SCHOOL  = lambda host: host in set(('monarch', 'recycle', 'bicycle', 'tricycle', 'godwit', 'buffalo', 'nest'))
MONARCH = lambda host: host in set(('monarch',))
NEST = lambda host: host in set(('nest',))
AWS     = lambda host: host.startswith('ip-')
DEFAULT = lambda host: True

config = {
    'work_dir': [
        (
            SCHOOL,
            lambda hostname, pid:
                '/scratch/darioush/worker.%s.%d' % (hostname, pid),
            '/scratch/darioush/worker.noname'
        ),
        (
            DEFAULT,
            lambda hostname, pid:
                '/tmp/worker.%s.%d' % (hostname, pid),
            '/tmp/worker.noname'
        )
    ],

    'd4j_path' : [
        (
            NEST,
            None,
            ['/homes/gws/darioush/defects4j/framework/bin'],
        ),
        (
            SCHOOL,
            None,
            ['/scratch/darioush/defects4j/framework/bin'],
        ),
        (
            AWS,
            None,
            ['/home/ec2-user/defects4j/framework/bin'],
        ),
        (
            DEFAULT,
            None,
            [],
        )
    ],

    's3_cache' : [
        (
            SCHOOL,
            None,
            ['/scratch/darioush/', '/scratch/darioush/cache'],
        ),
        (
            AWS,
            None,
            ['/tmp/cache'],
        ),
        (
            DEFAULT,
            None,
            [],
        )
    ],

    'redis_url': [
        (
            DEFAULT, None, 'redis://nest.cs.washington.edu:6379/1'
        )
    ],

}

def get_aws_info():
    fn = 'hosts.json'
    with open(fn) as f:
        return json.loads(f.read())

def workers(machine):
    known = {
        'monarch': {
            'hostname': 'monarch.cs.washington.edu',
            'rqdir': '/scratch/darioush/rq',
        },

        'recycle': {
            'hostname': 'recycle.cs.washington.edu',
            'rqdir': '/scratch/darioush/rq',
        },

        'bicycle': {
            'hostname': 'bicycle.cs.washington.edu',
            'rqdir': '/scratch/darioush/rq',
        },

        'tricycle': {
            'hostname': 'tricycle.cs.washington.edu',
            'rqdir': '/scratch/darioush/yolo/rq',
        },

        'godwit': {
            'hostname': 'godwit.cs.washington.edu',
            'rqdir': '/scratch/darioush/rq',
        },

        'buffalo': {
            'hostname': 'buffalo.cs.washington.edu',
            'rqdir': '/scratch/darioush/rq',
        },

        'nest': {
            'hostname': 'nest.cs.washington.edu',
            'rqdir': '/scratch/darioush/rq',
        },
    }
    if machine in known:
        return known[machine]

    if machine.startswith('ip-'):
        aws_info = get_aws_info()
        this_machine = [item for item in aws_info if item['private'].partition('.')[0] == machine]
        assert len(this_machine) == 1
        this_machine = this_machine[0]
        return {
            'hostname': this_machine['public'],
            'rqdir': '/home/ec2-user/rq',
            'kwargs': {
                'keyfile': KEYFILE,
                'user': 'ec2-user',
            }
        }


def get_property(property, hostname=None, pid=None):
    for (group, fn, default) in config.get(property, []):
        if group(hostname):
            if fn is not None and (hostname or pid):
                return fn(hostname, pid)
            else:
                return default

def get_property_defaults(property):
    hostname, _, _ = socket.gethostname().partition('.')
    pid = os.getpid()
    return get_property(property, hostname, pid)

