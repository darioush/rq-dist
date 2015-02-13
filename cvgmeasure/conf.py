
REDIS_PREFIX = 'results'
DATA_PREFIX = 'data'
REDIS_URL_RQ = 'redis://monarch.cs.washington.edu:6379/0'
REDIS_URL_TG = 'redis://monarch.cs.washington.edu:6379/2'

SCHOOL = set(('recycle', 'bicycle', 'tricycle', 'godwit', 'buffalo'))
MONARCH = set(('monarch',))
DEFAULT = object()

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
            SCHOOL,
            None,
            ['/scratch/darioush/defects4j/framework/bin'],
        ),
        (
            MONARCH,
            None,
            ['/homes/gws/darioush/defects4j/framework/bin'],
        ),
        (
            DEFAULT,
            None,
            [],
        )
    ],

    'redis_url': [
        (
            DEFAULT, None, 'redis://monarch.cs.washington.edu:6379/1'
        )
    ],

}

workers = {
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
}

def get_property(property, hostname=None, pid=None):
    for (group, fn, default) in config.get(property, []):
        if group is DEFAULT or hostname in group:
            if fn is not None and (hostname or pid):
                return fn(hostname, pid)
            else:
                return default
