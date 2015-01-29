
REDIS_PREFIX = 'results'
REDIS_URL_RQ = 'redis://monarch.cs.washington.edu:6379/0'

SCHOOL = set(('monarch', 'recycle', 'bicycle', 'tricycle'))
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
            'rqdir': '/scratch/darioush/rq',
        },
}

def get_property(property, hostname, pid):
    for (group, fn, default) in config.get(property, []):
        if group is DEFAULT or hostname in group:
            if fn is not None and (hostname or pid):
                return fn(hostname, pid)
            else:
                return default
