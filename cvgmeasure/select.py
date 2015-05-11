from cvgmeasure.analyze import minimization
from cvgmeasure.common import job_decorator_out, check_key

#def minimization(r, rr, conn, qm_name, project, version, bases, augs):

@job_decorator_out
def m(r, rr, rrr, work_dir, input):
    project     = input['project']
    version     = input['version']
    qm          = input['qm']
    granularity = input['granularity']
    bases       = input['bases'].split('.')
    pools       = input['pools'].split('.')
    redo        = input.get('redo', False)

    s_bases = '.'.join(sorted(bases))
    s_pools = '.'.join(sorted(pools))

    with check_key(
        rrr,
        'out',
        [qm, granularity, s_bases, s_pools, project, version, 'GREQ', 100],
        redo=redo,
        other_keys=[],
        split_at=-2, # split it at version
    ) as done:
        minimization(r, rr, rrr, qm, granularity, project, version, bases, pools)
    return "Success. :)"
