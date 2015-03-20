from cvgmeasure.common import mk_key

def gen(r, project, v, tail_key, suite, ts):
    key = mk_key('passcnt', [project, v, suite])
    fail_key = mk_key('fail', ['exec', project, v, suite])
    cnts = [int(cnt) if cnt is not None else 0 for cnt in r.hmget(key, *ts)]

    passed_enough = [test for (test, cnt) in zip(ts, cnts) if cnt >= 3]
    failed_removed = [test for test in passed_enough if not r.sismember(fail_key, test)]

    return failed_removed

def has_gen_suite(r, project, v, suite, arg, ts):
    key = mk_key('fetch-result', [project, v, suite])
    if r.get(key) == 'ok':
        # proceed with all tests in the bundle
        return ts
    else:
        # none
        return []
