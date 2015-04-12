#! /usr/bin/env python
import sys
import json

from collections import defaultdict
from redis import StrictRedis
from optparse import OptionParser
from rq import Queue
from contextlib import contextmanager

from cvgmeasure.d4 import iter_versions, is_empty
from cvgmeasure.conf import get_property_defaults, REDIS_URL_RQ
from cvgmeasure.common import mk_key, i_tn_s, tn_i_s, chunks
from cvgmeasure.s3 import list_from_s3

BUNDLE_SIZE=50
BUNDLE_FN='bundlefiles/bundle.out'
EXTRA_OPTS=''
RESET_BUNDLE_FN=True

def check_int(idxes):
    assert(all(type(x) is int for x in idxes))

def b_tpvs(bundle, key='tests'):
    tool, = bundle[:1]
    return b_pvs(bundle[1:]) + ' -K cvg_tool -a {tool} '.format(tool=tool)

def b_pvs(bundle, key='tests'):
    project, version, suite = bundle[:3]
    return '-k {key} -s suite -T {suite} -p {project} -v {version}'.format(
            key=key, suite=suite,
            project=project, version=version
        )

class Straggler(Exception):
    def __init__(self, kind, bundle, idxes, fix=None):
        check_int(idxes)
        msg = '-- {kind} {bundle} [{idxes}] {fixable}'.format(
                kind=kind,
                bundle=':'.join(map(str, bundle)),
                idxes=', '.join(map(str, idxes)),
                fixable='' if fix is None else '(fixable)'
            )
        self.kind, self.bundle, self.idxes, self.fix = kind, bundle, idxes, fix
        return super(Straggler, self).__init__(msg)

    def can_fix(self):
        return self.fix is not None

    def do_fix(self):
        fun_dotted, bundle_fun, params = self.fix
        my_bundle = bundle_fun(self.bundle)
        print fun_dotted, params
        sys.stderr.write((
                'python main.py qb-slice {fun_dotted} -b {bundle_size} ' +
                ' -S file:{bundle_fn}  -M cvgmeasure.common.M ' +
                ' {params} {extra_opts}\n').format(
                        fun_dotted=fun_dotted,
                        bundle_size=BUNDLE_SIZE,
                        bundle_fn=BUNDLE_FN,
                        params=params(my_bundle),
                        extra_opts=EXTRA_OPTS,
                ))
        with open(BUNDLE_FN, 'a') as  f:
            f.write(json.dumps({':'.join(map(str, ['file'] + my_bundle)): self.idxes}))
            f.write('\n')
        return 'Good'


@contextmanager
def run_with_fixes():
    try:
        yield
    except Straggler as s:
        if s.can_fix():
            s.do_fix()

def check_test_list(r, project, v, suite):
    key = mk_key('tms', [project, v, suite])
    test_list = r.llen(key)
    if test_list == 0 and suite != 'dev':
        ## empty test list should correspond to
        ## a bad fetch result
        fetch_result_key = mk_key('fetch', [project, v])
        fetch_result = r.hget(fetch_result_key, suite)
        if fetch_result not in ['missing', 'empty']:
            raise Straggler('-- {project}:{v}:{suite}'.format(project=project, v=v, suite=suite))
        else:
            print '-- {project}:{v}:{suite} has fetch result {fetch}'.format(project=project, v=v, suite=suite,
                    fetch=fetch_result)
            return [], []

    print '{key}: {tl}'.format(key=key, tl=test_list)

    test_list = r.lrange(key, 0, -1)
    test_names = i_tn_s(r, test_list, suite)
    assert len(test_list) == len(test_names)
    return map(int, test_list), test_names

def cobertura_covers(r, project, v, suite, idxes):
    return covers(r, 'cobertura', project, v, suite, idxes)

def covers(r, tool, project, v, suite, idxes):
    lookup_key = mk_key('exec', [tool, project, v, suite])
    lookup_results = [] if len(idxes) == 0 else r.hmget(lookup_key, *idxes)
    assert all(x is not None for x in lookup_results)
    assert len(lookup_results) == len(idxes)
    covers = map(int, [idx for (idx, lookup_result)
                in zip(idxes, lookup_results)
                if not is_empty(tool, json.loads(lookup_result))])
    return covers


def get_fails(r, tool, project, v, suite, idxes):
    check_int(idxes)
    [lookup_key_tool, lookup_key_exec] = [mk_key('fail', [T, project, v, suite]) for T in (tool, 'exec')]
    [failed_tool, failed_exec] = [set(map(int, list(r.smembers(K)))) for K in (lookup_key_tool, lookup_key_exec)]

    failed_idxes = [idx for idx in idxes if idx in failed_tool]
    failed_but_not_exec = [idx for idx in failed_idxes if idx not in failed_exec]
    if failed_but_not_exec:
        pass_key = mk_key('passcnt', [project, v, suite])
        passes = defaultdict(int, zip(idxes, map(lambda x: 0 if x is None else int(x), r.hmget(pass_key, *failed_but_not_exec))))
        failed_but_known_to_pass = [idx for idx in failed_but_not_exec if passes[idx] > 0]
        if failed_but_known_to_pass:
            raise Straggler('-- {project}:{v}:{suite}: {idxes} have failed but are known to pass'.format(project=project, v=v, suite=suite,
                idxes=' '.join(map(str, failed_but_known_to_pass)))
            )

        raise Straggler('FAIL_BUT_NOT_EXEC', [tool, project, v, suite], failed_but_not_exec,
                fix=('cvgmeasure.cvg.run_tests', lambda bundle: bundle[1:], b_pvs)
            )
    return failed_idxes


def check_cvg(r, tool, project, v, suite, t_idxs, ts):
    key = mk_key('exec', [tool, project, v, suite])
    cvg_infos = r.hmget(key, *t_idxs)
    assert len(cvg_infos) == len(t_idxs)

    nils = [(t_idx, t) for (t_idx, t, cvg_info) in zip(t_idxs, ts, cvg_infos) if cvg_info is None]
    print len(nils), len(ts)
    if suite == 'dev':
        check_for_nil_classes = [tc for tc, _, _ in [t.partition('::') for (t_idx, t) in nils]]
        check_for_nil_classes = list(set(check_for_nil_classes))
        check_for_nil_classes_idxes = tn_i_s(r, check_for_nil_classes, 'dev')
        non_nil_classes_idxes = set(cobertura_covers(r, project, v, suite, check_for_nil_classes_idxes))
        nil_class_dict = {class_name: idx not in non_nil_classes_idxes for (class_name, idx)
                            in zip(check_for_nil_classes, check_for_nil_classes_idxes)}
        nil_idxes = [(t_idx_, tc, tm) for (t_idx_, (tc, _, tm)) in [(t_idx, t.partition('::')) for (t_idx, t) in nils] if
                            nil_class_dict.get(tc) is False]
        # really only need the idxes
        nil_idxes = [t_idx for (t_idx, _, _) in nil_idxes]

    else:
        nil_idxes = [t_idx for (t_idx, _) in nils]

    if tool == 'cobertura' and nil_idxes:
        raise Straggler('COBERTURA_NOT_RUN', [project, v, suite],
                idxes=nil_idxes,
                fix=(
                    'cvgmeasure.cvg.do_cvg',
                    lambda bundle: bundle,
                    lambda bundle: b_pvs(bundle) + " -K cvg_tool -a cobertura"
                ))

    cc = cobertura_covers(r, project, v, suite, nil_idxes)
    if cc != []:
        raise Straggler('-- {project}:{v}:{suite} -- [{idxes}] should have been 0 coverage by cobertura'.format(
            project=project,v=v,suite=suite,
            idxes=' '.join(map(str,cc))))

    # time to check s3
    non_nils = [(t_idx, t) for (t_idx, t, cvg_info) in zip(t_idxs, ts, cvg_infos) if cvg_info is not None and not is_empty(tool, json.loads(cvg_info))]
    print '- non-nil len: {0}'.format(len(non_nils))
    if non_nils:
        s3_list = list_from_s3('cvg-files', [tool, project, v, suite])
        s3_tname_list = set([key.name.rpartition('/')[2] for key in s3_list])
        non_nils_missing_from_s3 = [(t_idx, t) for (t_idx, t) in non_nils if t not in s3_tname_list]
        if len(non_nils_missing_from_s3) != 0:
            raise Straggler('NON_NIL_CVG_BUT_NO_S3', [tool, project, v, suite],
                    idxes=[t_idx for (t_idx, _) in non_nils_missing_from_s3],
                    fix=(
                        'cvgmeasure.cvg.do_cvg',
                        lambda bundle: bundle[1:],
                        lambda bundle: b_pvs(bundle) + " -K cvg_tool -a {tool} -j '{{\"redo\": true}}'".format(tool=tool)
                    ))
        return "Cvg for in s3 : {0}".format(len(non_nils))




def main():
    r = StrictRedis.from_url(get_property_defaults('redis_url'))
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append")
    parser.add_option("-v", "--version", dest="restrict_version", action="append")
    parser.add_option("-T", "--tool", dest="tools", action="append", default=[])
    parser.add_option("-s", "--suite", dest="suite", action="append", default=[])
    parser.add_option("-S", "--suite-ids", dest="suite_ids", action="store", default=None)

    parser.add_option("-q", "--queue", dest="queue_name", action="store", type="string", default="default")
    parser.add_option("-t", "--timeout", dest="timeout", action="store", type="int", default=1800)
    parser.add_option("-b", "--bundle-size", dest="bundle_size", action="store", type="int", default=10)
    parser.add_option("-c", "--commit", dest="print_only", action="store_false", default=True)

    (options, args) = parser.parse_args(sys.argv)


    if options.suite_ids is None:
        suites = options.suite
    else:
        rmin, _, rmax = options.suite_ids.partition('-')
        rmin, rmax = int(rmin), int(rmax)
        suites = ['{name}.{id}'.format(name=suite, id=id) for id in xrange(rmin, rmax+1)
                for suite in options.suite]

    if options.tools == []:
        tools = [[]] ## ??
    else:
        tools = [[tool] for tool in options.tools]

    if suites == []:
        suites = ['dev']

    total_tms = 0
    for suite in suites:
        suite_tms = 0
        for project, v in iter_versions(options.restrict_project, options.restrict_version):
            v_idxs, v_tns = check_test_list(r, project, v, suite)
            suite_tms += len(v_idxs)
            total_tms += len(v_idxs)

            for tool_ in tools:
                tool = ''.join(tool_)
                print "- {tool}:{project}:{v}:{suite}".format(tool=tool, project=project, v=v, suite=suite)
                with run_with_fixes():
                    fails = get_fails(r, tool, project, v, suite, v_idxs)
                    passing = [(v_idx, v_tn) for (v_idx, v_tn) in zip(v_idxs, v_tns) if v_idx not in fails]
                    print len(passing), len(fails)
                    p_idxs, p_tns = zip(*passing)
                    cvg_info = check_cvg(r, tool, project, v, suite, p_idxs, p_tns)
                    print cvg_info


        print 'suite {suite} tms: {tms}'.format(suite=suite, tms=suite_tms)
    print 'total tms', total_tms


if __name__ == "__main__":
    if RESET_BUNDLE_FN:
        with open(BUNDLE_FN, 'w'):
            pass
    main()

