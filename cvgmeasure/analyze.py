import json
import random
import msgpack
import click

from bitarray import bitarray
from collections import defaultdict
from itertools import groupby

from cvgmeasure.common import mk_key, tg_i_s, tn_i_s, i_tn_s, Timer
from cvgmeasure.consts import ALL_TGS

def save_row_independent(r_out, key, val):
    r_out.set(key + ':info', json.dumps(val))

def save_rows(r_out, key, vals):
    # redis key -> out:qm:granularity:project:version:base:pool
    # parts of the hash: algorithm:run_id =>
    r_out.hmset(key, {k: json.dumps(v) for k, v in vals.iteritems()})

RUNS = 100

def bitmap_len(bit_map):
    return len(bit_map.itervalues().next())


def get_essential_tests(bit_map, tests):
    if len(tests) == 0:
        return set()

    unseen_tgs = bitarray(len(bit_map.itervalues().next()))
    unseen_tgs.setall(True)

    once_tgs = bitarray(len(bit_map.itervalues().next()))
    once_tgs.setall(False)

    for test in tests:
        t = bit_map[test]
        once_tgs = (unseen_tgs & t) | (once_tgs & ~t)
        unseen_tgs &= ~t

    return set(test for test in tests if (once_tgs & bit_map[test]).any())


def get_redundant_and_equal_tests(bit_map, tests):
    b1 = bitarray('1')
    inv_idx = defaultdict(set)
    tg_cnt_inv_idx = defaultdict(set)

    tg_counts = {test: bit_map[test].count() for test in tests}
    sorted_tests = sorted(tests, key= lambda x: (tg_counts[x], x))

    for t in tests:
        for tg_idx in bit_map[t].itersearch(b1):
            inv_idx[tg_idx].add(t)
        tg_cnt_inv_idx[tg_counts[t]].add(t)

    redundant_tests = set([])
    equal_tests = []
    eq_tests = set([])

    for t in sorted_tests:
        if t in redundant_tests or t in eq_tests:
            continue

        supersets = None
        for tg_idx in bit_map[t].itersearch(b1):
            if supersets is None:
                supersets = set(inv_idx[tg_idx])
            else:
                supersets &= inv_idx[tg_idx]

        equals = supersets & tg_cnt_inv_idx[tg_counts[t]]
        assert t in equals
        strict_supersets = supersets - equals

        if len(strict_supersets) > 0:
            redundant_tests.update(equals)
        elif len(equals) > 1:
            eq_tests.update(equals)
            equal_tests.append(equals)
    return redundant_tests, equal_tests


def run_selection(bit_map, tests, initial_tests=set(), verbose=False):
    if len(tests) + len(initial_tests) == 0:
        return set(), bitarray(0)

    chosen_tests = set(initial_tests) # don't modify
    remaining_tests = set(tests)

    chosen_tgs = bitarray(len(bit_map.itervalues().next()))
    chosen_tgs.setall(False)
    for test in initial_tests:
        chosen_tgs |= bit_map[test]
        remaining_tests.remove(test)

    while True:
        max_additional_len = 0
        max_additional_tests = []

        empties = []
        for test in remaining_tests:
            additional_tgs = (bit_map[test] & ~chosen_tgs).count()
            if additional_tgs == 0:
                empties.append(test) # won't ever need to check this test again
            elif additional_tgs > max_additional_len:
                max_additional_tests = [test]
                max_additional_len = additional_tgs
            elif additional_tgs == max_additional_len:
                max_additional_tests.append(test)

        highest_tg_count_per_test, choices = max_additional_len, max_additional_tests
        if highest_tg_count_per_test == 0:
            break

        for empty in empties:
            remaining_tests.remove(empty)

        choice = random.choice(choices)
        if verbose:
            print len(chosen_tests), choice, len(choices), highest_tg_count_per_test

        chosen_tests.add(choice)
        chosen_tgs |= bit_map[choice]
        remaining_tests.remove(choice)
    return chosen_tests, chosen_tgs


def get_do_one(tests, bit_map, redundants=lambda: set(), essentials=lambda tgs, tests: set(), tR=Timer(), tE=Timer(), tS=Timer()):
    def do_one(tests=tests, bit_map=bit_map, redundants=redundants, essentials=essentials, tR=tR, tE=tE, tS=tS):
        tR.start()
        redundant_set = redundants()
        non_redundant_tests = tests - redundant_set
        tR.stop()

        tE.start()
        essential_tests = essentials(bit_map, non_redundant_tests)
        tE.stop()

        tS.start()
        selected, selected_tgs = run_selection(bit_map, non_redundant_tests, initial_tests=essential_tests)
        tS.stop()

        return redundant_set, essential_tests, selected, selected_tgs
    return do_one


def process_results(results, tts, timing):
    redundants, essentials, selected, selected_tgs = results

    essential_tts = tts & essentials
    selected_tts = tts & selected

    fault_detection = len(selected_tts) > 0
    if tts <= redundants:
        determined_by = 'R'
        assert not fault_detection
    elif len(essential_tts) > 0:
        determined_by = 'E'
        assert fault_detection
    else:
        determined_by = 'S'
    return determined_by, len(selected_tts), len(selected), selected_tgs.count(), timing(selected)


def greedy_minimization(iterable, do_one, process_results):
    results = []
    for i in iterable:
        seed=7*i+13
        random.seed(seed)
        results.append(process_results(do_one()))
    return results


def get_unique_goal_tts(tts, all_tests_set, tg_map):
    non_tt_tg_union = set([])
    for t in all_tests_set - tts:
        non_tt_tg_union |= tg_map[t]

    def get_unique_goals(tt):
        unique_goals = tg_map[tt] - non_tt_tg_union
        return unique_goals

    tts_with_unique_goals = [tt for tt in tts if len(get_unique_goals(tt)) > 0]
    return tts_with_unique_goals


def get_triggers_from_results(r, project, version, suite):
    result = json.loads(r.hget(mk_key('trigger', [project, version]), suite))
    return set(result)


def ALL(*args):
    return True

POOLS = {
    '0': ([], ALL),
    'B': (['dev'], lambda tidx, trigger_set: tidx not in trigger_set),
    'F': (['dev'], lambda tidx, trigger_set: tidx in trigger_set),
    'R': (['randoop.1'], ALL),
    'E': (['evosuite-{0}-fse.{1}'.format(kind, id) for kind in ('branch', 'weakmutation', 'strongmutation') for id in xrange(1,11)], ALL),
}
POOLS['G'] = (POOLS['R'][0] + POOLS['E'][0], ALL)


def minimization(r, rr, conn, qm_name, project, version, bases, augs):
    qm = QMS[qm_name]

    # 1. Get the testing goals that match the qm
    all_tgs = rr.hkeys(mk_key('tg-i', [project, version]))
    relevant_tgs = filter(qm['fun'], all_tgs)
    relevant_tg_is = tg_i_s(rr, relevant_tgs, project, version)
    relevant_tg_set = set(relevant_tg_is)

    tp_idx = {}
    idx_tp = []

    fails = {}
    def get_fails(suite, fails=fails):
        if not fails.has_key(suite):
            fail_ids = set(map(int, r.smembers(mk_key('fail', ['exec', project, version, suite]))))
            fails[suite] = fail_ids
        return fails[suite]

    def register_suite(suite, ff, tp_idx=tp_idx, idx_tp=idx_tp):
        tests = map(int, rr.hkeys(mk_key('tgs', [project, version, suite])))
        suite_triggers = get_triggers_from_results(r, project, version, suite)
        fails = get_fails(suite)
        # always remove failing tests
        tests_no_fail = filter(lambda x: x not in fails, tests)
        tests_filtered = filter(lambda x: ff(x, suite_triggers), tests_no_fail)
        triggers = filter(lambda x: x in suite_triggers, tests_filtered)
        print 'Suite: {0}, Tests: {1}, Suite Triggers:{2}, Selected Triggers: {3}, Fails: {4}, No fails: {5}, Filtered: {6}'.format(
                suite, *map(len, [tests, suite_triggers, triggers, fails, tests_no_fail, tests_filtered]))
        for test in tests_filtered:
            tp_idx[(suite, test)] = tp_idx.get((suite, test), len(idx_tp))
            if len(idx_tp) != len(tp_idx):
                idx_tp.append((suite, test))
        triggers = set(tp_idx[(suite, t)] for t in triggers)
        return triggers, [tp_idx[(suite, test)] for test in tests_filtered]


    # 2. Get the list of tests with some coverage goals from base / augmenting pools
    def flatten(l):
        return reduce(lambda a,b: a+b, l, [])

    def pool_to_tests(pools, tp_idx=tp_idx, idx_tp=idx_tp):
        tests = []
        triggers = set([])
        for pool in pools:
            pool_suites, pool_filter = POOLS[pool]
            for pool_suite in pool_suites:
                a, b = register_suite(pool_suite, pool_filter, tp_idx=tp_idx, idx_tp=idx_tp)
                tests.append(b)
                triggers.update(a)
        return triggers, flatten(tests)

    base_tp_idx = {}
    base_idx_tp = []
    base_triggers, base_tests = pool_to_tests(bases, base_tp_idx, base_idx_tp)

    aug_triggers, aug_tests = pool_to_tests(augs, tp_idx, idx_tp)

    print "Triggers: Base {0}, Aug {1}".format(len(base_triggers), len(aug_triggers))
    print "Reading..."
    tRead = Timer()
    tRead.start()

    def get_tg_map(tests, idx_tp, relevant_tg_set):
        get_suite = lambda t: idx_tp[t][0]
        get_tid   = lambda t: idx_tp[t][1]
        tests_sorted = sorted(tests, key=get_suite) # get suite
        tg_map = {}
        for suite, suite_tests_iter in groupby(tests_sorted, key=get_suite):
            suite_tests = list(suite_tests_iter)
            vals = rr.hmget(mk_key('tgs', [project, version, suite]), map(get_tid, suite_tests))
            vals_mapped = [set(msgpack.unpackb(v)) & relevant_tg_set for v in vals]
            tg_map.update(dict(zip(suite_tests, vals_mapped)))
        return {k: v for k, v in tg_map.iteritems() if len(v) > 0}

    base_tg_map = get_tg_map(base_tests, base_idx_tp, relevant_tg_set)
    get_all_tgs = lambda tg_map: reduce(lambda a, b: a | b, tg_map.values(), set([]))
    get_all_tests = lambda tg_map: set(tg_map.keys())
    base_tgs = get_all_tgs(base_tg_map)
    print "Relevant_tgs {0}, Base tgs: {1}".format(*map(len, (relevant_tg_set, base_tgs)))

    base_relevant_tests = get_all_tests(base_tg_map)
    base_relevant_tts   = base_triggers & base_relevant_tests

    aug_relevant_tg_map = get_tg_map(aug_tests, idx_tp, relevant_tg_set)
    aug_tgs = get_all_tgs(aug_relevant_tg_map)
    aug_relevant_tests = get_all_tests(aug_relevant_tg_map)
    aug_relevant_tts = aug_triggers & aug_relevant_tests

    aug_additional_tg_map = get_tg_map(aug_tests, idx_tp, relevant_tg_set - base_tgs)
    aug_additional_tgs = get_all_tgs(aug_relevant_tg_map)
    aug_additional_tests = get_all_tests(aug_additional_tg_map)
    aug_additional_tts = aug_triggers & set(aug_additional_tests)

    print "Triggers: Base_relevant {0}, Aug_relevant {1}, Aug_additional {2}".format(*map(len,
            (base_relevant_tts, aug_relevant_tts, aug_additional_tts)))

    tRead.stop()
    print "Reading complete {0} msecs.".format(tRead.msec)

    tts_with_unique_goals = get_unique_goal_tts(aug_additional_tts, aug_additional_tests, aug_additional_tg_map)
    print "Guaranteed: ", len(tts_with_unique_goals)

    print "Building bit vectors"
    all_tgs = { k: i for i, k in enumerate(sorted(get_all_tgs(aug_additional_tg_map)))}
    def to_bitvector(s):
        bv = bitarray(len(all_tgs))
        bv.setall(False)
        for tg in s:
            bv[all_tgs[tg]] = 1
        return bv
    bit_map = {k : to_bitvector(v) for k, v in aug_additional_tg_map.iteritems()}
    print "Built.."

    def timing(tests, idx_tp=idx_tp):
        tps = map(lambda t: idx_tp[t], tests)
        tps_sorted = sorted(tps, key=lambda (suite, i): suite)
        total_time = 0
        for suite, i_it in groupby(tps_sorted, key=lambda(suite, i): suite):
            i_s = map(lambda (suite, i): i, i_it)
            if suite == 'dev':
                tns = i_tn_s(r, i_s, suite)
                tc_ns = set(tn.partition('::')[0] for tn in tns)
                tc_is = tn_i_s(r, list(tc_ns), suite, allow_create=False)
                all_is = tc_is + i_s
            else:
                all_is = i_s
            method_times = [msgpack.unpackb(b) for b in r.hmget(mk_key('time', [project, version, suite]), all_is)]
            bad_timings = [(time, i) for (time, i) in zip(method_times, all_is) if any(t < 0 for t in time)]
            if bad_timings:
                raise Exception('bad timing for tests: {project}, {version}, {suite} {idxs}'.format(
                    project=project, version=version, suite=suite, idxs=' '.join(map(str, [i for (time, i) in bad_timings]))))
            def aggr(l):
                if any(x == -1 for x in l):
                    raise Exception('bad timing for tests: {project}, {version}, {suite}'.format(
                        project=project, version=version, suite=suite))
                # let's go with average
                return reduce(lambda a, b: a+b, l)/len(l)
            method_times_aggregate = [aggr(timings) for timings in method_times]
            suite_time = reduce(lambda a, b: a+b, method_times_aggregate, 0)
            total_time += suite_time
        return total_time

    base_time = timing(base_tests, base_idx_tp)
    def timing_with_base(tests, idx_tp=idx_tp):
        return timing(tests, idx_tp) + base_time

    def reason(given):
        if len(tts_with_unique_goals) > 0:
            return 'U'
        elif len(aug_additional_tts) == 0:
            return 'X'
        else:
            return given
    # row independent info
    suite_schema = lambda x: ['{0} {1}'.format(x,i) for i in ('triggers', 'tests', 'tgs', 'time')]
    schema = [ 'Relevant tgs', 'Reason',] + \
            suite_schema('Base') + suite_schema('Base relevant') + suite_schema('Aug') + suite_schema('Aug relevant') \
            + suite_schema('Aug additional')
    info = (
                len(relevant_tgs), reason('-'),
                len(base_triggers), len(base_tests), len(base_tgs), base_time,   # all of base suite
                len(base_relevant_tts), len(base_relevant_tests), len(base_tgs), timing(base_relevant_tests, base_idx_tp), # relevant part of base suite
                len(aug_triggers), len(aug_tests), len(aug_tgs), timing(aug_tests),               # all of augmentation pool
                len(aug_relevant_tts), len(aug_relevant_tests), len(aug_tgs), timing(aug_relevant_tests),# relevant part of aug pool
                len(aug_additional_tts), len(aug_additional_tests), len(aug_additional_tgs), timing(aug_additional_tests), # part of aug pool in addition to the base suite
            )
    print ', '.join('{0}: {1}'.format(a,b) for a,b in (zip(schema, info)))
    print info

    key = mk_key('out', [qm['name'], qm['granularity'], '.'.join(sorted(bases)), '.'.join(sorted(augs)), project, version])
    save_row_independent(conn, key, info)
    minimization = lambda **kwargs: get_do_one(aug_additional_tests, bit_map, **kwargs)

    redundants, eq_partition = get_redundant_and_equal_tests(bit_map, aug_additional_tests)
    #print redundants, eq_partition
    choice = lambda eq_partition=eq_partition: reduce(lambda a, b: a | b, map(lambda s: s - set(random.sample(s, 1)), eq_partition), set())

    algo_params = [
        ('G', {}),
        ('GE', {'essentials': get_essential_tests}),
        ('GRE', {'redundants': lambda redundants=redundants: redundants , 'essentials': get_essential_tests}),
        ('GREQ', {'redundants': lambda redundants=redundants: redundants | choice(), 'essentials': get_essential_tests}),
    ]
    algos = [(name, lambda algo_param=algo_param: minimization(**algo_param)) for (name, algo_param) in algo_params]
    for algo, fun in algos:
        info = {'nofd': 0, 'cnt': 0, 'cvrtg': None, 'times':[], 'cnts':[], 'reasons': {'S': 0, 'R': 0, 'E': 0}}
        label=lambda :'{algo:<4} {info[cvrtg]:>4}tgs {info[reasons][S]:>3}S {info[reasons][R]:>3}R {info[reasons][E]:>3}E {info[nofd]:>3}F {time:>6}ms'.format(info=info, algo=algo, time=''.join(map(str,info['times'][-1:])))
        with click.progressbar(xrange(RUNS), label=label(), width=20) as bar:
            def pr(results):
                rr = process_results(results, aug_additional_tts, timing_with_base)
                determined_by, sel_tts, sel_cnt, sel_tgs, time = rr
                info['cnt'] += 1
                if sel_tts == 0:
                    info['nofd']  += 1
                info['reasons'][determined_by] += 1
                info['cnts'].append(sel_cnt)
                info['times'].append(time)
                if info['cvrtg'] is None:
                    info['cvrtg'] = sel_tgs
                else:
                    assert info['cvrtg'] == sel_tgs
                bar.label=label()
                return rr
            results = greedy_minimization(bar, fun(), pr)
# vals should be a dict from {'GRE:1': (fault_detection, reason, triggers, count, tgs, time) }
        save_rows(conn, key, {'{algo}:{run}'.format(algo=algo, run=run_id+1):
            (
                1 if selected_triggers > 0 else 0,
                reason(determined_by),
                selected_triggers, selected_count, selected_tgs, selected_time
            )
            for (run_id, (determined_by, selected_triggers, selected_count, selected_tgs, selected_time)) in enumerate(results)}
        )

def def_me(s):
    possible_tgs = s.split('-')
    return {'name': s, 'granularity': 'file', 'fun': lambda x: any(x.startswith('{0}:'.format(p)) for p in possible_tgs)}

QMS = { s: def_me(s) for s in [
    'line',
    'line:cobertura', 'line:codecover', 'line:jmockit',
    'branch',
    'branch:cobertura', 'branch:codecover', 'branch:jmockit',
    'branch-line',
    'statement-line',
    'statement:codecover',
    'data',
    'branch-loop-line', 'branch-loop-path-line',
    'mutcvg',
    'mutcvg-line',
    'mutant',
    'mutant-line',
]}

