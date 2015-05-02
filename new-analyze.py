import redis
import random
import msgpack
import sqlite3

from collections import defaultdict

from cvgmeasure.conf import get_property, REDIS_URL_TG
from cvgmeasure.common import mk_key, tg_i_s, tn_i_s, i_tn_s
from cvgmeasure.d4 import get_tts, get_num_bugs
from cvgmeasure.consts import ALL_TGS

def connect_db():
    conn = sqlite3.connect('result.db')
    conn.execute("""create table if not exists testselection  (key integer primary key autoincrement, qm string, granularity string, project string, version integer, base string, select_from string, algorithm string, run_id integer, fault_detection integer, determined_by string);""")
    return conn

def save_rows(conn, vals): #qm, granularity, project, version, base, select_from, algorithm, run_id, fault_detection, determined_by):
    conn.executemany("insert into testselection values (null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", vals)
    conn.commit()
#            qm, granularity, project, version, base, select_from, algorithm, run_id, fault_detection, determined_by)

RUNS = 100

class MissingTTs(Exception):
    pass


def get_essential_tests(tg_map, tests):
    tg_counts = defaultdict(int)
    for test in tests:
        for tg in tg_map[test]:
            tg_counts[tg] += 1

    return [test for test in tests if any(tg_counts[tg] == 1 for tg in tg_map[test])]


def get_redundant_tests(tg_map, tests):
    redundant_tests = [i for i in tests if any(
                    i != j and tg_map[i] < tg_map[j]
                    for j in tests)
            ]
    return redundant_tests


def get_equal_tests(tg_map, tests):
    equal_tests = defaultdict(list) # { min_test -> set([eqs]) }
    sorted_tests = sorted(tests)
    eq_tests = set([])
    for idx, i in enumerate(sorted_tests):
        if i in eq_tests:
            continue
        for j in sorted_tests[idx+1:]:
            if tg_map[i] == tg_map[j]:
                eq_tests.add(j)
                equal_tests[i].append(j)
    for k in equal_tests:
        equal_tests[k].append(k)

    redundant_tests = []
    for eqs in equal_tests.values():
        chosen_val = random.choice(eqs)
        redundant_tests.extend(x for x in eqs if x != chosen_val)
    return redundant_tests


def combinator(f1, f2):
    def combination(tg_map, tests):
        result = f1(tg_map, tests)
        remains = [t for t in tests if t not in result]
        result.extend(f2(tg_map, remains))
        return result
    return combination


def run_selection(tg_map, tests, seed=0, initial_tests=[], verbose=False):
    random.seed(seed)

    chosen_tests = list(initial_tests)
    chosen_tgs = reduce(lambda a, b: a | b, [tg_map[test] for test in chosen_tests], set([]))

    while True:
        additional_tg_count_per_test = {test: len(tg_map[test] - chosen_tgs)
                for test in tests if test not in chosen_tests}

        highest_tg_count_per_test = max(additional_tg_count_per_test.values())
        if highest_tg_count_per_test == 0:
            break

        choices = [test for (test, count) in additional_tg_count_per_test.items() if count == highest_tg_count_per_test]
        choice = random.choice(choices)
        if verbose:
            print len(chosen_tests), choice, len(choices), highest_tg_count_per_test

        chosen_tests.append(choice)
        chosen_tgs |= tg_map[choice]
    return chosen_tests


def greedy_minimization(all_tests, tts, tg_map, redundants=lambda tgs, tests: [], essentials=lambda tgs, tests: []):
        redundant_set = set(redundants(tg_map, all_tests))
        print "Redundants: ", len(redundant_set)
        non_redundant_tests = [test for test in all_tests if test not in redundant_set]
        if all(tt in redundant_set for tt in tts):
            print "All tts were redundant"
            return 'R', [0 for i in xrange(0, RUNS)]

        essential_tests = essentials(tg_map, non_redundant_tests)
        print "Essentials: ", len(essential_tests)
        essential_tts = [tt for tt in tts if tt in essential_tests]
        if len(essential_tts) > 0:
            print "Some tt is essential"
            return 'E', [1 for i in xrange(0, RUNS)]

        results = []
        for i in xrange(RUNS):
            selected = run_selection(tg_map, non_redundant_tests, seed=7*i+13, initial_tests=essential_tests)
            selected_set = set(selected)
            selected_tts = tts & selected_set
            print len(selected_tts),
            if len(selected_tts) > 0:
                results.append(1)
            else:
                results.append(0)
        print '...', len([r for r in results if r > 0])
        return 'S', results

def get_unique_goal_tts(tts, all_tests_set, tg_map):
    non_tt_tg_union = set([])
    for t in all_tests_set - tts:
        non_tt_tg_union |= tg_map[t]

    def get_unique_goals(tt):
        unique_goals = tg_map[tt] - non_tt_tg_union
        return unique_goals

    tts_with_unique_goals = [tt for tt in tts if len(get_unique_goals(tt)) > 0]
    return tts_with_unique_goals

QMS = {
        'line': {'name': 'line', 'granularity': 'file', 'fun': lambda s: s.startswith('line:')},
        'branch': {'name': 'branch', 'granularity': 'file', 'fun': lambda s: s.startswith('branch:') or s.startswith('line:')},
        'mutant': {'name': 'mutant', 'granularity': 'file', 'fun': lambda s: s.startswith('mutant:') or s.startswith('line:')},
    }

def minimization(conn, r, rr, qm_name, project, version, suite):
    qm = QMS[qm_name]
    # 1. Get the testing goals that match the qm
    relevant_tgs = filter(qm['fun'], rr.hkeys(mk_key('tg-i', [project, version])))
    relevant_tg_is = tg_i_s(rr, relevant_tgs, project, version)
    relevant_tg_set = set(relevant_tg_is)

    # 2. Get the list of tests with some coverage goals
    all_tests = map(int, rr.hkeys(mk_key('tgs', [project, version, suite])))
    all_tests_set = set(all_tests)

    tts = set(tn_i_s(r, list(get_tts(project, version)), suite))
    missing_tts = tts - all_tests_set

    if len(missing_tts) > 0:
        raise MissingTTs(' '.join(i_tn_s(r, missing_tts, suite)))

    print "Total # of tts: ", len(tts), " of ", len(all_tests)
    print "Reading..."
    tg_map = {k: set(msgpack.unpackb(v)) & relevant_tg_set
            for (k, v) in 
            zip(all_tests, 
                rr.hmget(mk_key('tgs', [project, version, suite]), all_tests))}
    tts_with_unique_goals = get_unique_goal_tts(tts, all_tests_set, tg_map)
    print "Guaranteed: ", len(tts_with_unique_goals)

#   qm, granularity, project, version, base, select_from, algorithm, run_id, fault_detection, determined_by)

    if len(tts_with_unique_goals) > 0:
        save_rows(conn, [(qm['name'], qm['granularity'], project, version, 'empty', 'dev', algo, run_id+1, 1, "U")
            for algo in ('G', 'GE', 'GRE', 'GREQ')
            for run_id in xrange(0, RUNS)
        ])
    else:
        algos = [
            ('G', lambda:  greedy_minimization(all_tests, tts, tg_map)),
            ('GE', lambda: greedy_minimization(all_tests, tts, tg_map, essentials=get_essential_tests)),
            ('GRE', lambda: greedy_minimization(all_tests, tts, tg_map, redundants=get_redundant_tests, essentials=get_essential_tests)),
            ('GREQ', lambda: greedy_minimization(all_tests, tts, tg_map, redundants=combinator(get_redundant_tests, get_equal_tests),
                essentials=get_essential_tests))
        ]
        for algo, fun in algos:
            determined_by, results = fun()
            save_rows(conn, [
                (qm['name'], qm['granularity'], project, version, 'empty', 'dev', algo, run_id+1, result, determined_by)
                for (run_id, result) in enumerate(results)
            ])



def main():
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    rr = redis.StrictRedis.from_url(REDIS_URL_TG)
    conn = connect_db()

    for qm in ['line', 'branch', 'mutant']:
        for project in ["Chart"]:
            for v in xrange(0, 10):
                version = v + 1
                print "----( %s %d --  %s )----" % (project, version, qm)
                minimization(conn, r, rr, qm, project, version, 'dev')
                print

if __name__ == "__main__":
    main()

