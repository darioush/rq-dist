#! /usr/bin/env python
import redis
import random
import msgpack
import sqlite3

from collections import defaultdict
from optparse import OptionParser
import sys

from cvgmeasure.conf import get_property, REDIS_URL_TG
from cvgmeasure.common import mk_key, tg_i_s, tn_i_s, i_tn_s
from cvgmeasure.d4 import get_tts, get_num_bugs, iter_versions
from cvgmeasure.consts import ALL_TGS

def connect_db():
    conn = sqlite3.connect('result.db')
    conn.execute("""create table if not exists testselection  (key integer primary key autoincrement, qm string, granularity string, project string, version integer, base string, select_from string, algorithm string, run_id integer, fault_detection integer, determined_by string);""")
    conn.execute("""create unique index if not exists unindx on testselection (qm, granularity, project, version, base, select_from, algorithm, run_id);""")
    return conn

def save_rows(conn, vals): #qm, granularity, project, version, base, select_from, algorithm, run_id, fault_detection, determined_by):
    conn.executemany("update testselection set fault_detection=?, determined_by=? where qm=? and granularity=? and project=? and version=? and base=? and select_from=? and algorithm=? and run_id=?", [val[-2:] + val[:-2] for val in vals])
    conn.executemany("insert or ignore into testselection values (null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ", vals)
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


def run_selection(tg_map, tests, initial_tests=[], verbose=False):
    chosen_tests = list(initial_tests)
    remaining_tests = set(tests)

    chosen_tgs = set([])
    for test in initial_tests:
        chosen_tgs |= tg_map[test]
        remaining_tests.remove(test)

    while True:
        max_additional_len = 0
        max_additional_tests = []

        empties = []
        for test in remaining_tests:
            additional_tgs = tg_map[test] - chosen_tgs
            if len(additional_tgs) == 0:
                empties.append(test) # won't ever need to check this test again
            elif len(additional_tgs) > max_additional_len:
                max_additional_tests = [test]
                max_additional_len = len(additional_tgs)
            elif len(additional_tgs) == max_additional_len:
                max_additional_tests.append(test)

        highest_tg_count_per_test, choices = max_additional_len, max_additional_tests
        if highest_tg_count_per_test == 0:
            break

        for empty in empties:
            remaining_tests.remove(empty)

        choice = random.choice(choices)
        if verbose:
            print len(chosen_tests), choice, len(choices), highest_tg_count_per_test

        chosen_tests.append(choice)
        chosen_tgs |= tg_map[choice]
        remaining_tests.remove(choice)

    return chosen_tests

import time
class Timer(object):
    def __init__(self):
        self.time = 0
        self.st_time = None

    def start(self):
        self.st_time = time.time()

    def stop(self):
        self.time += time.time() - self.st_time
        self.st_time = None

    @property
    def msec(self):
        return int(round(1000*self.time))


def greedy_minimization(all_tests, tts, tg_map, redundants=lambda tgs, tests: [], essentials=lambda tgs, tests: []):
        results = []
        tR, tE, tS = Timer(), Timer(), Timer()
        for i in xrange(RUNS):
                seed=7*i+13
                random.seed(seed)
                def do_one():
                    tR.start()
                    redundant_set = set(redundants(tg_map, all_tests))
                    non_redundant_tests = [test for test in all_tests if test not in redundant_set]
                    tR.stop()
                    if all(tt in redundant_set for tt in tts):
                        return ('R', 0)

                    tE.start()
                    essential_tests = essentials(tg_map, non_redundant_tests)
                    essential_tts = [tt for tt in tts if tt in essential_tests]
                    tE.stop()
                    if len(essential_tts) > 0:
                        return ('E', 1)

                    tS.start()
                    selected = run_selection(tg_map, non_redundant_tests, initial_tests=essential_tests)
                    selected_set = set(selected)
                    selected_tts = tts & selected_set
                    tS.stop()
                    if len(selected_tts) > 0:
                        return ('S',1)
                    else:
                        return ('S',0)
                results.append(do_one())
                print '{0}{1}'.format(results[-1][0], results[-1][1]),
        print '...', len([r for _, r in results if r > 0])
        print tR.msec, tE.msec, tS.msec
        return  results

def get_unique_goal_tts(tts, all_tests_set, tg_map):
    non_tt_tg_union = set([])
    for t in all_tests_set - tts:
        non_tt_tg_union |= tg_map[t]

    def get_unique_goals(tt):
        unique_goals = tg_map[tt] - non_tt_tg_union
        return unique_goals

    tts_with_unique_goals = [tt for tt in tts if len(get_unique_goals(tt)) > 0]
    return tts_with_unique_goals


def def_me(s):
    return {'name': s, 'granularity': 'file', 'fun': lambda s: s.startswith(s)}


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
    tRead = Timer()
    tRead.start()
    tg_map = {k: set(msgpack.unpackb(v)) & relevant_tg_set
            for (k, v) in 
            zip(all_tests, 
                rr.hmget(mk_key('tgs', [project, version, suite]), all_tests))}
    tRead.stop()
    print "Reading complete {0} msecs.".format(tRead.msec)
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
            results = fun()
            save_rows(conn, [
                (qm['name'], qm['granularity'], project, version, 'empty', 'dev', algo, run_id+1, result, determined_by)
                for (run_id, (determined_by, result)) in enumerate(results)
            ])


QMS = {
        'line': def_me('line'),
        'line:cobertura': def_me('branch:cobertura'),
        'line:codecover': def_me('branch:codecover'),
        'line:jmockit': def_me('line:jmockit'),

        'branch': def_me('branch'),
        'branch:cobertura': def_me('branch:cobertura'),
        'branch:codecover': def_me('branch:codecover'),
        'branch:jmockit': def_me('branch:jmockit'),

        'branch-line': {'name': 'branch-line', 'granularity': 'file', 'fun': lambda s: s.startswith('branch:') or s.startswith('line:')},

        'statement-line': {'name': 'statement-line', 'granularity': 'file', 'fun': lambda s: s.startswith('statement:') or s.startswith('line')},
        'statement:codecover': def_me('statement:codecover'),

        'data': {'name': 'data', 'granularity': 'file', 'fun': lambda s: s.startswith('data:')},

        'branch-loop-line': {'name': 'branch-loop', 'granularity': 'file', 'fun': lambda s: s.startswith('branch:') or s.startswith('loop:') or s.startswith('line:')},
        'branch-loop-path-line': {'name': 'branch-loop-path', 'granularity': 'file', 'fun': lambda s: s.startswith('branch:') or s.startswith('loop:') or s.startswith('path:') or s.startswith('line:')},

        'mutcvg': {'name': 'mutcvg', 'granularity': 'file', 'fun': lambda s: s.startswith('mutcvg:')},
        'mutcvg-line': {'name': 'mutcvg', 'granularity': 'file', 'fun': lambda s: s.startswith('mutcvg:') or s.startswith('line:')},
        'mutant': {'name': 'mutant', 'granularity': 'file', 'fun': lambda s: s.startswith('mutant:')},
        'mutant-line': {'name': 'mutant-line', 'granularity': 'file', 'fun': lambda s: s.startswith('mutant:') or s.startswith('line:')},
    }

def main(options):

    r = redis.StrictRedis.from_url(get_property('redis_url'))
    rr = redis.StrictRedis.from_url(REDIS_URL_TG)
    conn = connect_db()

    for qm in sorted(QMS.keys()):
        for project, v in iter_versions(restrict_project=options.restrict_project, restrict_version=options.restrict_version):
            print "----( %s %d --  %s )----" % (project, v, qm)
            minimization(conn, r, rr, qm, project, v, 'dev')
            print

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append", default=[])
    parser.add_option("-v", "--version", dest="restrict_version", action="append", default=[])
    (options, args) = parser.parse_args(sys.argv)
    main(options)

