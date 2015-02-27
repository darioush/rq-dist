import redis
import random

from collections import defaultdict

from cvgmeasure.conf import get_property, REDIS_URL_TG
from cvgmeasure.common import mk_key, mk_data_key, mk_tmp_key
from cvgmeasure.d4 import get_tts, get_num_bugs

tools = ['cobertura', 'codecover', 'jmockit']
source_list_keys = { 'linecvg': 'test-methods-agree-cvg-nonempty' }


class MissingTTs(Exception):
    pass


def get_unique_goal_tts(rr, qm, project, version, tts, all_tests):
    non_tts = [t for t in all_tests if t not in tts]
    tmp_store_key = mk_tmp_key('ntt-union', [qm, project, version])
    tg_keys = [mk_data_key('tgs', [qm, project, version, t]) for t in non_tts]

    if tg_keys:
        rr.sunionstore(tmp_store_key, *tg_keys)
        rr.expire(tmp_store_key, 500)

    def get_unique_goals(tt):
        tt_key = mk_data_key('tgs', [qm, project, version, tt])
        unique_goals = rr.sdiff(tt_key, tmp_store_key)
        return unique_goals

    tts_with_unique_goals = [tt for tt in tts if len(get_unique_goals(tt)) > 0]
    return tts_with_unique_goals


def get_essential_tests(tgs_map, tests):
    tg_counts = defaultdict(int)
    for test in tests:
        for tg in tgs_map[test]:
            tg_counts[tg] += 1

    return [test for test in tests if any(tg_counts[tg] == 1 for tg in tgs_map[test])]

def get_redundant_tests(tgs_map, tests):
    redundant_tests = [i for i in tests if any(
                    i != j and tgs_map[i] < tgs_map[j]
                    for j in tests)
            ]
    return redundant_tests

def run_selection(tgs_map, tests, seed=0, initial_tests=[], verbose=False):
    random.seed(seed)

    chosen_tests = list(initial_tests)
    chosen_tgs = reduce(lambda a, b: a | b, [tgs_map[test] for test in chosen_tests], set([]))

    while True:
        additional_tg_count_per_test = {test: len(tgs_map[test] - chosen_tgs)
                for test in tests if test not in chosen_tests}

        highest_tg_count_per_test = max(additional_tg_count_per_test.values())
        if highest_tg_count_per_test == 0:
            break

        choices = [test for (test, count) in additional_tg_count_per_test.items() if count == highest_tg_count_per_test]
        choice = random.choice(choices)
        if verbose:
            print len(chosen_tests), choice, len(choices), highest_tg_count_per_test

        chosen_tests.append(choice)
        chosen_tgs = chosen_tgs | tgs_map[choice]
    return chosen_tests


def greedy_minimization(label, all_tests, tts, tgs_map, redundants=lambda tgs, tests: [], essentials=lambda tgs, tests: []):
        redundant_set = set(redundants(tgs_map, all_tests))
        print "Redundants: ", len(redundant_set)
        non_redundant_tests = [test for test in all_tests if test not in redundant_set]
        if all(tt in redundant_set for tt in tts):
            print "All tts were redundant"
            return 0

        essential_tests = essentials(tgs_map, non_redundant_tests)
        print "Essentials: ", len(essential_tests)
        if any(tt in essential_tests for tt in tts):
            print "Some tt is essential"
            return 1

        print label,
        success = 0
        for i in xrange(100):
            selected = run_selection(tgs_map, non_redundant_tests, seed=7*i+13, initial_tests=essential_tests)
            selected_tts = [t for t in tts if t in selected]
            print len(selected_tts),
            if len(selected_tts) > 0:
                success += 1
        print "...", success


def minimization(r, rr, qm, project, version):
    source_list_key = source_list_keys[qm]
    all_tests = r.lrange(mk_key(source_list_key, [project, version]), 0, -1)

    tts = get_tts(project, version)
    missing_tts = tts - set(all_tests)

    if len(missing_tts) > 0:
        raise MissingTTs(' '.join(missing_tts))

    print "Total # of tts: ", len(tts), " of ", len(all_tests)
    tts_with_unique_goals = get_unique_goal_tts(rr, qm, project, version, tts, all_tests)
    print "Guaranteed: ", len(tts_with_unique_goals)

    if len(tts_with_unique_goals) == 0:
        print "Reading..."
        tgs_map = {test: rr.smembers(mk_data_key('tgs', [qm, project, version, test]))
                            for test in all_tests}

        greedy_minimization("G  ", all_tests, tts, tgs_map)
        greedy_minimization("GE ", all_tests, tts, tgs_map, essentials=get_essential_tests)
        greedy_minimization("GRE", all_tests, tts, tgs_map, redundants=get_redundant_tests, essentials=get_essential_tests)


def main():
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    rr = redis.StrictRedis.from_url(REDIS_URL_TG)
    qm = 'linecvg'

    for project in ["Lang", "Chart", "Time"]:
        for v in xrange(0, get_num_bugs(project)):
            version = v + 1
            print "----( %s %d --  %s )----" % (project, version, qm)
            minimization(r, rr, qm, project, version)
            print

if __name__ == "__main__":
    main()

