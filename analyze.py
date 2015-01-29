import redis

from cvgmeasure.conf import get_property


def main():
    r = redis.StrictRedis.from_url(get_property('redis_url'))
    tools = ['cobertura', 'codecover', 'jmockit']

    project, version = "Chart", 1

    all_classes = r.lrange(':'.join(['results', 'test-classes', project, str(version)]), 0, -1)

    candidate_classes = dict((tool, set(r.hkeys(':'.join(['results', 'test-classes-cvg-nonempty',
        tool, project, str(version)])))) for tool in tools)

    cores = reduce(lambda a, b: a & b, [candidates for candidates in candidate_classes.values()])
    sums = reduce(lambda a, b: a | b, [candidates for candidates in candidate_classes.values()])

    print len(cores)
    print len(sums)
    print len(all_classes)

    C = candidate_classes
    import ipdb
    ipdb.set_trace()
    print "o"




if __name__ == "__main__":
    main()

