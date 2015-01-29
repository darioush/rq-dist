import sys
import plumbum
from plumbum import SshMachine

from cvgmeasure.conf import workers, REDIS_URL_RQ
from rq import Worker
from redis import StrictRedis


def is_local(m, worker):
    machine, _, _ = worker.partition('.')
    return machine == m

def main(machine, instances):
    r = StrictRedis.from_url(REDIS_URL_RQ)
    machine_workers = [worker.name for worker in Worker.all(connection=r)
            if is_local(machine, worker.name)]

    print "%d workers running on %s" % (len(machine_workers), machine)
    print '\n'.join(machine_workers)

    rem = SshMachine(workers[machine]['hostname'])
    dir = rem.path(workers[machine]['rqdir'])
    if not dir.exists():
        print "CLONING REPO..."
        rem["git"]("clone", "http://github.com/darioush/rq-dist", dir)
        print "CLONED..."
        print "MAKING VIRTUAL ENV..."
        with rem.cwd(dir):
            rem["virtualenv"]("env")
        print "MADE VIRTUAL ENV..."

    with rem.cwd(dir):
        print "UPDATING CODE ..."
        rem["git"]("pull", "origin", "master")
        print "UPDATING VENV ..."
        rem["./update-venv.sh"]()

        for i in xrange(0, instances - len(machine_workers)):
                print rem["./worker.sh"]()
                print "Worker spawned"


if __name__ == "__main__":
        main(sys.argv[1], int(sys.argv[2]))

