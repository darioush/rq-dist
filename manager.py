import sys
import signal
import os
import plumbum
from plumbum import SshMachine

from cvgmeasure.conf import workers, REDIS_URL_RQ
from rq import Worker
from redis import StrictRedis


def is_local(m, worker):
    machine, _, _ = worker.partition('.')
    return machine == m


def setup(machine):
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


def main(machine, instances):
    r = StrictRedis.from_url(REDIS_URL_RQ)
    machine_workers = [worker
            for worker in Worker.all(connection=r)
            if is_local(machine, worker.name)]

    print "%d workers running on %s" % (len(machine_workers), machine)
    print '\n'.join(map(lambda m: "%s\t%s" % (m.name, m.get_state()),
        machine_workers))

    rem = SshMachine(workers[machine]['hostname'])
    dir = rem.path(workers[machine]['rqdir'])

    with rem.cwd(dir):
        for i in xrange(0, instances - len(machine_workers)):
            rem["./worker.sh"]()
            print "Worker spawned"


def killall(machine):
    r = StrictRedis.from_url(REDIS_URL_RQ)
    machine_workers = [worker
            for worker in Worker.all(connection=r)
            if is_local(machine, worker.name)]

    idle_workers = [worker for worker in machine_workers
            if worker.get_state() == 'idle']

    rem = SshMachine(workers[machine]['hostname'])

    for worker in idle_workers:
        machine, _, _pid = worker.name.partition('.')
        try:
            rem['kill'](_pid)
        except:
            print "WARNING:: Couldn't kill %s" % worker.name


if __name__ == "__main__":
    if sys.argv[1] == 'spawn':
        main(sys.argv[2], int(sys.argv[3]))

    if sys.argv[1] == 'setup':
        setup(sys.argv[2])

    if sys.argv[1] == 'killall':
        killall(sys.argv[2])
