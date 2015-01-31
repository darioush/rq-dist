import redis
import sys
import re

from rq import get_failed_queue
from rq.job import NoSuchJobError, Job, Status
from rq.exceptions import InvalidJobOperationError
from rq import Queue
from optparse import OptionParser

from cvgmeasure.conf import REDIS_URL_RQ

def _requeue(r, fq, job_id, to_q, timeout=None, action=False):
        """Requeues the job with the given job ID."""
        try:
            job = Job.fetch(job_id, connection=r)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            fq.remove(job_id)
            return
        print job.exc_info

        if to_q:
            q = Queue(to_q, connection=r)
        else:
            q = Queue(job.origin, connection=r)

        print q
        print "Timeout will be: %d" % timeout if timeout else job.timeout

        if action:
            # Delete it from the failed queue (raise an error if that failed)
            if fq.remove(job) == 0:
                raise InvalidJobOperationError('Cannot requeue non-failed jobs.')

            job.set_status(Status.QUEUED)
            job.exc_info = None

            if timeout is not None:
                job.timeout = timeout

            q.enqueue_job(job)
            print "DONE!"

def requeue(options):
    r = redis.StrictRedis.from_url(REDIS_URL_RQ)
    fq = get_failed_queue(connection=r)
    print fq
    _requeue(r, fq, job_id=options.job, to_q=options.to_q, timeout=options.timeout, action=options.action)

def list_timeouts(options):
    r = redis.StrictRedis.from_url(REDIS_URL_RQ)
    fq = get_failed_queue(connection=r)
    def get_timeout(job):
        reason = job.exc_info.split('\n')[-2:-1]
        for r in reason:
            match = re.match('JobTimeoutException.*?(\d+)', r)
            if match:
                return int(match.group(1))
        return None

    jobs = fq.get_jobs()
    timeouts = map(get_timeout, jobs)

    timeouted_jobs = [(job, timeout) for (job, timeout) in zip(jobs, timeouts) if timeout is not None]

    for job, to in timeouted_jobs:
        print "%s\t\ttimeouted at: %d" % (job.id, to)



if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-q", "--queue", dest="to_q", action="store", type="string", default="default")
    parser.add_option("-t", "--timeout", dest="timeout", action="store", type="int", default=None)
    parser.add_option("-j", "--job", dest="job", action="store", type="string", default=None)
    parser.add_option("-a", "--action", dest="action", action="store_true", default=False)
    parser.add_option("-l", "--list-timeouts", dest="list", action="store_true", default=False)

    (options, args) = parser.parse_args(sys.argv[1:])

    if options.job:
        requeue(options)

    if options.list:
        list_timeouts(options)

