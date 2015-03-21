#! /usr/bin/env python
import redis
import json
import sys
import re

from rq import get_failed_queue
from rq.job import NoSuchJobError, Job, JobStatus
from rq.exceptions import InvalidJobOperationError
from rq import Queue
from optparse import OptionParser

from cvgmeasure.conf import REDIS_URL_RQ

class DummyQ(object):
    def enqueue_job(self, job):
        print "DummyQ not enqueuing:", job.id

    def __str__(self):
        return "DummyQ"

def _requeue(r, fq, job_id, to_q, timeout=None, change_method=None, update_dict=None, action=False):
        """Requeues the job with the given job ID."""
        try:
            job = Job.fetch(job_id, connection=r)
        except NoSuchJobError:
            # Silently ignore/remove this job and return (i.e. do nothing)
            fq.remove(job_id)
            return
        print job
        print job.exc_info


        if to_q and to_q == 'dummy':
            q = DummyQ()
        elif to_q:
            q = Queue(to_q, connection=r)
        else:
            q = Queue(job.origin, connection=r)

        print q
        print "Timeout will be: %d" % timeout if timeout else job.timeout

        if timeout is not None:
            job.timeout = timeout

        job_func_name = job.func_name
        job_args = job.args


        if change_method is not None:
            job.func_name = change_method
            job.args = job_args
            print "changed method to: {0}".format(change_method)

        if update_dict is not None:
            update_json = json.loads(update_dict)
            current_json = json.loads(job.args[0])
            current_json.update(update_json)
            print "changed arguments to: {0}".format(json.dumps(current_json))
            job.args = (json.dumps(current_json),)

        if action:
            # Delete it from the failed queue (raise an error if that failed)
            if fq.remove(job) == 0:
                import ipdb
                ipdb.set_trace()
                raise InvalidJobOperationError('Cannot requeue non-failed jobs.')

            job.set_status(JobStatus.QUEUED)
            job.exc_info = None


            q.enqueue_job(job)
            print "DONE!"

def requeue(options, job_list=[]):
    r = redis.StrictRedis.from_url(REDIS_URL_RQ)

    if options.source:
        fq = Queue(options.source, connection=r)
    else:
        fq = get_failed_queue(connection=r)
    print fq
    for job in job_list:
        _requeue(r, fq, job_id=job, to_q=options.to_q, timeout=options.timeout, change_method=options.method, update_dict=options.update_dict, action=options.action)

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

def list_regexp(options):
    r = redis.StrictRedis.from_url(REDIS_URL_RQ)

    if options.source:
        fq = Queue(options.source, connection=r)
    else:
        fq = get_failed_queue(connection=r)

    def exception_matches(job):
        reason = job.exc_info.split('\n')[-2:-1]
        for r in reason:
            match = re.search(options.regexp, r)
            if match:
                return True
        return False

    jobs = fq.get_jobs()

    if options.regexp:
        jobs = [job for job in jobs if exception_matches(job)]

    if options.descr_regexp:
        jobs = [job for job in jobs if re.search(options.descr_regexp, job.description)]

    for job in jobs:
        print job.id

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-q", "--queue", dest="to_q", action="store", type="string", default="default")
    parser.add_option("-S", "--source", dest="source", action="store", type="string", default=None)
    parser.add_option("-t", "--timeout", dest="timeout", action="store", type="int", default=None)
    parser.add_option("-j", "--job", dest="job", action="store", type="string", default=None)
    parser.add_option("-J", "--job-file", dest="job_file", action="store", type="string", default=None)
    parser.add_option("-n", "--newest", dest="newest", action="store_true", default=False)
    parser.add_option("-x", "--commit", dest="action", action="store_true", default=False)
    parser.add_option("-l", "--list-timeouts", dest="list", action="store_true", default=False)
    parser.add_option("-g", "--list-regexp", dest="regexp", action="store", default=None)
    parser.add_option("-G", "--descr-regexp", dest="descr_regexp", action="store", default=None)
    parser.add_option("-m", "--method", dest="method", action="store", default=None)
    parser.add_option("-U", "--update-json", dest="update_dict", action="store", default=None)


    (options, args) = parser.parse_args(sys.argv[1:])


    if options.job_file:
        with open(options.job_file) as f:
            job_list = [job.strip() for job in f]
        requeue(options, job_list)

    if options.job:
        requeue(options, job_list=[options.job])

    if options.list:
        list_timeouts(options)

    if options.regexp or options.descr_regexp:
        list_regexp(options)

    if options.newest:
        r = redis.StrictRedis.from_url(REDIS_URL_RQ)
        fq = get_failed_queue(connection=r)
        newest = fq.get_job_ids()[:1]
        requeue(options, job_list=newest)
