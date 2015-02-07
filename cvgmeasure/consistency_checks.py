import os
import tarfile
import xpath
import re

from plumbum import local
from plumbum.cmd import rm, mkdir, ls
from redis import StrictRedis
from xml.dom.minidom import parse

from cvgmeasure.conf import get_property
from cvgmeasure.common import job_decorator, mk_key
from cvgmeasure.d4 import d4, refresh_dir, add_to_path, checkout, test


class SubMismatch(Exception):
    pass

class LenMismatch(Exception):
    pass

class DupMismatch(Exception):
    pass

class TestFail(Exception):
    pass

def no_dups(it1, label1):
    if len(it1) != len(set(it1)):
        raise DupMismatch("There are duplicated in %s" % label1)

def check_sub(it1, label1, it2, label2):
    s2 = set(it2)
    diff = [item for item in it1 if item not in s2]
    if len(diff) > 0:
        missings_str = ' '.join(diff)
        raise SubMismatch("(%s) were in %s but missing from %s" % (missings_str, label1, label2))

def check_eq(it1, label1, it2, label2):
    check_sub(it1, label1, it2, label2)
    check_sub(it2, label2, it1, label1)


def with_fails(fun):
    try:
        fun()
    except Exception as e:
        return [e]
    return []

def plausable_static_field(project, version, t):
    print "----------------"
    # TODO: only works locally for now
    tarpath = local.path('/scratch/darioush/files') / ("cobertura:%s:%d:%s.tar.gz" % (project, version, t))
    with tarfile.open(str(tarpath)) as tar:
        f = tar.extractfile('coverage/coverage.xml')
        tree = parse(f)
        covered_lines_ok_clinit = xpath.find('//class/methods/method[@name="<clinit>"]/lines/line[@hits>0]', tree)
        covered_lines_ok_init = xpath.find('//class/methods/method[@name="<init>" and @signature="()V"]/lines/line[@hits>0]', tree)
        covered_lines_ok = covered_lines_ok_clinit + covered_lines_ok_init
        covered_lines    = xpath.find('//class/methods/method/lines/line[@hits>0]', tree)
        assert len(covered_lines) > 0
        covered_line_numbers_ok = [int(node.getAttribute('number')) for node in covered_lines_ok]
        covered_line_numbers    = [int(node.getAttribute('number')) for node in covered_lines   ]

        result = all(number in covered_line_numbers_ok for number in covered_line_numbers)

        print result, covered_line_numbers, covered_line_numbers_ok, project, version, t
        return result

    return False

def plausable_codecover_field(project, version, t):
    # TODO: only works locally for now
    tarpath = local.path('/scratch/darioush/files') / ("codecover:%s:%d:%s.tar.gz" % (project, version, t))
    with tarfile.open(str(tarpath)) as tar:
        f = tar.extractfile('coverage/report_html/report_single.html')
        tree = parse(f)
        nodes = xpath.find('//span[@class="covered fullyCovered Statement_Coverage"]', tree)
        covered_line_numbers = [int(xpath.find("td/a/text()", node.parentNode.parentNode)[0].nodeValue) for node in nodes]
        covered_code = [xpath.find('text()', node)[0].nodeValue for noe in nodes]

        def is_ok(code):
            return re.match(r'(public|private|)? static (final)? (long) .* = .*;', code) is not None

        result = all(map(is_ok, covered_code))
        print result, covered_line_numbers, covered_code
        return result

    return False

@job_decorator
def non_empty_match(input, hostname, pid):
    project = input['project']
    version = input['version']

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    r = StrictRedis.from_url(redis_url)
    key = mk_key('test-classes', [project, version])
    test_classes = r.llen(key) #r.lrange(key, 0, -1)


    cobertura, codecover, jmockit = [r.hkeys(mk_key('test-classes-cvg-nonempty', [tool, project, version]))
            for tool in ['cobertura', 'codecover', 'jmockit']]


    exclude_static_fields_from = [('Closure', 117), ('Closure', 47), ('Math', 3), ('Math', 63), ('Lang', 6),
            ('Lang', 17), ('Lang', 19)]
    exclude_static_fields = [t for t in cobertura if t not in codecover and t in jmockit and \
            (project, version) in exclude_static_fields_from and plausable_static_field(project, version, t)]

    codecover_exception_from = [('Chart', 1), ('Lang', 64)]
    codecover_exception = [t for t in codecover if t not in jmockit and t not in cobertura and \
            (project, version) in codecover_exception_from and plausable_codecover_field(project, version, t)]

    cobertura = [t for t in cobertura if t not in exclude_static_fields]
    jmockit   = [t for t in jmockit   if t not in exclude_static_fields]
    codecover = [t for t in codecover if t not in codecover_exception]
    core = set(cobertura) & set(codecover) & set(jmockit)

    cobertura_, codecover_, jmockit_ = [[t for t in l if t not in core] for l in (cobertura, codecover, jmockit)]

    print test_classes, '/', len(core), "Agreement"
    print len(exclude_static_fields), " Excluded from jmockit and cobertura as static field initializers"
    print len(codecover_exception), " Excluded from codecover as static field initializers"

    print "---"
    print len(cobertura_), sorted(cobertura_)
    print len(codecover_), sorted(codecover_)
    print len(jmockit_), sorted(jmockit_)

    fails = []
    fails.extend(with_fails(lambda: check_sub(cobertura, 'cobertura', codecover, 'codecover')))
    fails.extend(with_fails(lambda: check_sub(codecover, 'codecover', cobertura, 'cobertura')))
    fails.extend(with_fails(lambda: check_sub(cobertura, 'cobertura', jmockit, 'jmockit')))
    fails.extend(with_fails(lambda: check_sub(jmockit, 'jmockit', cobertura, 'cobertura')))
    fails.extend(with_fails(lambda: check_sub(codecover, 'codecover', jmockit, 'jmockit')))
    fails.extend(with_fails(lambda: check_sub(jmockit, 'jmockit', codecover, 'codecover')))


    if fails:
        raise SubMismatch(' AND '.join([str(ex) for ex in fails]))


    return "Success"


