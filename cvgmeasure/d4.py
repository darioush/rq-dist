import re

from contextlib import contextmanager
from plumbum import local
from plumbum.cmd import rm, mkdir, ls

PROJECTS = ['Lang', 'Chart', 'Math', 'Closure', 'Time']

@contextmanager
def refresh_dir(dir, cleanup=True):
    rm('-rf', dir)
    mkdir('-p', dir)
    with local.cwd(dir):
        try:
            yield
            if cleanup:
                rm('-rf', dir)
        except:
            raise

@contextmanager
def add_to_path(l):
    for item in reversed(l):
        local.env.path.insert(0, item)
    yield
    for _ in l:
        local.env.path.pop()

def d4():
    return local['defects4j']

@contextmanager
def checkout(project, version, to):
    d4()('checkout', '-p', project, '-v', "%df" % version, '-w', to)
    with local.cwd(to):
        yield


def get_num_bugs(project):
    if not project in PROJECTS:
        raise Exception("Bad project")
    return int(d4()('info', '-p', project, '-c').rstrip())

class CoverageCalculationException(Exception):
    pass

def get_coverage(cvg_tool, tc):
    cvg = d4()['coverage', '-T', cvg_tool, '-t']
    output = cvg(tc)
    regexps = {
            r'Lines total: (\d+)': 'lt',
            r'Lines covered: (\d+)': 'lc',
            r'Branches total: (\d+)': 'bt',
            r'Branches covered: (\d+)': 'bc',
    }
    result = {}
    def update_dict(line, result):
        for regexp, key in regexps.iteritems():
            match = re.match(regexp, line)
            if match:
                result[key] = int(match.group(1))
    for line in output.split('\n'):
        update_dict(line, result)
    if not all(val in result for val in regexps.values()):
        raise CoverageCalculationException("Could not calculate coverage for: %s, %s" % (cvg_tool, tc))

    if result['lt'] == 0:
        raise CoverageCalculationException("Lines Total reported as 0 for: %s, %s" % (cvg_tool, tc))

    return result


def get_coverage_files_to_save(cvg_tool):
    return {
        'cobertura': ['cobertura.ser', 'coverage/'],
        'codecover': ['coverage/'],
        'jmockit'  : ['coverage/'],
    }[cvg_tool]


def get_tar_gz_str(files, out='output.tar.gz'):
    rm('-rf', out)
    local['tar']['cfz', out](*files)
    with open(out) as f:
        result = f.read()
    return result


def test(extra_args=[]):
    lines = d4()['test'](*extra_args).rstrip().split('\n')
    count_matches = [re.match(r'Failing tests: (\d+)', line) for line in lines]
    fail_matches = [re.match(r'\s+- (.*)', line) for line in lines]

    non_none_count_matches = [int(x.group(1)) for x in count_matches if x is not None]
    failed_tests = [x.group(1) for x in fail_matches  if x is not None]
    assert len(non_none_count_matches) == 1
    failed_tests_cnt = non_none_count_matches[0]
    assert len(failed_tests) == failed_tests_cnt
    return failed_tests

