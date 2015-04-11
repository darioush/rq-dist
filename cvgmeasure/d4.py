import re

from contextlib import contextmanager
from plumbum import local
from plumbum.cmd import rm, mkdir, ls

PROJECTS = ['Lang', 'Chart', 'Math', 'Closure', 'Time']

@contextmanager
def refresh_dir(dir, cleanup=True, cleanup_anyways=True):
    rm('-rf', dir)
    mkdir('-p', dir)
    with local.cwd(dir):
        try:
            yield
            if cleanup:
                rm('-rf', dir)
        except:
            if cleanup_anyways:
                rm('-rf', dir)
            raise

@contextmanager
def add_to_path(l):
    for item in reversed(l):
        local.env.path.insert(0, item)
    yield
    for _ in l:
        local.env.path.pop()

@contextmanager
def add_timeout(timeout):
    TIMEOUT_KEY = 'D4J_TEST_TIMEOUT'
    prev_val = local.env.get(TIMEOUT_KEY, None)
    local.env[TIMEOUT_KEY] = timeout
    yield
    if prev_val is None:
        del local.env[TIMEOUT_KEY]
    else:
        local.env[TIMEOUT_KEY] = prev_val

def d4():
    return local['defects4j']

@contextmanager
def checkout(project, version, to):
    d4()('checkout', '-p', project, '-v', "%df" % version, '-w', to)
    with local.cwd(to):
        yield


def get_num_bugs(project, old=False):
    if not project in PROJECTS:
        raise Exception("Bad project")

    if old:
        return {'Chart': 26, 'Closure': 133, 'Time': 27, 'Math': 106, 'Lang': 65}[project]

    return int(d4()('info', '-p', project, '-c').rstrip())

class CoverageCalculationException(Exception):
    pass

def get_tts(project, version):
    tts = d4()('info', '-p', project, '-v', str(version), '-t').rstrip().split('\n')
    assert len(tts) > 0
    return set(tts)


def get_coverage(cvg_tool, tc, generated=False):
    if cvg_tool == 'major':
        if tc == 'reset':
            tc = 'edu.washington.cs.emptyTest.EmptyTest::testNothing'
        cvg = d4()['mutation']
        if generated:
            cvg = cvg['-g']
        output = cvg('-t', tc)
        regexps = {
                r'\s*Mutants generated: (\d+)': 'mt',
                r'\s*Mutants covered: (\d+)': 'mc',
                r'\s*Mutants killed: (\d+)': 'mk',
        }
        fail_file = 'failing-tests.txt'
    else:
        cvg = d4()['coverage', '-T', cvg_tool]
        if generated:
            cvg = cvg['-g']
        output = cvg('-t', tc)
        regexps = {
                r'Lines total: (\d+)': 'lt',
                r'Lines covered: (\d+)': 'lc',
                r'Branches total: (\d+)': 'bt',
                r'Branches covered: (\d+)': 'bc',
        }
        fail_file = 'coverage/coverage_fails'

    result = {}
    def update_dict(line, result):
        for regexp, key in regexps.iteritems():
            match = re.match(regexp, line)
            if match:
                result[key] = int(match.group(1))
    for line in output.split('\n'):
        update_dict(line, result)
    if not all(val in result for val in regexps.values()):
        try:
            with open(fail_file) as f:
                traceback = f.read()
        except:
            traceback = ''
        raise CoverageCalculationException("{traceback}\nCould not calculate coverage for: {cvg_tool}, {tc}".format(
                traceback=traceback, cvg_tool=cvg_tool, tc=tc)
            )

    if denominator_empty(cvg_tool, result):
        raise CoverageCalculationException("Lines Total reported as 0 for: %s, %s" % (cvg_tool, tc))

    return result

def compile_if_needed(cvg_tool):
    if cvg_tool == 'major':
        return
    return d4()('compile')

def is_empty(cvg_tool, results):
    if cvg_tool == 'major':
        return (results['mk'] + results['mc']) == 0
    return (results['lc'] + results['bc']) == 0

def denominator_empty(cvg_tool, results):
    if cvg_tool == 'major':
        return results['mt'] == 0
    return results['lt'] == 0

def get_coverage_files_to_save(cvg_tool):
    return {
        'cobertura': ['cobertura.ser', 'coverage/'],
        'codecover': ['coverage/'],
        'jmockit'  : ['coverage/'],
        'major'    : ['exclude.txt', 'kill.csv', 'mutants.log', '.mutation.log', 'summary.csv', 'testMap.csv', 'mml/'],
    }[cvg_tool]


@contextmanager
def get_tar_gz_file(files, out='output.tar.gz'):
    rm('-rf', out)
    local['tar']['cfz', out](*files)
    with open(out) as f:
        yield f

def get_tar_gz_str(files, out='output.tar.gz'):
    rm('-rf', out)
    local['tar']['cfz', out](*files)
    with open(out) as f:
        result = f.read()
    return result


def test(extra_args=[], generated=False, single_test=None):
    if generated:
        extra_args = ['-g'] + extra_args
    if single_test is not None:
        extra_args = extra_args + ['-t', single_test]
    lines = d4()['test'](*extra_args).rstrip().split('\n')
    count_matches = [re.match(r'Failing tests: (\d+)', line) for line in lines]
    fail_matches = [re.match(r'\s+- (.*)', line) for line in lines]

    non_none_count_matches = [int(x.group(1)) for x in count_matches if x is not None]
    failed_tests = [x.group(1) for x in fail_matches  if x is not None]
    assert len(non_none_count_matches) == 1
    failed_tests_cnt = non_none_count_matches[0]
    assert len(failed_tests) == failed_tests_cnt
    return failed_tests

def get_modified_sources(project, version):
    lines = d4()('info', '-p', project, '-v', str(version), '-m').rstrip().split('\n')
    directory = lines[0]
    files = lines[1:]
    return (directory, files)

####
def _is_ok(i, v):
    min, _, max = v.partition("-")
    if max == '':
        return i == int(min)
    if max == 'MAX':
        return int(min) <= i
    return int(min) <= i <= int(max)

def iter_versions(restrict_project=[], restrict_version=[], old=False, minimum=False):
    for project in PROJECTS:
        if restrict_project and project not in restrict_project:
            continue
        bug_cnt = get_num_bugs(project, old)
        if minimum:
            bug_cnt = min(get_num_bugs(project, not old), bug_cnt)

        for i in xrange(1, bug_cnt + 1):
            if restrict_version and not any(_is_ok(i, v) for v in restrict_version):
                continue
            yield project, i

