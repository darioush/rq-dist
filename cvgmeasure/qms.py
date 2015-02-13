import os
import xpath
import csv
import re

from plumbum import local
from plumbum.cmd import which
from redis import StrictRedis
from xml.dom.minidom import parse

from cvgmeasure.d4 import d4, checkout, refresh_dir, get_coverage, add_to_path, get_modified_sources
from cvgmeasure.fileaccess import get_file
from cvgmeasure.common import job_decorator, mk_key, mk_data_key, filter_key_list
from cvgmeasure.conf import get_property
from cvgmeasure.conf import REDIS_URL_TG


java = local['java']

def get_tgs_cobertura_raw(tar):
    f = tar.extractfile('coverage/coverage.xml')
    tree = parse(f)
    lines = xpath.find('//class/methods/method/lines/line', tree)
    result = []
    for line in lines:
        fname = line.parentNode.parentNode.parentNode.parentNode.getAttribute('filename')
        number, hits = int(line.getAttribute('number')), int(line.getAttribute('hits'))
        result.append((fname,number,hits))
    return result

def get_tgs_cobertura(tar):
    tgs = {}
    for fname, lnumber, hits in get_tgs_cobertura_raw(tar):
        key = (fname, lnumber)
        if key in tgs:
            raise Exception("Duplicate line number in report from cobertura")
        tgs[key] = 1 if hits > 0 else 0
    return tgs


def get_tgs_codecover_raw(tar):
    ## overview / filenames information
    f = tar.extractfile('coverage/report.csv')
    reader = csv.reader(f)
    fmap = []
    for row in reader:
        if row[2] == 'class':
            fmap.append(row[0].replace('.', '/') + '.java')

    ## code coverage information
    f = tar.extractfile('coverage/report_html/report_single.html')
    tree = parse(f)
    lines = xpath.find('//tr[@class="code"]/td[@class="code text"]', tree)
    result = []
    for line in lines:
        lnumberStr = line.parentNode.getAttribute('id')
        if not lnumberStr.startswith('F'):
            lnumberStr = 'F0' + lnumberStr

        fnumber, lnumber = map(int, re.match(r'F(\d+)L(\d+)', lnumberStr).groups())
        fully_cvrd, partially_cvrd, not_cvrd = [
                len(xpath.find('span[contains(@class, "%s")]' % token, line)) > 0
                for token in ("fullyCovered", "partlyCovered", "notCovered")
        ]
        result.append(((fmap[fnumber], lnumber) , fully_cvrd, partially_cvrd, not_cvrd))
    return result

def get_tgs_codecover(tar):
    tgs = {}
    for (fname, lnumber), full, partial, nocover in get_tgs_codecover_raw(tar):
        key = (fname, lnumber)
        if full or partial:
            tgs[key] = 1
        elif nocover:
            tgs[key] = 0
    return tgs


def get_tgs_jmockit_raw(tar, path):
    f = tar.extractfile('coverage/coverage.ser')
    with open('coverage.ser', 'w') as out:
        out.write(f.read())

    lib_location = str(local.path(path) / '..' / 'lib' / 'serparser.jar')
    ser_parser = java['-cp', lib_location]\
                     ['edu.washington.cs.serParser.ListTgs']\
                     ('lines', 'coverage.ser').rstrip().split('\n')
    result = []
    for line in ser_parser:
        fname_lnumber, cvrStr, totalStr = line.split(',')
        fname, _, lnumberStr = fname_lnumber.partition(':')
        lnumber, cvr, total = map(int, (lnumberStr, cvrStr, totalStr))
        result.append(((fname, lnumber), cvr, total))
    return result

def get_tgs_jmockit(tar, path):
    tgs = {}
    for (fname, lnumber), cvr, total in get_tgs_jmockit_raw(tar, path):
        key = (fname, lnumber)
        assert total > 0
        tgs[key] = 1 if cvr > 0 else 0
    return tgs


def get_tgs(path, tool, project, version, test):
    fkey = [tool, project, version, test]
    f = get_file(fkey)
    with f as o:
        return {
            'cobertura': get_tgs_cobertura,
            'codecover': get_tgs_codecover,
            'jmockit': lambda o: get_tgs_jmockit(tar=o, path=path)
        }[tool](o)


class AgreementException(Exception):
            pass

def assert_agree(ex, tool1, tool2, f, n):
    try:
        assert ex[tool1] == ex[tool2]
    except:
        raise AgreementException("Tools %s and %s disagree on tg %s:%d: %s vs. %s" % (tool1,
            tool2, f, n, ex[tool1], ex[tool2]))


# bundle = [qm, project, version]
def pp_tgs(r, bundle, test, tgs, tools, verbose=0):
    all_keys = sorted(reduce(lambda a,b: a|b, [set(tgs[tool].keys()) for tool in tools]))
    for f, n in all_keys:
        def get_chr(tool):
            return str(tgs[tool].get((f, n), 'x'))

        ex = {tool: get_chr(tool) for tool in tools}
        assert_agree(ex, 'cobertura', 'jmockit', f, n)
        if ex['codecover'] != 'x' and ex['cobertura'] != 'x':
            assert_agree(ex, 'codecover', 'cobertura', f, n)

        if verbose:
            chr_str = '\t'.join(map(get_chr, tools))
            print '%s %s:%d' % (chr_str, f, n)

    before_tgs = r.hlen(mk_data_key('tg-id', bundle))
    ## There is an agreement up to here.
    sat_ids = []
    for f, n in all_keys:
        tg = '%s:%d' % (f,n)
        id = tg_to_id(r, bundle, tg)
        def satisfied_by(tool):
            return tgs[tool].get((f, n), False)
        satisfied = any(map(satisfied_by, tools))
        if satisfied:
            sat_ids.append(id)

    after_tgs = r.hlen(mk_data_key('tg-id', bundle))
    r.sadd(mk_data_key('tgs', bundle + [test]), *sat_ids)
    print "-> %d / %d satisfied (%d new, %d total tgs)" % (len(sat_ids), len(all_keys),
            after_tgs - before_tgs, after_tgs)


def tg_to_id(r, bundle, tg):
    lua = """local g = redis.call('HGET', KEYS[1], ARGV[1]); if (g == false) then local size = redis.call('HLEN', KEYS[1]); redis.call('HSET', KEYS[1], ARGV[1], size+1); redis.call('HSET', KEYS[2], size+1, ARGV[1]); return size+1; else return g; end"""
    script = r.register_script(lua)
    return int(script(keys=[mk_data_key(token, bundle) for token in ('tg-id', 'id-tg')], args=[tg]))

def id_to_tg(r, bundle, tg):
    return r.hget(mk_data_key('id-tg', bundle), tg)

@job_decorator
def setup_tgs(input, hostname, pid):
    project = input['project']
    version = input['version']
    qm      = input['qm']
    tests   = input['tests']
    redo    = input.get('redo', False)
    verbose = input.get('verbose', False)

    work_dir, d4j_path, redis_url = map(
            lambda property: get_property(property, hostname, pid),
            ['work_dir', 'd4j_path', 'redis_url']
    )

    work_dir_path = local.path(work_dir) / ('child.%d' % os.getpid())
    print work_dir

    directory, sources = get_modified_sources(project, version)

    tools = ['cobertura', 'codecover', 'jmockit']

    r = StrictRedis.from_url(redis_url)
    rr = StrictRedis.from_url(REDIS_URL_TG)
    d4j_location = '/'.join(which('defects4j').rstrip().split('/')[:-1])

    with filter_key_list(
            r,
            key='',
            bundle=[qm, project, version],
            list=tests,
            redo=redo,
            other_keys=[],
    ) as worklist:
        for test, callback in worklist:
            with refresh_dir(work_dir_path, cleanup=True):
                print test
                tgs = {tool: get_tgs(d4j_location, tool, project, version, test) for tool in tools}
                pp_tgs(rr, [qm, project, version], test, tgs, tools, verbose=verbose)
                callback()

    return "Success"

