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

## This is so bad. Something something research tradeoff
terms_only = {}
branches_only = {}
access_method = {}
bc_unreachable = {}
cobertura_is_init = {}

def get_tgs_cobertura_raw(tar):
    f = tar.extractfile('coverage/coverage.xml')
    tree = parse(f)
    lines = xpath.find('//class/methods/method/lines/line', tree)
    result = []
    for line in lines:
        fname = line.parentNode.parentNode.parentNode.parentNode.getAttribute('filename')
        number, hits = int(line.getAttribute('number')), int(line.getAttribute('hits'))
        is_init = line.parentNode.parentNode.getAttribute('name') == '<init>'
        is_clinit = line.parentNode.parentNode.getAttribute('name') == '<clinit>'
        is_access = line.parentNode.parentNode.getAttribute('name').startswith('access$')
        result.append((fname,number,hits, is_init, is_clinit, is_access))
    return result

def get_tgs_cobertura(tar):
    tgs = {}
    inits = {}
    clinits = {}
    for fname, lnumber, hits, is_init, is_clinit, is_access in get_tgs_cobertura_raw(tar):
        key = (fname, lnumber)
        if key in tgs:
            if inits.get(key, None) or is_init:
                print "Warning -- skipping %s:%d because it was an init dup" % key
            elif clinits.get(key, None) or is_clinit:
                print "Warning -- skipping %s:%d because it was a clinit dup" % key
            else:
                raise Exception("Duplicate line number in report from cobertura: %s:%d" % key)
        tgs[key] = 1 if hits > 0 else 0
        inits[key] = 1 if is_init else 0
        access_method[key] = 1 if is_access else 0
        clinits[key] = 1 if is_clinit else 0
        cobertura_is_init[key] = inits[key]
    return tgs


def get_tgs_codecover_raw(tar):
    ## overview / filenames information
    f = tar.extractfile('coverage/report.csv')
    reader = csv.reader(f)
    packages = set([])
    name_to_full_name_map = {} ## short name -> array of full names

    for row in reader:
        if row[2] == 'package':
            packages.add(row[0])
        if row[2] == 'class' and '.'.join(row[0].split('.')[:-1]) in packages:
            name = row[0].split('.')[-1]
            if name in name_to_full_name_map:
                l = name_to_full_name_map[name]
            else:
                l = []
            l.append(row[0].replace('.', '/') + '.java')
            name_to_full_name_map[name] = l

    ## code coverage information
    f = tar.extractfile('coverage/report_html/report_single.html')
    tree = parse(f)

    ## next, read hyperlinking information from the overview table!
    tbody = xpath.find('//tbody[@class="overview"]', tree)[0]
    trs = [elem for elem in tbody.getElementsByTagName("tr")]
    first_tds = [tr.getElementsByTagName("td")[0] for tr in trs]

    first_tds_names = reduce(lambda a,b: a+b,
            [[(a.getAttribute("href"), a.firstChild.nodeValue.strip()) for a in
                td.getElementsByTagName("a")] for td in first_tds])

    filtered_tds_names = [(x,y) for (x,y) in first_tds_names if y in name_to_full_name_map]

    xrefs = [xpath.findnode('//a[@name="%s"]' % name[1:], tree) for (name, _) in filtered_tds_names]
    code_hash = [myx.parentNode.parentNode.getAttribute('id') for myx in xrefs]
    regexp_match = [re.match('F(\d+)(L\d+)?', x) for x in code_hash]
    regexp_numbers = [int(match.group(1)) if match else 0 for match in regexp_match]
    zipped_numbers = zip(regexp_numbers, map(lambda (_, x): x, filtered_tds_names))

    def relevant_numbers(fn):
        return [x for (x, y) in zipped_numbers if y == fn]

    #print name_to_full_name_map
    #print filtered_tds_names
    #print regexp_numbers
    #print zipped_numbers

    ## next build up this map
    fmap = {name : zip(name_to_full_name_map[name], relevant_numbers(name)) for name in name_to_full_name_map}

    #print fmap

    ## and the short fname map
    short_name_elems = [s.replace('.java', '') for s in xpath.findvalues('//thead[@class="code"]/tr/th/text()', tree)]
    #print short_name_elems


    ## lines = xpath.find('//tbody[@class="code"]/tr[@class="code"]/td[@class="code text"]', tree)
    ## parse lines
    def get_lines():
        tbodys = xpath.find('//tbody[@class="code"]', tree)
        trs = reduce(lambda a,b: a+b,
                [[elem for elem in tbody.getElementsByTagName("tr") if elem.getAttribute('class') == 'code']
                    for tbody in tbodys])
        tds = reduce(lambda a,b: a+b,
                [[elem for elem in tr.getElementsByTagName("td") if elem.getAttribute('class') == 'code text']
                    for tr in trs])
        return tds
    lines = get_lines()
    result = []
    for line in lines:
        lnumberStr = line.parentNode.getAttribute('id')
        if not lnumberStr.startswith('F'):
            lnumberStr = 'F0' + lnumberStr

        fnumber, lnumber = map(int, re.match(r'F(\d+)L(\d+)', lnumberStr).groups())

        text = []
        def get_text_nodes(n):
            if n.nodeType == line.TEXT_NODE:
                text.append(n.nodeValue)
            for child in n.childNodes:
                get_text_nodes(child)

        get_text_nodes(line)
        code = ''.join(text).strip()
        is_unreachable_in_bytecode = code in [
            "continue;", "break;"
        ]

        fully_cvrd, partially_cvrd, not_cvrd = [
                len(xpath.find('span[contains(@class, "%s")]' % token, line)) > 0
                for token in ("fullyCovered", "partlyCovered", "notCovered")
        ]
        terms_only = all(
                len(xpath.find('span[contains(@class, "%s_Coverage")]' % token, line)) == 0
                for token in ("Loop", "Branch", "Statement", "Operator")
        )
        branches_only = all( ## Terms are allowed too, e.g., } else if { ... 
                len(xpath.find('span[contains(@class, "%s_Coverage")]' % token, line)) == 0
                for token in ("Loop", "Statement", "Operator")
        )


        ## ok now, this is ugly::::
        this_line_short_fname = short_name_elems[fnumber]
        #print fmap[this_line_short_fname], fnumber
        ## search in the fmap for the last item that has idx <= this fnumber!!!!
        this_line_full_name = [full for (full, idx) in fmap[this_line_short_fname] if idx <= fnumber][-1]

        result.append(((this_line_full_name, lnumber) , fully_cvrd, partially_cvrd, not_cvrd, terms_only, branches_only, is_unreachable_in_bytecode))
    return result



def get_tgs_codecover(tar):
    tgs = {}
    for (fname, lnumber), full, partial, nocover, term, branch_only, bc_un in get_tgs_codecover_raw(tar):
        key = (fname, lnumber)
        if full or partial:
            tgs[key] = 1
        elif nocover:
            tgs[key] = 0
        terms_only[key] = term
        branches_only[key] = branch_only
        bc_unreachable[key] = bc_un
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

def assert_agree(ex, tool1, tool2, f, n, test):
    try:
        assert ex[tool1] == ex[tool2]
    except:
        raise AgreementException("Tools %s and %s disagree on tg %s:%d: %s vs. %s [test: %s]" % (tool1,
            tool2, f, n, ex[tool1], ex[tool2], test))



def known_exception(f, n, tgs, test):

    if test in ['org.apache.commons.lang3.RandomStringUtilsTest::testRandomStringUtils']:
        print "Warning -- allowing bypass for known random test"
        return True

    def get_chrs(n):
        return [tgs[tool].get((f, n), 'x') for tool in ['cobertura', 'codecover', 'jmockit']]

    if get_chrs(n) == [1, 0 ,1] and f == 'org/joda/time/DateTimeZone.java' and any((
            123 <= n <= 224,
            290 <= n <= 315,
            316 <= n <= 349,
            544 <= n <= 626,
    )):
        print "Warning -- codecover messes with static TZ %s:%d" % (f, n)
        return True

    if get_chrs(n) == [1, 0, 1] and cobertura_is_init[(f, n)] and get_chrs(n+1) == [0, 'x', 0]:
        print "Warning -- super call from init has weird bytecode desugaring: %s:%d" % (f, n)
        return True

    if get_chrs(n) == [0, 1, 0] and (test in [
            'org.apache.commons.lang.enums.ValuedEnumTest::testCompareTo_classloader_equal',
            'org.apache.commons.lang.enums.ValuedEnumTest::testCompareTo_classloader_different',
    ] or
        any([
            (f, n) == ('org/joda/time/chrono/GJChronology.java', 440),  ## invoked from <CLINIT>
            (f, n) == ('org/joda/time/chrono/GJChronology.java', 297), ## issue with compile I haven't figured out exactly?!
            (f, n) == ('org/joda/time/chrono/GJChronology.java', 299), ## issue with compile I haven't figured out exactly?!
        ])
    ):
        print "Warning -- in these cases class loader is messed with and cobertura and jmockit are wrong. %s:%d" % (f, n)
        return True

    if get_chrs(n) == [1, 0, 1] and test in [
            'org.apache.commons.lang.enums.ValuedEnumTest::testCompareTo_null',
    ]:
        print "Warning -- codecover is wrong : %s:%d" % (f, n)
        return True

    if get_chrs(n) == [0, 1, 0] and bc_unreachable[(f, n)]:
        print "Warning -- allowing bypass for known impossible coding situation : e.g., break; after else %s:%d" % (f, n)
        return True

    if get_chrs(n) in ([0, 1, 0], [1, 0, 1]) and branches_only[(f, n)]:
        print "Warning -- allowing bypass for line %s:%d because of branch only, e.g., } else {" % (f, n)
        return True

    if get_chrs(n) == [1, 0, 1] and terms_only[(f, n)]:
        print "Warning -- allowing bypass for line: %s:%d because of terms only (split lines, do while)" % (f, n)
        return True
#        prev_line = n - 1
#        while get_chrs(prev_line) == ['x', 0, 'x'] and terms_only[(f, prev_line)]:
#            prev_line -= 1
#
#        if get_chrs(prev_line) == ['x', 1, 'x'] and not terms_only[(f, prev_line)]:
#            print "Warning -- allowing bypass for line: %s:%d because of split lines" % (f, n)
#            return True

    if get_chrs(n) in [[1, 'x', 0], [1, 'x', 'x'], [0, 'x', 'x']] and access_method[(f, n)]:
        print "Warning -- allowing bypass for line %s:%d because of access methods" % (f, n)
        return True

    if get_chrs(n) in [[0, 'x', 'x'], [1, 'x', 'x']] and (f, n) in [
        ('org/apache/commons/lang3/time/DateUtils.java', 1821),
        ('org/apache/commons/lang3/math/Fraction.java', 40),
        ('org/joda/time/field/UnsupportedDurationField.java', 32),
        ('org/joda/time/base/BaseSingleFieldPeriod.java', 46),
        ('org/joda/time/LocalDate.java', 82),
        ('org/joda/time/LocalDateTime.java', 80),
        ('org/joda/time/format/DateTimeParserBucket.java', 449),
    ]:
        print "Warning -- Special casing Iterator / Comparable Object next Method desugarizartion: %s:%d" % (f, n)
        return True

    if get_chrs(n) in [[0, 'x', 'x']] and f == 'org/joda/time/format/DateTimeFormatterBuilder.java' and any((
            2498 <= n <= 2510,
    )):
        print "Warning -- Special casing enum field desugaring : %s:%d " % (f , n)
        return True

# bundle = [qm, project, version]
def pp_tgs(r, bundle, test, tgs, tools, verbose=0):
    all_keys = sorted(reduce(lambda a,b: a|b, [set(tgs[tool].keys()) for tool in tools]))
    for f, n in all_keys:
        def get_chr(tool):
            return str(tgs[tool].get((f, n), 'x'))

        ex = {tool: get_chr(tool) for tool in tools}
        if known_exception(f, n, tgs, test):
            r.sadd(mk_data_key('exceptions', bundle), "%s:%d" % (f,n))
            continue
        assert_agree(ex, 'cobertura', 'jmockit', f, n, test)
        if ex['codecover'] != 'x' and ex['cobertura'] != 'x':
            assert_agree(ex, 'codecover', 'cobertura', f, n, test)

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
            key='qm-computed',
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

