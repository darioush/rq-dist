import tarfile

from plumbum import local
from plumbum.cmd import ls, cp



cmd = local['/homes/gws/darioush/defects4j/framework/build-scripts/lib/codecover-batch-trunk/codecover.sh']['report', '-c', 'coverage/codecover.xml', '-s', 'merged-session', '-d', 'report_single.html', '-t', '/homes/gws/darioush/defects4j/framework/build-scripts/lib/codecover-batch-trunk/report-templates/HTML_Report_SingleFile.xml']

def do_it(fn):
    with tarfile.open(fn) as tar:
        try:
            tar.getmember('coverage/report_html/report_single.html')
            print "ok"
        except KeyError:
            tar.extract('coverage/codecover.xml', path='.')
            print "Got it from ", fn
            print cmd()


            cp(fn, 'mytar.tar.gz')
            local['gunzip']('mytar.tar.gz')
            with tarfile.open('mytar.tar', 'a') as tar:
                tar.add('report_single.html', arcname='coverage/report_html/report_single.html')
            local['gzip']('mytar.tar')
            cp('mytar.tar.gz', fn)

def fix_fn(project, version, test):
    fname = '/scratch/darioush/files/codecover:%s:%d:%s.tar.gz' % (project, version, test)
    do_it(fname)

def main():
    flist = local['find']('/scratch/darioush/files', '-name', 'cobertura:Lang:.tar.gz').rstrip().split('\n')
    for fn in flist:
        do_it(fn)

if __name__ == "__main__":
    #main()
    fix_fn('Lang', 64, 'org.apache.commons.lang.enums.EnumTest')

