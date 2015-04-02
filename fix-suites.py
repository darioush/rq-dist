import sys
import tarfile

from plumbum import local
from plumbum.cmd import mkdir, rm, touch
from optparse import OptionParser

from cvgmeasure.d4 import iter_versions, d4

TOOL='evosuite-branch-fse'
IN_DIR=local.path('/scratch/darioush/evosuite-randoop-suites')
OUT_DIR=local.path('/scratch/darioush/generated-tests')

def get_name_of_tar(tool, project):
    return IN_DIR / "{tool}-{project}-tests.tar".format(tool={
        'randoop': 'randoop',
        'evosuite-branch': 'evosuite',
        'evosuite-branch-fse': 'evosuite-branch-fse',
        'evosuite-strongmutation-fse': 'evosuite-strongmutation-fse',
        'evosuite-weakmutation-fse': 'evosuite-weakmutation-fse',
    }[tool], project=project)

def get_extract_list(tool, project, v, mapping = lambda x: x):
    mapped_v = mapping(v)
    if tool == 'randoop':
        return [(
            '/'.join([project, tool, str(id), "{project}-{version}f-{tool}.{id}.tar.bz2.bak".format(
                project=project, version=mapped_v, tool=tool, id=id
            )]),
            '/'.join([project, str(v), "{tool}-{id}.tar.bz2".format(
                tool=tool, id=id
            )])
        ) for id in xrange(1, 11)]
    if tool == 'evosuite-branch':
        return [(
            '/'.join([project, str(mapped_v), project, tool, str(id),
                "{project}-{version}f-{tool}.{id}.tar.bz2.bak".format(
                    project=project, version=mapped_v, tool=tool, id=id
            )]),
            '/'.join([project, str(v), "{tool}-{id}.tar.bz2".format(
                tool=tool, id=id
            )])
        ) for id in xrange(0, 10)]
    if tool.startswith('evosuite-') and tool.endswith('-fse'):
        suite_type = tool[len('evosuite-'):-len('-fse')]

        return [(
            '/'.join([project, "evosuite-{0}".format(suite_type), str(id),
                "{project}-{version}-evosuite-{suite_type}.{id}.tar.bz2.bak".format(
                    project=project, version=mapped_v, suite_type=suite_type, id=id
            )]),
            '/'.join([project, str(v), "{tool}-{id}.tar.bz2".format(
                tool=tool, id=id
            )])
        ) for id in xrange(1, 31)]

def extract(t, source, dest):
    if dest.exists():
        return
    dest_dir = dest.dirname
    mkdir('-p', dest_dir)
    source_file = t.extractfile(source)
    with open(str(dest), "w") as dest_file:
        dest_file.write(source_file.read())
    source_file.close()

def main(options):
    tool = options.tool
    bunzip2 = local['bunzip2']
    bzip2 = local['bzip2']
    tar = local['tar']

    for project, v in iter_versions(options.restrict_project, options.restrict_version, old=True, minimum=True):
        tarname = get_name_of_tar(tool, project)
        with tarfile.open(str(tarname), "r") as t:

            def mapping(inv, v=v):
                assert inv == v
                _, _, result = d4()('match-commits', '-p', project, '-v', '{0}f'.format(inv),  '-c', 'fse-commit-dbs').rstrip().partition('-> ')
                return int(result)


            for source, dest in get_extract_list(tool, project, v, mapping=mapping):
                alt_source = source[:-len('.bak')]
                try:
                    extract(t, source, OUT_DIR / dest)
                    # check for broken files
                    fixed_file = t.extractfile(alt_source)
                    with tarfile.open(fileobj=fixed_file) as t_fixed:
                        broken_files = [name[:-len('.broken')] for name in t_fixed.getnames() if name.endswith('.broken')]
                    fixed_file.close()

                    # now we have to remove the broken files from the archive
                    if broken_files:
                        plain_name = str(OUT_DIR / dest)[:-len('.bz2')]
                        bunzip2(str(OUT_DIR / dest))

                        # get number of .java currently in archive
                        with tarfile.open(plain_name) as t_current:
                            java_files = [name for name in t_current.getnames()
                                if name.endswith('.java') and not name.endswith('_scaffolding.java')]

                        if len(broken_files) == len(java_files):
                            # we are going to remove them all.
                            rm(plain_name)
                            touch(plain_name[:-len('.tar')] + '.empty')

                        else:
                            for broken_file in broken_files:
                                tar('--delete', '-f', plain_name, './' + broken_file)
                            bzip2(plain_name)

                        print "+ {source} -> {dest} ({broken} / {total} broken)".format(
                                source=source, dest=dest, broken=len(broken_files), total=len(java_files)
                            )
                    else:
                        print "+ {source} -> {dest} (none broken)".format(source=source, dest=dest)

                except KeyError:
                    try:
                        # no .bak file was ever created, so we are good.
                        extract(t, alt_source, OUT_DIR / dest)
                        print "* {source} -> {dest} (as is)".format(source=alt_source, dest=dest)
                    except KeyError:
                        print "- {source} -> missing".format(source=alt_source)
                        touch(str(OUT_DIR / dest)[:-len('.tar.bz2')] + '.missing')



if __name__ == "__main__":

    parser = OptionParser()
    parser.add_option("-p", "--project", dest="restrict_project", action="append")
    parser.add_option("-v", "--version", dest="restrict_version", action="append")

    (options, args) = parser.parse_args(sys.argv)

    options.tool = TOOL
    main(options)
