#! /usr/bin/env python

from plumbum import local
import re

def main():
    jps = local['jps']['-m']
    jstack = lambda ps: local['jstack'](ps)
    wc = lambda fn: int(local['wc']('-l', fn).rstrip().partition(' ')[0])
    lines = jps().rstrip().split('\n')
    mutations = [line for line in lines if line.endswith('mutation.test')]
    def info(line):
        match = re.match(r'(\d+) ant-launcher.jar .* -Dbasedir=(\S+) .* mutation.test', line)
        pid, dir = int(match.group(1)), match.group(2)
        total_muts = wc(dir + '/mutants.log')

        mutants = jstack(pid).rstrip().split('\n')
        executing_muts = [int(match.group(0)) for match in
                [re.match(r'"Mutant-(\d+)" .*', line) for line in mutants]
                if match is not None]
        print executing_muts

    [info(line) for line in mutations]

#5865 ant-launcher.jar -f /home/ec2-user/defects4j/framework/build-scripts/Math/Math.build.xml -Dscript.dir=/home/ec2-user/defects4j/framework -Dbasedir=/tmp/worker.ip-172-31-25-99.5851/checkout -Dmajor.exclude=/tmp/worker.ip-172-31-25-99.5851/checkout/exclude.txt -Dmajor.kill.log=/tmp/worker.ip-172-31-25-99.5851/checkout/kill.csv -logfile /tmp/worker.ip-172-31-25-99.5851/checkout/.mutation.log -Dtest.entry.class=org.apache.commons.math3.analysis.function.SincTest -Dtest.entry.method=testDerivativeShortcut mutation.test

if __name__ == "__main__":
    main()


