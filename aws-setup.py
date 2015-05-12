#! /usr/bin/env python
import json
import sys
import paramiko
import logging

from pssh import ParallelSSHClient

#logging.basicConfig()
logger = logging.getLogger('pssh.host_logger')
logger.setLevel(logging.WARNING)

def main(fn, key_file):
    with open(fn) as f:
        hosts = [item['public'] for item in json.loads(f.read())] #+ sys.argv[1:]
    print len(hosts)

    if sys.argv[1:]:
        hosts = [hosts[int(idx)] for idx in sys.argv[1:]]

    print hosts
    if key_file is not None:
        client_key = paramiko.RSAKey.from_private_key_file(key_file)
        client = ParallelSSHClient(hosts, pkey=client_key, user="ec2-user")
    else:
        client = ParallelSSHClient(hosts, user="ec2-user")

    client.copy_file('setup.sh', 'setup.sh')
    client.copy_file('../.boto', '.boto')
    client.pool.join()
    print "copied"


    output = client.run_command('bash setup.sh rq defects4j darioush password00 yes')
    for host in output:
        print "Host: {0}".format(host)
        PREFIX = "*** "
        filtered_lines = [line[len(PREFIX):] for line in output[host]['stdout'] if line.startswith(PREFIX)]
        print ' '.join(filtered_lines)
        stderr = [line for line in output[host]['stderr']]
        sys.stderr.write('\n'.join(stderr))
        assert client.get_exit_code(output[host]) == 0

    client.pool.join()


    output = client.run_command('df')
    for host in output:
        print "Host: {0}".format(host)
        filtered_lines = [line for line in output[host]['stdout']]
        print ' '.join(filtered_lines)
        stderr = [line for line in output[host]['stderr']]
        sys.stderr.write('\n'.join(stderr))
        assert client.get_exit_code(output[host]) == 0
    client.pool.join()
    print "joined"




if __name__ == "__main__":
    main('hosts.json', '/homes/gws/darioush/mykeypair.pem')

