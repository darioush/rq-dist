import socket
import tarfile

from plumbum import SshMachine, LocalPath
from contextlib import contextmanager

DIR = '/scratch/darioush/files/'
FS = 'monarch.cs.washington.edu'
CACHE = '/scratch/darioush/cache/'


@contextmanager
def get_file(keys):
    key = ':'.join(map(str, keys))
    if socket.gethostname() == FS:
        yield get_file_local(key)
    else:
        yield get_file_remote(key)


def get_file_local(key):
    path = DIR + key + '.tar.gz'
    fileobj = open(path)
    yield tarfile.open(fileobj=fileobj)


def get_file_remote(key):
    cache = LocalPath(CACHE)
    cache.mkdir()
    local_path = str(cache) / (key + '.tar.gz')
    if not local_path.exists():
        with SshMachine(FS) as rem:
            path = rem.path(DIR) / (key + '.tar.gz')
            rem.copy(str(cache))
            assert local_path.exists()

    fileobj = open(str(local_path))
    yield tarfile.open(fileobj=fileobj)
