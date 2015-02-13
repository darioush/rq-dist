import socket
import tarfile
import plumbum.path.utils

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
    return tarfile.open(fileobj=fileobj)


def get_file_remote(key):
    cache = LocalPath(CACHE)
    cache.mkdir()
    local_path = cache / (key + '.tar.gz')
    if not local_path.exists():
        with SshMachine(FS) as rem:
            path = rem.path(DIR) / (key + '.tar.gz')
            assert path.exists()
            plumbum.path.utils.copy(path, local_path)
            assert local_path.exists()

    fileobj = open(str(local_path))
    return tarfile.open(fileobj=fileobj)
