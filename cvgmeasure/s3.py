import socket
import boto.s3
import tarfile

from contextlib import contextmanager
from plumbum import local, LocalPath

from cvgmeasure.conf import get_property

s3 = boto.s3.connect_to_region('us-west-2')

def list_from_s3(bucket_name, bundle, prefix = ''):
    key_name = '/'.join(map(str, bundle)) + '/' + prefix
    b = s3.lookup(bucket_name)
    return b.list(key_name)

def put_into_s3(bucket_name, bundle, t, f):
    key_name = '/'.join(map(str, bundle) + [t])
    b = s3.lookup(bucket_name)
    key = b.lookup(key_name)
    if key is None:
        key = b.new_key(key_name)
    return key.set_contents_from_file(f)


def mkdir_p(dst):
    LocalPath(LocalPath(dst).dirname).mkdir()


class NoFileOnS3(Exception):
    pass


def get_file_from_cache_or_s3(bucket, fn, dst, cache=True):
    hostname, _, _ = socket.gethostname().partition('.')
    look_dirs = get_property('s3_cache', hostname)
    if cache:
        cache_dir = look_dirs[-1:]
    else:
        cache_dir = []

    for d in look_dirs:
        path = (LocalPath(d) / bucket / fn)
        if path.exists():
            mkdir_p(dst)
            path.copy(dst)
            break
    else:
        b = s3.lookup(bucket)
        key = b.lookup(fn)
        if key is None:
            raise NoFileOnS3("Key missing from bucket {bucket}: {key_name}".format(bucket=bucket, key_name=fn))
        mkdir_p(dst)
        with open(dst, 'w') as out_f:
            out_f.write(key.read())

        for d in cache_dir:
            path = (LocalPath(d) / bucket / fn)
            if not path.exists():
                mkdir_p(path)
                LocalPath(dst).copy(path)


@contextmanager
def get_compiled_from_s3(bucket_name, bundle, t, dest_dir):
    key_name = '/'.join(map(str, bundle) + [t])

    # untar the file
    # todo -> make this more pipe-ish
    get_file_from_cache_or_s3(bucket_name, key_name, str(dest_dir / 'compiled.tar.gz'))
    with local.cwd(dest_dir):
        with tarfile.open(str(dest_dir / 'compiled.tar.gz'), mode='r:gz') as tar:
            
            import os
            
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar)
        yield
