import boto.s3

s3 = boto.s3.connect_to_region('us-west-2')

def put_into_s3(bucket_name, bundle, t, f):
    key_name = '/'.join(map(str, bundle) + [t])
    b = s3.lookup(bucket_name)
    key = b.lookup(key_name)
    if key is None:
        key = b.new_key(key_name)
    return key.set_contents_from_file(f)


