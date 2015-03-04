import boto.ec2
import json

def main():
    ec2 = boto.ec2.connect_to_region('us-west-2')
    instances = ec2.get_only_instances(filters={'instance-state-name':'running'}) #filters={'tag:runner': "true"})
    print json.dumps([{"id": instance.id, "public": instance.public_dns_name, "private": instance.private_dns_name, "placement": instance.placement} for instance in instances], indent=1)


if __name__ == "__main__":
    main()

