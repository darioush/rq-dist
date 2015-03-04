import boto.ec2

from datetime import datetime, timedelta
from dateutil import parser


ec2 = boto.ec2.connect_to_region('us-west-2')

instance_type = 'c4.xlarge'
end_time = datetime.now()
start_time = end_time - timedelta(minutes=30)
availability_zone='us-west-2c'

prices = ec2.get_spot_price_history(
        instance_type=instance_type,
        start_time=start_time.isoformat(),
        end_time=end_time.isoformat(),
        availability_zone=availability_zone,
        product_description='Linux/UNIX',
    )


total_time, total_cost = 0, 0
for p1, p2 in zip(prices, prices[1:]):
    timediff = parser.parse(p1.timestamp) - parser.parse(p2.timestamp)
    price = p2.price
    total_time += timediff.seconds
    total_cost += price

avg_price = float(total_cost) / total_time * 3600
print "$.%2f" % avg_price

#print [price.price for price in prices]

times = [price.timestamp for price in prices]



import ipdb
ipdb.set_trace()

print prices
