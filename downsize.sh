#! /bin/sh
#python requeue.py -U '{"timeout": 2700, "individual_timeout": 900}' -t 2700 -n -S timeout -T cvgmeasure.common.downsize -q observational --commit
python requeue.py -U '{"timeout": 2700}' -t 2700 -n -S timeout -T cvgmeasure.common.downsize -q default --commit
