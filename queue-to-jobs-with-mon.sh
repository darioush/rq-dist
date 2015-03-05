#! /bin/bash

set -e
jobfile=$1
sourceq=$2
timeout=$3
downsize=$4
commit=$5


json_arg="{\"bundle\": \"cvg_tool:project:version\", \"check_key\": \"test-methods-run\", \"list_key\": \"test_methods\", \"monitor_queue\": \"mon\", \"job_queue\": \"jobs\", \"job_name\": \"cvgmeasure.nocover_test_cases.test_cvg_methods\", \"timeout\": $timeout, \"downsize\": $downsize, \"commit\": true}"

echo $json_arg

python requeue.py -J $jobfile -q mon -S $sourceq -m cvgmeasure.monitor.monitor_job -U "$json_arg" $commit
