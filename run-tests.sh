#! /bin/bash
set -e

project=$1
version=$2
suite=$3
commit=$4

# get lists
json="{\"suite\": \"$suite\"}"
python main.py qb cvgmeasure.cvg.test_lists_gen -q default -t 1800 -p $project -v $version -j "$json" $commit


# run tests
#python main.py qb-slice cvgmeasure.cvg.run_tests_gen -q default -t 7200 -p Math -v 1-MAX -k tests -S test-methods -T evosuite-branch.0 -j '{"suite": "evosuite-branch.0", "passcnt":3}' -K cvg_tool -a exec -Z test-methods-exec -b 100

# run cvg!!
#python main.py qb-slice cvgmeasure.cvg.generated_cvg -q observational -t 1800 -b 30 -p Chart -v 1 -k tests -S test-methods -T evosuite-branch.0 -j '{"suite": "evosuite-branch.0"}' -F cvgmeasure.filters.gen -A evosuite-branch.0 -K cvg_tool -a cobertura -Z test-methods-exec
