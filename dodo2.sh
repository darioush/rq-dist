#! /bin/bash

p=$1
v=$2
s=$3

if [ -z "$4" ]; then
    ./main.py qb-slice cvgmeasure.cvg.generated_cvg -M cvgmeasure.common.M -Z exec -p $p -v $v -K cvg_tool -a codecover -a jmockit -S nonempty:cobertura -q default -b 50 -k tests -T randoop.$s -s suite | wc -l
else
    ./main.py qb-slice cvgmeasure.cvg.generated_cvg -M cvgmeasure.common.M -Z exec -p $p -v $v -K cvg_tool -a codecover -a jmockit -S nonempty:cobertura -q default -b 50 -k tests -T randoop.$s -s suite $4 $5 $6 $7 $8 $9
fi
