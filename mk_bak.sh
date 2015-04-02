#! /bin/bash

for i in `find . -name '*.tar.bz2'`; do
    f=`basename $i`;
    d=`dirname $i`;
    echo "$i: $d / $f"
    tar xf $i --xform='s/$/.bak/' $f
    mv ${f}.bak $d
done;
