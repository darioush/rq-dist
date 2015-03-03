#! /bin/bash

set -e

dir=$1
d4jdir=$2
gituser=$3
gitpass=$4
is_aws=$5

RQ_DIST="darioush/rq-dist"
D4J="uwplse/defects4j"
D4J_BRANCH="coverage_paper"

function progress {
   echo "$1";
}

# Step 0: Install system packages
if [ -n "$is_aws" ]; then
    sudo yum --quiet updateinfo >/dev/null
    sudo yum -y --quiet install git subversion python-devel python-virtualenv
    progress "+sys"
else
    progress "-sys"
fi

# Step 1: Clone / update code rq-dist repository
if [ -e $dir ]; then
    pushd $dir
        git pull origin
    popd
    progress "-rqdist"
else
    git clone "http://github.com/$RQ_DIST" $dir
    pushd $dir
        virtualenv env
    popd
    progress "+rqdist"
fi

# Step 2: Update venv
pushd $dir
    ./update-venv.sh
    progress "+venv"
popd

# Step 3: Clone d4j repository
if [ -e $d4jdir ]; then
    pushd $d4jdir
        git pull origin
    popd
    progress "-d4jclone"
else
    git clone -b $D4J_BRANCH "https:/${gituser}:${gitpassword}@github.com/$D4J" $d4jdir
    progress "+d4jclone"
fi

# Step 4: Check for repos
pushd $d4jdir
    ./get-repos.sh
    progress "+repos"
popd

