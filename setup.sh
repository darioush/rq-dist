#! /bin/bash

set -e

dir=$1
d4jdir=$2
gituser=$3
gitpassword=$4
is_aws=$5

RQ_DIST="darioush/rq-dist"
D4J="uwplse/defects4j"
D4J_BRANCH="coverage_paper"

function progress {
    echo "*** $1";
}

function Pushd {
    pushd $1 > /dev/null;
}

function Popd {
    popd > /dev/null
}

# Step 0: Install system packages
if [ -n "$is_aws" ]; then
    swap=`/sbin/swapon -s`;
    if [ -z "$swap" ]; then
        echo "Creating swap...";
        if [ -e "/dev/sdb" ]; then
            sudo /sbin/mkswap /dev/sdb
            sudo /sbin/swapon /dev/sdb
        else
            sudo /bin/dd if=/dev/zero of=/var/swap.1 bs=1M count=2048
            sudo /sbin/mkswap /var/swap.1
            sudo /sbin/swapon /var/swap.1
        fi
    fi
    sudo yum --quiet updateinfo >/dev/null
    sudo yum -y --quiet install git subversion python27 python27-setuptools python27-devel patch htop
    sudo easy_install-2.7 virtualenv
    progress "+sys"
else
    progress "-sys"
fi

# Step 1: Clone / update code rq-dist repository
if [ -e $dir ]; then
    Pushd $dir
        git pull origin
    Popd
    progress "-rqdist"
else
    git clone "http://github.com/$RQ_DIST" $dir
    progress "+rqdist"
fi

# Step 2: Update venv
if [ -e "$dir/env" ]; then
    echo; # skip
else
    Pushd $dir
        virtualenv env
    Popd
fi
Pushd $dir
    ./update-venv.sh
    progress "+venv"
Popd

# Step 3: Clone d4j repository
if [ -e $d4jdir ]; then
    Pushd $d4jdir
        git pull origin
    Popd
    progress "-d4jclone"
else
    git clone -b $D4J_BRANCH "https://${gituser}:${gitpassword}@github.com/$D4J" $d4jdir
    progress "+d4jclone"
fi

# Step 4: Check for repos
Pushd $d4jdir
    ./get-repos.sh
    progress "+repos"
Popd

