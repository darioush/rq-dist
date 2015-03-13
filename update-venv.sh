#! /bin/bash

set -e

source env/bin/activate
pip install -r requirements.txt --upgrade
if [ ! -f "env/bin/get-file.py" ]; then
    ln -s ../../get-file.py env/bin/
fi;
