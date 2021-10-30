#!/usr/bin/env bash

# WARNING: THIS FILE IS MANAGED IN THE 'BOILERPLATE' REPO AND COPIED TO OTHER REPOSITORIES.
# ONLY EDIT THIS FILE FROM WITHIN THE 'FLYTEORG/BOILERPLATE' REPOSITORY:
# 
# TO OPT OUT OF UPDATES, SEE https://github.com/latchbio/boilerplate/blob/master/Readme.rst

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

# Clone the config.yml file
echo "     - copying ${DIR}/requirements-build.txt to the root directory."
cp "${DIR}"/requirements-build.txt "${DIR}"/../../../requirements-build.txt
