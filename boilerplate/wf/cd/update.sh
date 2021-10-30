#!/usr/bin/env bash

# WARNING: THIS FILE IS MANAGED IN THE 'BOILERPLATE' REPO AND COPIED TO OTHER REPOSITORIES.
# ONLY EDIT THIS FILE FROM WITHIN THE 'FLYTEORG/BOILERPLATE' REPOSITORY:
# 
# TO OPT OUT OF UPDATES, SEE https://github.com/latchbio/boilerplate/blob/master/Readme.rst

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

echo "     - copying ${DIR}/.github to the root directory."
cp -r "${DIR}"/.github "${DIR}"/../../../.github

