#!/usr/bin/env bash

set -e

export DEBIAN_FRONTEND=noninteractive

apt-get update
apt-get install -y --no-install-recommends curl gnupg2

curl -sL https://deb.nodesource.com/setup_8.x | bash -

apt-get update
apt-get install -y --no-install-recommends git nodejs
pip install GitPython
python setup.py compile_assets sdist -q
