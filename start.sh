#!/usr/bin/env bash
set -xe

python3 data_acquire.py & python3 app.py;
