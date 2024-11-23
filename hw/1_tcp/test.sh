#!/usr/bin/env bash
./venv/bin/python -m pip install mod/

set -xeuo pipefail

./venv/bin/pytest -v protocol_test.py -o log_cli=true --durations=0
