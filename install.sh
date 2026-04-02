#!/bin/sh
# Thin wrapper — implementation is install.py (Python 3, stdlib-only for downloads).
ROOT=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
exec python3 "$ROOT/install.py" "$@"
