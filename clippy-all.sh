#!/bin/bash
set -euo pipefail

# Change into directory where this shell script is located
SCRIPT_ROOT=$(cd -P -- "$(dirname -- "$0")" && pwd -P)
cd "${SCRIPT_ROOT}"

# Check all sources with clippy.
exec cargo clippy --all
