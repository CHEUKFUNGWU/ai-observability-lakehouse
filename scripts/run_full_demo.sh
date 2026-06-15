#!/usr/bin/env bash
set -euo pipefail

scripts/run_streaming_demo.sh "${1:-100}"
scripts/run_serving_demo.sh
