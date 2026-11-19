#!/usr/bin/env bash
set -euo pipefail

# Run from repository root regardless of caller working directory.
cd "$(dirname "$0")"

exec docker compose up --build -d "$@"
