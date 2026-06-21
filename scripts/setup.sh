#!/usr/bin/env bash
set -eu

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

mkdir -p "${ROOT}/orchestration"
mkdir -p "${ROOT}/streaming"
mkdir -p "${ROOT}/processing/conf"
mkdir -p "${ROOT}/docs"

touch "${ROOT}/docs/.gitkeep"

echo "Project structure created under ${ROOT}"
