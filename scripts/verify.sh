#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
.venv/bin/python -m unittest discover -s tests -v
bash -n scripts/setup.sh scripts/kafka.sh
# Giữ cùng phiên shell trong môi trường chạy có dọn tiến trình con khi phiên kết thúc.
trap 'bash scripts/kafka.sh stop' EXIT
bash scripts/kafka.sh start
.venv/bin/python scripts/integration_test.py
