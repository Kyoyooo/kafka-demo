#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p .runtime/downloads .runtime/jdk-17

# Cài JDK riêng: không thay đổi Java mặc định của hệ điều hành.
if [[ ! -x .runtime/jdk-17/bin/java ]]; then
  curl -fsSL --connect-timeout 15 --max-time 60 --retry 2 \
    'https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=x64&image_type=jdk&os=linux&vendor=eclipse' \
    -o .runtime/downloads/jdk-metadata.json
  python3 - <<'PY'
import json
from pathlib import Path
package = json.loads(Path('.runtime/downloads/jdk-metadata.json').read_text())[0]['binary']['package']
Path('.runtime/downloads/jdk-url.txt').write_text(package['link'])
Path('.runtime/downloads/jdk.sha256').write_text(package['checksum'] + '  .runtime/downloads/jdk-17.tar.gz\n')
PY
  curl -fL --connect-timeout 15 --max-time 600 --retry 2 \
    "$(cat .runtime/downloads/jdk-url.txt)" -o .runtime/downloads/jdk-17.tar.gz
  sha256sum -c .runtime/downloads/jdk.sha256
  tar -xzf .runtime/downloads/jdk-17.tar.gz -C .runtime/jdk-17 --strip-components=1
fi

KAFKA_NAME=kafka_2.13-3.9.1
if [[ ! -x ".runtime/$KAFKA_NAME/bin/kafka-server-start.sh" ]]; then
  ARCHIVE=".runtime/downloads/$KAFKA_NAME.tgz"
  if [[ ! -f "$ARCHIVE" ]]; then
    BASE=https://archive.apache.org/dist/kafka/3.9.1
    if ! curl -fL --connect-timeout 8 --max-time 300 "$BASE/$KAFKA_NAME.tgz" -o "$ARCHIVE.part"; then
      BASE=https://repo.huaweicloud.com/apache/kafka/3.9.1
      curl -fL --connect-timeout 15 --max-time 600 --retry 2 "$BASE/$KAFKA_NAME.tgz" -o "$ARCHIVE.part"
    fi
    mv "$ARCHIVE.part" "$ARCHIVE"
  fi
  python3 - "$ARCHIVE" <<'PY'
import hashlib
import sys
from pathlib import Path
archive = Path(sys.argv[1])
# SHA512 từ https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz.sha512
expected = ('1EA204BA73411737A275429CA976D440F007FF0957B90B19'
            'BE41DC5A4BAE52617267769BE9F0B5791714D0B3C4C76060'
            '5BD426FAEA39EDD90763585523FA2CFE')
if hashlib.sha512(archive.read_bytes()).hexdigest() != expected.lower():
    raise SystemExit('SHA512 Kafka không khớp')
print('Kafka SHA512: OK')
PY
  tar -xzf "$ARCHIVE" -C .runtime
fi
python3 -m venv .venv
.venv/bin/python -m pip install --no-cache-dir -r requirements.txt
.runtime/jdk-17/bin/java -version
printf 'Cài đặt hoàn tất. Tiếp theo: bash scripts/kafka.sh start\n'
