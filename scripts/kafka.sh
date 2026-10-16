#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export JAVA_HOME="$ROOT/.runtime/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
export KAFKA_HEAP_OPTS="-Xms256m -Xmx512m"
export LOG_DIR="$ROOT/.runtime/logs"
KAFKA="$ROOT/.runtime/kafka_2.13-3.9.1"
mkdir -p .runtime/config .runtime/data .runtime/logs .runtime/pids

alive() {
  [[ -f ".runtime/pids/$1.pid" ]] && kill -0 "$(cat ".runtime/pids/$1.pid")" 2>/dev/null
}
wait_port() {
  .venv/bin/python - "$1" "${2:-}" <<'PY'
import os
import socket
import sys
import time
port = int(sys.argv[1])
for _ in range(120):
    if sys.argv[2]:
        from pathlib import Path
        pid = int(Path(f'.runtime/pids/{sys.argv[2]}.pid').read_text())
        try:
            os.kill(pid, 0)
            if Path(f'/proc/{pid}/stat').read_text().split()[2] == 'Z':
                raise ProcessLookupError()
        except (ProcessLookupError, FileNotFoundError):
            raise SystemExit(f'{sys.argv[2]} đã thoát; xem .runtime/logs/')
    try:
        with socket.create_connection(('127.0.0.1', port), timeout=0.5):
            break
    except OSError:
        time.sleep(0.5)
else:
    raise SystemExit(f'Không mở được cổng {port}; xem .runtime/logs/')
PY
}

case "${1:-status}" in
  start)
    [[ -x "$JAVA_HOME/bin/java" && -d "$KAFKA" && -x .venv/bin/python ]] || {
      printf 'Chạy bash scripts/setup.sh trước.\n' >&2; exit 1;
    }
    cat > .runtime/config/zookeeper.properties <<EOF
dataDir=$ROOT/.runtime/data/zookeeper
clientPort=2181
clientPortAddress=127.0.0.1
maxClientCnxns=60
admin.enableServer=false
EOF
    cat > .runtime/config/server.properties <<EOF
broker.id=1
listeners=PLAINTEXT://127.0.0.1:9092
advertised.listeners=PLAINTEXT://127.0.0.1:9092
log.dirs=$ROOT/.runtime/data/kafka
zookeeper.connect=127.0.0.1:2181
num.partitions=3
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
min.insync.replicas=1
log.retention.hours=168
log.cleanup.policy=delete
auto.create.topics.enable=false
group.initial.rebalance.delay.ms=0
EOF
    # Không dùng nhầm dịch vụ Kafka/ZooKeeper đang có sẵn trên máy.
    .venv/bin/python - <<'PY'
import socket
from pathlib import Path
import os
for name, port in [('zookeeper', 2181), ('kafka', 9092)]:
    pid = Path(f'.runtime/pids/{name}.pid')
    try:
        if pid.exists():
            os.kill(int(pid.read_text()), 0)
            continue
    except (ProcessLookupError, ValueError):
        pass
    with socket.socket() as sock:
        try:
            sock.bind(('127.0.0.1', port))
        except OSError as error:
            raise SystemExit(f'Cổng {port} không khả dụng: {error}')
PY
    if ! alive zookeeper; then
      nohup "$KAFKA/bin/zookeeper-server-start.sh" "$ROOT/.runtime/config/zookeeper.properties" > .runtime/logs/zookeeper.out 2>&1 < /dev/null &
      echo $! > .runtime/pids/zookeeper.pid
    fi
    wait_port 2181 zookeeper
    if ! alive kafka; then
      nohup "$KAFKA/bin/kafka-server-start.sh" "$ROOT/.runtime/config/server.properties" > .runtime/logs/kafka.out 2>&1 < /dev/null &
      echo $! > .runtime/pids/kafka.pid
    fi
    if ! wait_port 9092 kafka; then
      # Sau khi máy/process bị tắt đột ngột, ZooKeeper cần hết hạn session cũ.
      if grep -q 'NodeExistsException' .runtime/logs/kafka.out; then
        printf 'Chờ ZooKeeper hết hạn session broker cũ (20 giây)...\n'
        sleep 20
        nohup "$KAFKA/bin/kafka-server-start.sh" "$ROOT/.runtime/config/server.properties" > .runtime/logs/kafka.out 2>&1 < /dev/null &
        echo $! > .runtime/pids/kafka.pid
        wait_port 9092 kafka
      else
        exit 1
      fi
    fi
    .venv/bin/python -m kafka_lab.topics
    ;;
  stop)
    for service in kafka zookeeper; do
      if alive "$service"; then
        pid="$(cat ".runtime/pids/$service.pid")"
        # Chỉ dừng đúng Java process được khởi động từ thư mục này.
        if [[ -r "/proc/$pid/cmdline" ]] && tr '\0' '\n' < "/proc/$pid/cmdline" | grep -Fq "$ROOT/.runtime/config/"; then
          kill -TERM "$pid"
          for ((i=0; i<60; i++)); do
            if ! kill -0 "$pid" 2>/dev/null; then break; fi
            sleep 0.5
          done
          printf 'Đã gửi lệnh dừng %s (PID %s).\n' "$service" "$pid"
        else
          printf 'PID %s không thuộc bài thực hành; bỏ qua.\n' "$pid" >&2
        fi
      fi
    done
    ;;
  status)
    for service in zookeeper kafka; do
      if alive "$service"; then printf '%s: running (PID %s)\n' "$service" "$(cat ".runtime/pids/$service.pid")";
      else printf '%s: stopped\n' "$service"; fi
    done
    ;;
  topics)
    .venv/bin/python -m kafka_lab.topics
    ;;
  *) printf 'Dùng: bash scripts/kafka.sh {start|stop|status|topics}\n' >&2; exit 2 ;;
esac
