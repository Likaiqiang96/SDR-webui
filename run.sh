#!/bin/bash
# 重启看板（用 pidfile，避免误杀）
PIDFILE=/tmp/webdash.pid
PORT="${PORT:-8095}"
if [ -f "$PIDFILE" ]; then
  kill "$(cat $PIDFILE)" 2>/dev/null
  sleep 1
fi
cd "$(dirname "$0")"
nohup python3 server.py --port "$PORT" > /tmp/webdash.log 2>&1 &
echo $! > "$PIDFILE"
sleep 2
tail -2 /tmp/webdash.log
