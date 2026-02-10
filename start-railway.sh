#!/bin/sh
set -eu

cd /app/backend
gunicorn "app:create_app()" --bind 0.0.0.0:5000 --workers="${GUNICORN_WORKERS:-2}" &
BACKEND_PID=$!

cleanup() {
  kill "$BACKEND_PID" 2>/dev/null || true
}

trap cleanup INT TERM

cd /app/frontend
exec npm run start -- --hostname 0.0.0.0 --port "${PORT:-3000}"
