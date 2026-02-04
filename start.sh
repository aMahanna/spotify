#!/bin/bash

# txt2kg-minimal start script
# Runs both Python backend and React frontend

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

cleanup() {
    echo ""
    echo "Stopping services..."
    kill $BACKEND_PID 2>/dev/null || true
    kill $FRONTEND_PID 2>/dev/null || true
    exit 0
}

trap cleanup INT TERM

# Setup Python backend
echo "=== Setting up Python backend ==="
cd backend

if [ ! -d ".venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q -r requirements.txt

echo "Starting backend on http://localhost:5000..."
python main.py &
BACKEND_PID=$!

cd ..

# Setup React frontend
echo ""
echo "=== Setting up React frontend ==="
cd frontend

if [ ! -d "node_modules" ]; then
    echo "Installing npm dependencies..."
    npm install
fi

echo "Starting frontend on http://localhost:3001..."
npm run dev &
FRONTEND_PID=$!

cd ..

echo ""
echo "=========================================="
echo "txt2kg-minimal is running!"
echo "=========================================="
echo ""
echo "  Backend:  http://localhost:5000"
echo "  Frontend: http://localhost:3001"
echo ""
echo "Press Ctrl+C to stop both services"
echo ""

wait
