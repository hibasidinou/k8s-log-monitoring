#!/bin/bash

# Script to run the Streamlit dashboard
# Usage: ./scripts/run_dashboard.sh [port]

set -e

# Get the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

# Default port
PORT=${1:-8501}

# Check if virtual environment exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
elif [ -d ".venv" ]; then
    echo "Activating virtual environment..."
    source .venv/bin/activate
fi

# Check if streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "Streamlit is not installed. Installing dependencies..."
    pip install -r requirements.txt
fi

# Check if data directory exists
if [ ! -d "data/output" ]; then
    echo "Warning: data/output directory does not exist. Creating it..."
    mkdir -p data/output/dashboard
fi

# Run the dashboard
echo "Starting Streamlit dashboard on port $PORT..."
echo "Dashboard will be available at: http://localhost:$PORT"
echo "Press Ctrl+C to stop the server"

streamlit run src/dashboard/app.py --server.port=$PORT --server.address=localhost

