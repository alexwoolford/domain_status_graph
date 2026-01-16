#!/bin/bash
# Run a long-running script with caffeinate to prevent sleep
# Usage: ./scripts/run_with_caffeinate.sh python scripts/create_graphrag_layer.py --execute

SCRIPT="$@"

if [ -z "$SCRIPT" ]; then
    echo "Usage: $0 <command>"
    echo "Example: $0 python scripts/create_graphrag_layer.py --execute"
    exit 1
fi

echo "🚀 Starting process with caffeinate (prevents sleep)..."
echo "   Command: $SCRIPT"
echo "   Press Ctrl+C to stop"
echo ""

# caffeinate options:
# -d: Prevent display from sleeping
# -i: Prevent system from idle sleeping
# -m: Prevent disk from idle sleeping
# -s: Prevent system from sleeping (only on AC power)
# -u: Simulate user activity
# -w: Wait for process to exit
caffeinate -d -i -m -s -u -w $SCRIPT

echo ""
echo "✓ Process completed"
