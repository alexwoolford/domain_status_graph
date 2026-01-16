#!/bin/bash
# Quick script to check if extraction is running and estimate progress

LOG_FILE=$(ls -t logs/extract_llm_verified_*.log 2>/dev/null | head -1)

if [ -z "$LOG_FILE" ]; then
    echo "No extraction log file found"
    echo "Note: Log files are temporary and may have been cleaned up."
    echo "Run the extraction script to generate a new log file."
    exit 1
fi

echo "📊 Extraction Status"
echo "==================="
echo ""

# Check if process is running
if pgrep -f "extract_with_llm_verification.py" > /dev/null; then
    echo "✅ Process is RUNNING"
    PID=$(pgrep -f "extract_with_llm_verification.py" | head -1)
    echo "   PID: $PID"

    # Check CPU usage
    CPU=$(ps -p $PID -o %cpu= | tr -d ' ')
    echo "   CPU: ${CPU}%"

    # Check memory
    MEM=$(ps -p $PID -o rss= | awk '{printf "%.1f MB", $1/1024}')
    echo "   Memory: $MEM"
else
    echo "❌ Process is NOT running"
fi

echo ""
echo "📝 Latest Log Entries:"
echo "---------------------"
tail -5 "$LOG_FILE" | sed 's/^/   /'

echo ""
echo "🔍 Phase Detection:"
echo "-------------------"
if grep -q "PHASE 2: Parallel LLM verification" "$LOG_FILE"; then
    echo "   ⚠️  PHASE 2 (LLM Verification) - Making OpenAI API calls"
    echo "   💰 Cost: ~\$0.00015 per verification"

    # Try to extract candidate count
    CANDIDATES=$(grep "candidates for LLM verification" "$LOG_FILE" | tail -1 | grep -oE '[0-9]+ candidates' | grep -oE '[0-9]+' || echo "unknown")
    if [ "$CANDIDATES" != "unknown" ]; then
        EST_COST=$(echo "$CANDIDATES * 0.00015" | bc 2>/dev/null || echo "$(($CANDIDATES * 15 / 100000))")
        echo "   📊 Candidates: $CANDIDATES"
        echo "   💵 Estimated cost: \$$EST_COST"
    fi
elif grep -q "Phase 1 complete" "$LOG_FILE"; then
    echo "   ✅ PHASE 1 complete - Waiting for Phase 2"
elif grep -q "Extracting and verifying relationships" "$LOG_FILE"; then
    echo "   🔄 PHASE 1 (Extraction) - NO OpenAI calls yet"
    echo "   ✅ Safe - Only using pre-computed embeddings"

    # Try to estimate progress
    if grep -q "Extracted:" "$LOG_FILE"; then
        LAST_PROGRESS=$(grep "Extracted:" "$LOG_FILE" | tail -1)
        echo "   📊 Last progress: $LAST_PROGRESS"
    else
        echo "   ⏳ Still processing first 500 companies..."
    fi
else
    echo "   🔄 Initializing..."
fi

echo ""
echo "💡 Tips:"
echo "   - Phase 1: No OpenAI costs (uses pre-computed embeddings)"
echo "   - Phase 2: Makes OpenAI calls (only for SUPPLIER/CUSTOMER)"
echo "   - Progress logs every 500 companies in Phase 1"
echo "   - Full run typically takes 20-40 minutes"
