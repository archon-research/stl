#!/bin/bash

# Parallel Liquidation Scanner
# Runs multiple instances of failedLiquidations on different block ranges for faster scanning

set -e

# Default values
DATA_DIR="/mnt/data"
START_BLOCK=17203646  # SparkLend deployment block
END_BLOCK=0           # 0 = latest
NUM_PROCESSES=4
OUTPUT_DIR="./parallel_results"
TRACE_ALL=false
FAILED_ONLY=true

usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -d, --data-dir DIR     Erigon data directory (default: $DATA_DIR)"
    echo "  -s, --start BLOCK      Start block number (default: $START_BLOCK)"
    echo "  -e, --end BLOCK        End block number (default: latest)"
    echo "  -p, --processes N      Number of parallel processes (default: $NUM_PROCESSES)"
    echo "  -o, --output-dir DIR   Output directory for results (default: $OUTPUT_DIR)"
    echo "  -t, --trace-all        Use trace-all mode (slower but complete)"
    echo "  -a, --all              Include successful liquidations too"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Example:"
    echo "  $0 -s 17200000 -e 18000000 -p 8 -t"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -d|--data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        -s|--start)
            START_BLOCK="$2"
            shift 2
            ;;
        -e|--end)
            END_BLOCK="$2"
            shift 2
            ;;
        -p|--processes)
            NUM_PROCESSES="$2"
            shift 2
            ;;
        -o|--output-dir)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        -t|--trace-all)
            TRACE_ALL=true
            shift
            ;;
        -a|--all)
            FAILED_ONLY=false
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCANNER="$SCRIPT_DIR/failedLiquidations"

# Check if scanner exists
if [[ ! -x "$SCANNER" ]]; then
    echo "Building scanner..."
    cd "$SCRIPT_DIR"
    go build -o failedLiquidations main.go
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Get latest block if END_BLOCK is 0
if [[ "$END_BLOCK" -eq 0 ]]; then
    echo "Detecting latest frozen block..."
    END_BLOCK=$("$SCANNER" -data-dir="$DATA_DIR" -start=1 -end=1 2>&1 | grep "Frozen blocks available" | grep -oE '[0-9]+' || echo "0")
    if [[ "$END_BLOCK" -eq 0 ]]; then
        echo "Error: Could not detect latest block"
        exit 1
    fi
    echo "Latest frozen block: $END_BLOCK"
fi

# Calculate block range per process
TOTAL_BLOCKS=$((END_BLOCK - START_BLOCK + 1))
BLOCKS_PER_PROCESS=$((TOTAL_BLOCKS / NUM_PROCESSES))
REMAINDER=$((TOTAL_BLOCKS % NUM_PROCESSES))

echo "============================================"
echo "Parallel Liquidation Scanner"
echo "============================================"
echo "Data Directory: $DATA_DIR"
echo "Block Range: $START_BLOCK - $END_BLOCK ($TOTAL_BLOCKS blocks)"
echo "Processes: $NUM_PROCESSES"
echo "Blocks per process: ~$BLOCKS_PER_PROCESS"
echo "Output Directory: $OUTPUT_DIR"
echo "Trace All: $TRACE_ALL"
echo "Failed Only: $FAILED_ONLY"
echo "============================================"
echo ""

# Build command options
TRACE_FLAG=""
if [[ "$TRACE_ALL" == "true" ]]; then
    TRACE_FLAG="-trace-all"
fi

FAILED_FLAG=""
if [[ "$FAILED_ONLY" == "true" ]]; then
    FAILED_FLAG="-failed-only"
fi

# Start processes
PIDS=()
CURRENT_START=$START_BLOCK

for ((i=0; i<NUM_PROCESSES; i++)); do
    # Calculate this process's block range
    EXTRA=0
    if [[ $i -lt $REMAINDER ]]; then
        EXTRA=1
    fi
    CHUNK_SIZE=$((BLOCKS_PER_PROCESS + EXTRA))
    CURRENT_END=$((CURRENT_START + CHUNK_SIZE - 1))
    
    # Don't exceed END_BLOCK
    if [[ $CURRENT_END -gt $END_BLOCK ]]; then
        CURRENT_END=$END_BLOCK
    fi
    
    OUTPUT_FILE="$OUTPUT_DIR/part_${i}_blocks_${CURRENT_START}_${CURRENT_END}.csv"
    LOG_FILE="$OUTPUT_DIR/part_${i}.log"
    
    echo "Starting process $i: blocks $CURRENT_START - $CURRENT_END -> $OUTPUT_FILE"
    
    # Run scanner in background
    "$SCANNER" \
        -data-dir="$DATA_DIR" \
        -start="$CURRENT_START" \
        -end="$CURRENT_END" \
        -output="$OUTPUT_FILE" \
        $TRACE_FLAG \
        $FAILED_FLAG \
        -resume \
        > "$LOG_FILE" 2>&1 &
    
    PIDS+=($!)
    
    # Move to next chunk
    CURRENT_START=$((CURRENT_END + 1))
    
    # Small delay to stagger DB opens
    sleep 2
done

echo ""
echo "All $NUM_PROCESSES processes started."
echo "PIDs: ${PIDS[*]}"
echo ""
echo "Monitoring progress (Ctrl+C to stop monitoring, processes continue in background)..."
echo ""

# Function to check if all processes are done
all_done() {
    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
    done
    return 0
}

# Monitor progress
trap 'echo ""; echo "Stopping monitoring. Processes continue in background."; echo "To kill all: kill ${PIDS[*]}"; exit 0' INT

while ! all_done; do
    echo "--- Progress at $(date '+%H:%M:%S') ---"
    for ((i=0; i<NUM_PROCESSES; i++)); do
        LOG_FILE="$OUTPUT_DIR/part_${i}.log"
        if [[ -f "$LOG_FILE" ]]; then
            # Get last progress line
            PROGRESS=$(grep -E "Progress:|Scan complete" "$LOG_FILE" 2>/dev/null | tail -1 || echo "Starting...")
            if kill -0 "${PIDS[$i]}" 2>/dev/null; then
                echo "  Process $i (PID ${PIDS[$i]}): $PROGRESS"
            else
                # Check exit status
                wait "${PIDS[$i]}" 2>/dev/null && STATUS="Done ✓" || STATUS="Failed ✗"
                echo "  Process $i: $STATUS - $PROGRESS"
            fi
        else
            echo "  Process $i: Waiting for log..."
        fi
    done
    echo ""
    sleep 10
done

echo "============================================"
echo "All processes completed!"
echo "============================================"

# Combine results
COMBINED_FILE="$OUTPUT_DIR/combined_liquidations.csv"
echo "Combining results into $COMBINED_FILE..."

# Write header from first file
FIRST_FILE=$(ls "$OUTPUT_DIR"/part_*.csv 2>/dev/null | head -1)
if [[ -f "$FIRST_FILE" ]]; then
    head -1 "$FIRST_FILE" > "$COMBINED_FILE"
    
    # Append data from all files (skip headers)
    for f in "$OUTPUT_DIR"/part_*.csv; do
        tail -n +2 "$f" >> "$COMBINED_FILE"
    done
    
    # Count results
    TOTAL_LINES=$(($(wc -l < "$COMBINED_FILE") - 1))
    echo "Combined $TOTAL_LINES liquidation calls into $COMBINED_FILE"
else
    echo "No result files found"
fi

echo ""
echo "Done!"
