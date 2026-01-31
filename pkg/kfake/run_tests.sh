#!/bin/bash

# Usage: ./run_tests.sh [options]
# Options:
#   -t, --test PATTERN     Test to run (default: all tests)
#                          Examples: Txn, Group, Txn/range, Group/sticky
#   -n, --iterations NUM   Max iterations (default: 50)
#   -r, --records NUM      Number of records (default: 100000)
#   --race                 Enable race detector (default: off)
#   -l, --log-level LEVEL  Set KGO_LOG_LEVEL (e.g., debug, info)
#   -h, --help             Show this help

MAX_ITERATIONS=50
RECORDS=500000
TEST_TYPE=""
RACE=""
LOG_LEVEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--test)
            TEST_TYPE="$2"
            shift 2
            ;;
        -n|--iterations)
            MAX_ITERATIONS="$2"
            shift 2
            ;;
        -r|--records)
            RECORDS="$2"
            shift 2
            ;;
        --race)
            RACE="-race"
            shift
            ;;
        -l|--log-level)
            LOG_LEVEL="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: ./run_tests.sh [options]"
            echo "Options:"
            echo "  -t, --test PATTERN     Test to run (default: all tests)"
            echo "                         Examples: Txn, Group, Txn/range, Group/sticky"
            echo "  -n, --iterations NUM   Max iterations (default: 50)"
            echo "  -r, --records NUM      Number of records (default: 100000)"
            echo "  --race                 Enable race detector (default: off)"
            echo "  -l, --log-level LEVEL  Set KGO_LOG_LEVEL (e.g., debug, info)"
            echo "  -h, --help             Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Build test pattern
if [ -z "$TEST_TYPE" ]; then
    # Run all tests by default
    TEST_PATTERN=""
    RUN_ARG=""
else
    # User specified a pattern like "Txn", "Group", "Txn/range"
    TEST_PATTERN="Test${TEST_TYPE}"
    RUN_ARG="-run $TEST_PATTERN"
fi
LOG_DIR="/tmp/kfake_test_logs"
mkdir -p "$LOG_DIR"
SERVER_LOG="$LOG_DIR/server.log"
TEST_LOG="$LOG_DIR/test.log"
SERVER_PID=""

cleanup() {
    echo ""
    echo "Interrupted, cleaning up..."
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
    fi
    lsof -ti:9092 | xargs kill -9 2>/dev/null || true
    lsof -ti:9093 | xargs kill -9 2>/dev/null || true
    lsof -ti:9094 | xargs kill -9 2>/dev/null || true
    exit 1
}
trap cleanup SIGINT SIGTERM

echo "Configuration:"
echo "  Iterations: $MAX_ITERATIONS"
echo "  Records: $RECORDS"
echo "  Test pattern: ${TEST_PATTERN:-all}"
echo "  Race detector: ${RACE:-disabled}"
echo "  Log level: ${LOG_LEVEL:-default}"
echo "  Logs: $LOG_DIR"
echo ""

for i in $(seq 1 $MAX_ITERATIONS); do
    echo "=== Run $i of $MAX_ITERATIONS ==="

    # Kill any existing server and wait for ports to be free
    pkill -f "__kfake" 2>/dev/null || true
    lsof -ti:9092 | xargs kill -9 2>/dev/null || true
    lsof -ti:9093 | xargs kill -9 2>/dev/null || true
    lsof -ti:9094 | xargs kill -9 2>/dev/null || true
    sleep 3

    # Start server and capture its output
    cd /Users/travisbischel/src/twmb/franz-go/pkg/kfake
    go run main.go > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!
    sleep 3

    # Check if server is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "FAILED: Server crashed on startup (run $i)"
        echo "Server log:"
        cat "$SERVER_LOG"
        exit 1
    fi

    # Run the test
    cd /Users/travisbischel/src/twmb/franz-go/pkg/kgo
    KGO_TEST_RECORDS=$RECORDS KGO_LOG_LEVEL=$LOG_LEVEL go test $RACE $RUN_ARG -timeout 600s > "$TEST_LOG" 2>&1
    TEST_EXIT=$?

    # Kill server and ensure it's dead
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null
    lsof -ti:9092 | xargs kill -9 2>/dev/null || true
    lsof -ti:9093 | xargs kill -9 2>/dev/null || true
    lsof -ti:9094 | xargs kill -9 2>/dev/null || true

    if [ $TEST_EXIT -ne 0 ]; then
        echo "FAILED on run $i"
        echo "Test log: $TEST_LOG"
        echo "Server log: $SERVER_LOG"
        echo ""
        echo "=== Last 50 lines of test log ==="
        tail -50 "$TEST_LOG"
        exit 1
    fi

    echo "PASS (run $i)"
done

echo ""
echo "SUCCESS: All $MAX_ITERATIONS iterations passed!"
