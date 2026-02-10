#!/bin/bash

# Usage: ./run_tests.sh [options]
# Options:
#   -t, --test PATTERN     Test to run (default: all tests)
#                          Examples: Txn, Group, Txn/range, Group/sticky
#   -n, --iterations NUM   Max iterations (default: 50)
#   -r, --records NUM      Number of records (default: 100000)
#   --race                 Enable race detector (default: off)
#   -l, --log-level LEVEL  Set KGO_LOG_LEVEL (e.g., debug, info)
#   -v, --version VERSION  Kafka version to emulate (e.g., 2.8, 3.5)
#   -k, --kill             Kill processes on ports 9092-9094 and exit
#   -h, --help             Show this help

MAX_ITERATIONS=50
RECORDS=500000
TEST_TYPE=""
RACE=""
LOG_LEVEL=""
KFAKE_VERSION="${KFAKE_VERSION:-}"

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
        -v|--version)
            KFAKE_VERSION="$2"
            shift 2
            ;;
        -k|--kill)
            echo "Killing processes on ports 9092, 9093, 9094..."
            for port in 9092 9093 9094; do
                pid=$(lsof -ti:$port 2>/dev/null)
                if [ -n "$pid" ]; then
                    echo "  Killing PID $pid on port $port"
                    kill $pid 2>/dev/null || true
                fi
            done
            echo "Done."
            exit 0
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
            echo "  -v, --version VERSION  Kafka version to emulate (e.g., 2.8, 3.5)"
            echo "  -k, --kill             Kill processes on ports 9092-9094 and exit"
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

cleanup_interrupt() {
    echo ""
    echo "Interrupted, cleaning up..."
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null
    fi
    exit 1
}
trap cleanup_interrupt SIGINT SIGTERM

cleanup_exit() {
    if [ -n "$SERVER_PID" ]; then
        kill $SERVER_PID 2>/dev/null || true
        wait $SERVER_PID 2>/dev/null
    fi
}
trap cleanup_exit EXIT

echo "Configuration:"
echo "  Iterations: $MAX_ITERATIONS"
echo "  Records: $RECORDS"
echo "  Test pattern: ${TEST_PATTERN:-all}"
echo "  Race detector: ${RACE:-disabled}"
echo "  Log level: ${LOG_LEVEL:-default}"
echo "  Kafka version: ${KFAKE_VERSION:-latest}"
echo "  Logs: $LOG_DIR"
echo ""

for i in $(seq 1 $MAX_ITERATIONS); do
    echo "=== Run $i of $MAX_ITERATIONS ==="

    # Wait for ports to be free (max 20 seconds)
    for port in 9092 9093 9094; do
        for _ in $(seq 1 40); do
            if ! lsof -ti:$port >/dev/null 2>&1; then
                break
            fi
            sleep 0.5
        done
    done

    # Start server and capture its output
    cd /Users/travisbischel/src/twmb/franz-go/pkg/kfake
    VERSION_ARG=""
    if [ -n "$KFAKE_VERSION" ]; then
        VERSION_ARG="--as-version $KFAKE_VERSION"
    fi
    KFAKE_LOG_LEVEL=$LOG_LEVEL go run main.go $VERSION_ARG -c group.consumer.heartbeat.interval.ms=100 > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!

    # Wait for server to be listening (max 10 seconds)
    for _ in $(seq 1 20); do
        if lsof -ti:9092 >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done

    # Check if server is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "FAILED: Server crashed on startup (run $i)"
        echo "Server log:"
        cat "$SERVER_LOG"
        exit 1
    fi

    # Run the test (use longer timeout with race detector)
    cd /Users/travisbischel/src/twmb/franz-go/pkg/kgo
    if [ -n "$RACE" ]; then
        TIMEOUT="100s"
    else
        TIMEOUT="60s"
    fi
    KGO_TEST_RECORDS=$RECORDS KGO_LOG_LEVEL=$LOG_LEVEL go test $RACE $RUN_ARG -timeout $TIMEOUT > "$TEST_LOG" 2>&1
    TEST_EXIT=$?

    if [ $TEST_EXIT -ne 0 ]; then
        echo "FAILED on run $i"
        echo "Test log: $TEST_LOG"
        echo "Server log: $SERVER_LOG"
        echo ""
        echo "=== Last 50 lines of test log ==="
        tail -50 "$TEST_LOG"
        echo ""
        echo "Server (pid $SERVER_PID) left running for debugging."
        echo "Connect to localhost:9092 to inspect state."
        echo "Kill manually when done: kill $SERVER_PID"
        SERVER_PID=""  # Clear so EXIT trap doesn't kill it
        exit 1
    fi

    # Kill server and wait for clean shutdown (only on success)
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null

    echo "PASS (run $i)"
done

echo ""
echo "SUCCESS: All $MAX_ITERATIONS iterations passed!"
