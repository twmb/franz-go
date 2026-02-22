#!/bin/bash

# Usage: ./run_tests.sh [options]
# Options:
#   -t, --test PATTERN     Test to run (default: all tests)
#                          Examples: Txn, Group, Txn/range, Group/sticky
#   -n, --iterations NUM   Max iterations (default: 50)
#   -r, --records NUM      Number of records (default: 100000)
#   --race                 Enable race detector (default: off)
#   -l, --log-level LEVEL  Set log level for both client and server
#   --client-log LEVEL     Set KGO_LOG_LEVEL for the test client only
#   --server-log LEVEL     Set kfake server log level only
#   -v, --version VERSION  Kafka version to emulate (e.g., 2.8, 3.5)
#   --pprof ADDR           Enable pprof on server (e.g., :6060)
#   -k, --kill             Kill processes on ports 9092-9094 and exit
#   --clean                Kill servers and remove /tmp/kfake_test_logs
#   -h, --help             Show this help

MAX_ITERATIONS=50
RECORDS=500000
TEST_TYPE=""
RACE=""
CLIENT_LOG=""
SERVER_LOG_LEVEL=""
KFAKE_VERSION="${KFAKE_VERSION:-}"
PPROF_ADDR=""

KFAKE_DIR=/Users/travisbischel/src/twmb/franz-go/pkg/kfake
KGO_DIR=/Users/travisbischel/src/twmb/franz-go/pkg/kgo
LOG_DIR="/tmp/kfake_test_logs"

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
            CLIENT_LOG="$2"
            SERVER_LOG_LEVEL="$2"
            shift 2
            ;;
        --client-log)
            CLIENT_LOG="$2"
            shift 2
            ;;
        --server-log)
            SERVER_LOG_LEVEL="$2"
            shift 2
            ;;
        -v|--version)
            KFAKE_VERSION="$2"
            shift 2
            ;;
        --pprof)
            PPROF_ADDR="$2"
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
        --clean)
            for port in 9092 9093 9094; do
                pid=$(lsof -ti:$port 2>/dev/null)
                if [ -n "$pid" ]; then
                    echo "Killing PID $pid on port $port"
                    kill $pid 2>/dev/null || true
                fi
            done
            rm -rf "$LOG_DIR"
            echo "Removed $LOG_DIR"
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
            echo "  -l, --log-level LEVEL  Set log level for both client and server"
            echo "  --client-log LEVEL     Set KGO_LOG_LEVEL for the test client only"
            echo "  --server-log LEVEL     Set kfake server log level only"
            echo "  -v, --version VERSION  Kafka version to emulate (e.g., 2.8, 3.5)"
            echo "  --pprof ADDR           Enable pprof on server (e.g., :6060)"
            echo "  -k, --kill             Kill processes on ports 9092-9094 and exit"
            echo "  --clean                Kill servers and remove /tmp/kfake_test_logs"
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
    TEST_PATTERN=""
    RUN_ARG=""
else
    TEST_PATTERN="Test${TEST_TYPE}"
    RUN_ARG="-test.run $TEST_PATTERN"
fi
mkdir -p "$LOG_DIR"
SERVER_LOG="$LOG_DIR/server.log"
CLIENT_LOG_FILE="$LOG_DIR/client.log"
SERVER_BIN="$LOG_DIR/kfake-server"
TEST_BIN="$LOG_DIR/kgo-test"
SERVER_PID=""

# Build binaries once up front
echo "Building server binary..."
(cd "$KFAKE_DIR" && go build -o "$SERVER_BIN" main.go) || { echo "FAILED: server build"; exit 1; }

echo "Building test binary..."
(cd "$KGO_DIR" && go test -c $RACE -o "$TEST_BIN") || { echo "FAILED: test build"; exit 1; }

cleanup_interrupt() {
    echo ""
    echo "Interrupted. Server (pid $SERVER_PID) left running."
    echo "Use --clean to kill servers and remove logs."
    SERVER_PID=""
    exit 1
}
trap cleanup_interrupt SIGINT SIGTERM

# Check if a port is in use via a TCP connect attempt.
port_in_use() {
    (echo >/dev/tcp/127.0.0.1/$1) 2>/dev/null
}

if [ -n "$RACE" ]; then
    TIMEOUT="300s"
else
    TIMEOUT="90s"
fi

echo ""
echo "Configuration:"
echo "  Iterations: $MAX_ITERATIONS"
echo "  Records: $RECORDS"
echo "  Test pattern: ${TEST_PATTERN:-all}"
echo "  Race detector: ${RACE:-disabled}"
echo "  Client log level: ${CLIENT_LOG:-default}"
echo "  Server log level: ${SERVER_LOG_LEVEL:-default}"
echo "  Kafka version: ${KFAKE_VERSION:-latest}"
echo "  Pprof: ${PPROF_ADDR:-disabled}"
echo "  Timeout: $TIMEOUT"
echo "  Logs: $LOG_DIR"
echo ""

for i in $(seq 1 $MAX_ITERATIONS); do
    echo "=== Run $i of $MAX_ITERATIONS ==="
    RUN_START=$SECONDS

    # Build server args
    VERSION_ARG=""
    if [ -n "$KFAKE_VERSION" ]; then
        VERSION_ARG="--as-version $KFAKE_VERSION"
    fi
    LOG_ARG=""
    if [ -n "$SERVER_LOG_LEVEL" ]; then
        LOG_ARG="-l $SERVER_LOG_LEVEL"
    fi
    PPROF_ARG=""
    if [ -n "$PPROF_ADDR" ]; then
        PPROF_ARG="-pprof $PPROF_ADDR"
    fi
    "$SERVER_BIN" $VERSION_ARG $LOG_ARG $PPROF_ARG -c group.consumer.heartbeat.interval.ms=1000 > "$SERVER_LOG" 2>&1 &
    SERVER_PID=$!

    # Wait for server to be listening (max 5 seconds)
    for _ in $(seq 1 50); do
        if port_in_use 9092; then
            break
        fi
        sleep 0.1
    done

    # Check if server is still running
    if ! kill -0 $SERVER_PID 2>/dev/null; then
        echo "FAILED: Server crashed on startup (run $i)"
        echo "Server log:"
        cat "$SERVER_LOG"
        exit 1
    fi

    # Run the test
    KGO_TEST_RECORDS=$RECORDS KGO_LOG_LEVEL=$CLIENT_LOG "$TEST_BIN" $RUN_ARG -test.timeout $TIMEOUT > "$CLIENT_LOG_FILE" 2>&1
    TEST_EXIT=$?

    if [ $TEST_EXIT -ne 0 ]; then
        echo "FAILED on run $i"
        echo "Client log: $CLIENT_LOG_FILE"
        echo "Server log: $SERVER_LOG"
        echo ""
        echo "=== Last 50 lines of client log ==="
        tail -50 "$CLIENT_LOG_FILE"
        echo ""
        echo "Server (pid $SERVER_PID) left running for debugging."
        echo "Connect to localhost:9092 to inspect state."
        echo "Kill manually when done: kill $SERVER_PID"
        SERVER_PID=""  # Clear so EXIT trap doesn't kill it
        exit 1
    fi

    # Kill server and wait for clean shutdown (ports freed by Close)
    kill $SERVER_PID 2>/dev/null || true
    wait $SERVER_PID 2>/dev/null

    echo "PASS (run $i) - $((SECONDS - RUN_START))s"
done

echo ""
echo "SUCCESS: All $MAX_ITERATIONS iterations passed!"
