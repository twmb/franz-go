//go:build race

package kgo

// testIsRace reports whether the test binary was built with -race. Some ETL
// tests budget their internal context against full-suite parallel execution
// on a single broker; under -race, the same workload is ~10x slower because
// of race-detector instrumentation on every atomic and map access in the
// fetch/ack hot path.
const testIsRace = true
