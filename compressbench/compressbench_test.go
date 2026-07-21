package compressbench

import (
	"fmt"
	"testing"

	"github.com/klauspost/compress/zstd"
)

const maxBatch = 1 << 20 // 1 MiB

type codec struct {
	name string
	new  func() stream
}

func codecs() []codec {
	return []codec{
		{"gzip", newGzip(6)},
		{"zstd", newZstd(zstd.SpeedDefault)},
	}
}

var payloads = []struct {
	name string
	gen  func(size int) genFn
}{
	{"high", highCompressVal},
	{"med", medCompressVal},
	{"low", lowCompressVal},
}

// TestReport prints the density comparison. Run:
//
//	go test -run TestReport -v ./compressbench/
func TestReport(t *testing.T) {
	const targetBytes = 8 << 20 // ~8 MiB uncompressed per config
	valSizes := []int{128, 1024, 16384}

	fmt.Printf("\nmaxBatch=%d KiB   (A = uncompressed-bound/today, B = compressed-bound/proposed)\n", maxBatch>>10)
	fmt.Printf("%-6s %-5s %-6s | %6s %11s %7s | %6s %11s %7s | %7s %7s\n",
		"codec", "data", "valB", "A:nBt", "A:wire", "A:rec/bt", "B:nBt", "B:wire", "B:rec/bt", "batchX", "wire%")
	fmt.Println("-----------------------------------------------------------------------------------------------------")

	for _, c := range codecs() {
		for _, p := range payloads {
			for _, vs := range valSizes {
				n := targetBytes / vs
				wires := genWires(n, vs, p.gen(vs))
				na, wa := packA(wires, maxBatch, c.new, nil)
				nb, wb := packB(wires, maxBatch, c.new, nil)
				fmt.Printf("%-6s %-5s %-6d | %6d %11d %7d | %6d %11d %7d | %6.2fx %6.1f%%\n",
					c.name, p.name, vs,
					na, wa, n/max1(na),
					nb, wb, n/max1(nb),
					float64(na)/float64(max1(nb)),
					100*float64(wb)/float64(max1(wa)))
			}
		}
		fmt.Println()
	}
}

// TestInvariantNeverOverflow fuzzes packB and asserts every sealed batch fits,
// including near-batch-size and incompressible records (the reservation's worst
// case).
func TestInvariantNeverOverflow(t *testing.T) {
	worst := []struct {
		name string
		gen  func(size int) genFn
	}{
		{"high", highCompressVal},
		{"low", lowCompressVal},
	}
	for _, c := range codecs() {
		for _, p := range worst {
			for _, vs := range []int{64, 512, 4096, 100_000} {
				wires := genWires((16<<20)/vs, vs, p.gen(vs))
				maxSeen := 0
				packB(wires, maxBatch, c.new, func(clen int) {
					framed := batchHeader + clen + framingReserve
					if framed > maxSeen {
						maxSeen = framed
					}
					if framed > maxBatch {
						t.Fatalf("%s/%s/val=%d: batch framed size %d exceeds maxBatch %d",
							c.name, p.name, vs, framed, maxBatch)
					}
				})
				t.Logf("%s/%s/val=%-6d max framed batch = %d (%.1f%% of limit)",
					c.name, p.name, vs, maxSeen, 100*float64(maxSeen)/maxBatch)
			}
		}
	}
}

func BenchmarkPack(b *testing.B) {
	const targetBytes = 2 << 20 // ~2 MiB per op
	configs := []struct {
		payload string
		valSize int
		gen     genFn
	}{
		{"high", 1024, highCompressVal(1024)},
		{"low", 1024, lowCompressVal(1024)},
	}
	for _, c := range codecs() {
		for _, cfg := range configs {
			wires := genWires(targetBytes/cfg.valSize, cfg.valSize, cfg.gen)
			b.Run(fmt.Sprintf("%s/%s/A", c.name, cfg.payload), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					packA(wires, maxBatch, c.new, nil)
				}
			})
			b.Run(fmt.Sprintf("%s/%s/B", c.name, cfg.payload), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					packB(wires, maxBatch, c.new, nil)
				}
			})
			b.Run(fmt.Sprintf("%s/%s/AD", c.name, cfg.payload), func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					packAdaptive(wires, maxBatch, c.new, nil)
				}
			})
		}
	}
}

func max1(v int) int {
	if v < 1 {
		return 1
	}
	return v
}
