package weighted

import "math/bits"

// ExactTwoWorkers computes the optimum for two workers exactly, at any size and
// for weights with nothing in common, by asking which loads are reachable at
// all rather than which assignment produces them.
//
// Every subset of the free items gives one reachable load for the first worker,
// and the second worker takes the rest, so one pass building the set of
// reachable sums answers the whole question. Held as a bitmap it costs a word
// per sixty four sums per item, which does not care how large or how unrelated
// the weights are -- only how much there is in total.
//
// This is the search moved out of the space of assignments and into the space
// of loads, where the count of distinct answers is the total weight rather than
// workers raised to the power of items.
func ExactTwoWorkers(in *Instance) (int64, bool) {
	if in.Workers != 2 {
		return 0, false
	}
	var base [2]int64
	var free []int64
	for i := range in.Items {
		e0 := containsInt(in.Items[i].Eligible, 0)
		e1 := containsInt(in.Items[i].Eligible, 1)
		switch {
		case e0 && e1:
			free = append(free, in.Items[i].Weight)
		case e0:
			base[0] += in.Items[i].Weight
		case e1:
			base[1] += in.Items[i].Weight
		default:
			return 0, false
		}
	}
	var freeTotal int64
	for _, w := range free {
		freeTotal += w
	}

	// reach[s] = some subset of the free items sums to s.
	words := int(freeTotal/64) + 1
	reach := make([]uint64, words)
	reach[0] = 1
	for _, w := range free {
		shiftUp(reach, int(w))
	}

	total := base[0] + base[1] + freeTotal
	best := int64(1) << 62
	for s := int64(0); s <= freeTotal; s++ {
		if reach[s/64]&(1<<(s%64)) == 0 {
			continue
		}
		l0 := base[0] + s
		l1 := total - l0
		if sq := l0*l0 + l1*l1; sq < best {
			best = sq
		}
	}
	return best, true
}

func containsInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

// shiftUp ors the bitmap into itself shifted left by n, which is the step that
// adds one more item's weight to every sum already reachable.
func shiftUp(b []uint64, n int) {
	word, bit := n/64, n%64
	for i := len(b) - 1; i >= word; i-- {
		v := b[i-word] << bit
		if bit > 0 && i-word-1 >= 0 {
			v |= b[i-word-1] >> (64 - bit)
		}
		b[i] |= v
	}
	_ = bits.UintSize
}
