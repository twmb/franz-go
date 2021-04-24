package sticky

import "container/heap"

// Graph maps members to partitions they want to steal.
//
// The representation was chosen so as to avoid updating all members on any
// partition move; move updates are one map update.
type graph struct {
	b *balancer

	// node => edges out
	// "from a node (member), which topicNum could we steal?"
	out [][]uint32

	// edge => who owns this edge; built in balancer's assignUnassigned
	cxns []uint16

	// scores are all node scores from a seach node. The gscore field
	// is reset on findSteal to noScore.
	scores pathScores

	// heapBuf and pathBuf are backing buffers that are reused every
	// findSteal; note that pathBuf must be done being used before
	// the next find steal, but it always is.
	heapBuf pathHeap
	pathBuf []stealSegment
}

func (b *balancer) newGraph(
	partitionConsumers []uint16,
	topicPotentials [][]uint16,
) graph {
	g := graph{
		b:       b,
		out:     make([][]uint32, len(b.members)),
		cxns:    partitionConsumers,
		scores:  make([]pathScore, len(b.members)),
		heapBuf: make([]*pathScore, len(b.members)),
	}
	outBufs := make([]uint32, len(b.members)*len(topicPotentials))
	for memberNum := range b.plan {
		out := outBufs[:0:len(topicPotentials)]
		outBufs = outBufs[len(topicPotentials):]
		// In the worst case, if every node is linked to each other,
		// each node will have nparts edges. We preallocate the worst
		// case. It is common for the graph to be highly connected.
		g.out[memberNum] = out
	}
	for topicNum, potentials := range topicPotentials {
		for _, potential := range potentials {
			g.out[potential] = append(g.out[potential], uint32(topicNum))
		}
	}
	return g
}

func (g *graph) changeOwnership(edge uint32, newDst uint16) {
	g.cxns[edge] = newDst
}

// findSteal uses A* search to find a path from the best node it can reach.
func (g *graph) findSteal(from uint16) ([]stealSegment, bool) {
	// First, we must reset our scores from any prior run. This is O(M),
	// but is fast and faster than making a map and extending it a lot.
	for i := range g.scores {
		g.scores[i].gscore = noScore
		g.scores[i].done = false
	}

	first, _ := g.getScore(from)

	first.gscore = 0
	first.fscore = h(first, first)
	first.done = true

	g.heapBuf = append(g.heapBuf[:0], first)
	rem := &g.heapBuf
	for rem.Len() > 0 {
		current := heap.Pop(rem).(*pathScore)
		if current.level > first.level+1 {
			path := g.pathBuf[:0]
			for current.parent != nil {
				path = append(path, stealSegment{
					current.node,
					current.parent.node,
					current.srcEdge,
				})
				current = current.parent
			}
			g.pathBuf = path
			return path, true
		}

		current.done = true

		for _, topicNum := range g.out[current.node] {
			info := g.b.topicInfos[topicNum]
			firstPartNum, lastPartNum := info.partNum, info.partNum+uint32(info.partitions)
			for edge := firstPartNum; edge < lastPartNum; edge++ {
				neighborNode := g.cxns[edge]
				neighbor, isNew := g.getScore(neighborNode)
				if neighbor.done {
					continue
				}

				gscore := current.gscore + 1
				// If our neghbor gscore is less or equal, then we can
				// reach the neighbor through a previous route we have
				// tried and should not try again.
				if gscore < neighbor.gscore {
					neighbor.parent = current
					neighbor.srcEdge = edge
					neighbor.gscore = gscore
					neighbor.fscore = gscore + h(first, neighbor)
					if isNew {
						heap.Push(rem, neighbor)
					}
					// We never need to fix the heap position.
					// Our level and fscore is static, and once
					// we set gscore, it is the minumum it will be
					// and we never revisit this neighbor.
				}
			}
		}
	}

	return nil, false
}

type stealSegment struct {
	src  uint16 // member num
	dst  uint16 // member num
	part uint32 // partNum
}

type pathScore struct {
	done    bool
	node    uint16 // member num
	parent  *pathScore
	srcEdge uint32 // partNum
	level   int32  // partitions owned on this segment
	gscore  int32  // how many steals it would take to get here
	fscore  int32
}

type pathScores []pathScore

const infinityScore = 1<<31 - 1
const noScore = -1

// For A*, if we never overestimate (with h), then the path we find is
// optimal. A true estimation of our distance to any node is the node's
// level minus ours.
//
// At worst, our target must be +2 levels from us. So, our estimation
// any node can be our level, +2, minus theirs.
func h(first, target *pathScore) int32 {
	r := first.level + 2 - target.level
	if r < 0 {
		return 0
	}
	return r
}

func (g *graph) getScore(node uint16) (*pathScore, bool) {
	r := &g.scores[node]
	exists := r.gscore != noScore
	if !exists {
		*r = pathScore{
			node:   node,
			level:  int32(len(g.b.plan[node])),
			gscore: infinityScore,
			fscore: infinityScore,
		}
	}
	return r, !exists
}

type pathHeap []*pathScore

func (p *pathHeap) Len() int { return len(*p) }
func (p *pathHeap) Swap(i, j int) {
	h := *p
	h[i], h[j] = h[j], h[i]
}

func (p *pathHeap) Less(i, j int) bool {
	l, r := (*p)[i], (*p)[j]
	return l.level > r.level || l.level == r.level &&
		(l.fscore < r.fscore || l.fscore == r.fscore &&
			(l.gscore < r.gscore || l.gscore == r.gscore &&
				l.node < r.node))
}

func (p *pathHeap) Push(x interface{}) { *p = append(*p, x.(*pathScore)) }
func (p *pathHeap) Pop() interface{} {
	h := *p
	l := len(h)
	r := h[l-1]
	*p = h[:l-1]
	return r
}
