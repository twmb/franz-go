package sticky

import "container/heap"

// Graph maps members to partitions they want to steal.
//
// The representation was chosen so as to avoid updating all members on any
// partition move; move updates are one map update.
type graph struct {
	// node => edges out
	// "from a node, which partitions could we steal?"
	out []memberPartitions

	// reference to balancer plan for determining node levels
	plan membersPartitions

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

func newGraph(
	plan membersPartitions,
	partitionConsumers []uint16,
	partitionPotentials [][]uint16,
) graph {
	g := graph{
		out:     make([]memberPartitions, len(plan)),
		plan:    plan,
		cxns:    partitionConsumers,
		scores:  make([]pathScore, len(plan)),
		heapBuf: make([]*pathScore, len(plan)/2),
	}
	memberPartsBufs := make([]uint32, len(plan)*len(partitionConsumers))
	for memberNum := range plan {
		memberPartsBuf := memberPartsBufs[:0:len(partitionConsumers)]
		memberPartsBufs = memberPartsBufs[len(partitionConsumers):]
		// In the worst case, if every node is linked to each other,
		// each node will have nparts edges. We preallocate the worst
		// case. It is common for the graph to be highly connected.
		g.out[memberNum] = memberPartsBuf
	}
	for partNum, potentials := range partitionPotentials {
		for _, potential := range potentials {
			g.out[potential].add(uint32(partNum))
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
	}

	first, _ := g.getScore(from)

	// For A*, if we never overestimate (with h), then the path we find is
	// optimal. A true estimation of our distance to any node is the node's
	// level minus ours. However, we do not actually know what we want to
	// steal; we do not know what we are searching for.
	//
	// If we have a neighbor 10 levels up, it makes more sense to steal
	// from that neighbor than one 5 levels up.
	//
	// At worst, our target must be +2 levels from us. So, our estimation
	// any node can be our level, +2, minus theirs. This allows neighbor
	// nodes that _are_ 10 levels higher to flood out any bad path and to
	// jump to the top of the priority queue. If there is no high level
	// to steal from, our estimator works normally.
	h := func(p *pathScore) int64 { return int64(first.level + 2 - p.level) }

	first.gscore = 0
	first.fscore = h(first)
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

		for _, edge := range g.out[current.node] { // O(P) worst case, should be less
			neighborNode := g.cxns[edge]
			neighbor, isNew := g.getScore(neighborNode)
			if neighbor.done {
				continue
			}

			gscore := current.gscore + 1
			if gscore < neighbor.gscore {
				neighbor.parent = current
				neighbor.srcEdge = edge
				neighbor.gscore = gscore
				neighbor.fscore = gscore + h(neighbor)
				if isNew {
					heap.Push(rem, neighbor)
				}
				// We never need to fix the heap position; it is
				// not possible for us to find a lower score.
			}
		}
	}

	return nil, false
}

type stealSegment struct {
	src  uint16 // member num
	dst  uint16 // member num
	part uint32 // topicPartition; partNum
}

type pathScore struct {
	node    uint16 // member num
	parent  *pathScore
	srcEdge uint32 // topicPartition; partNum
	level   int
	gscore  int64
	fscore  int64
	done    bool
}

type pathScores []pathScore

const infinityScore = 1 << 31
const noScore = 1<<31 - 1

func (g *graph) getScore(node uint16) (*pathScore, bool) {
	r := &g.scores[node]
	exists := r.gscore != noScore
	if !exists {
		*r = pathScore{
			node:    node,
			parent:  nil,
			srcEdge: 0,
			level:   len(g.plan[node]),
			gscore:  infinityScore,
			fscore:  infinityScore,
			done:    false,
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
	return l.fscore < r.fscore ||
		l.fscore == r.fscore &&
			l.node < r.node
}

func (p *pathHeap) Push(x interface{}) { *p = append(*p, x.(*pathScore)) }
func (p *pathHeap) Pop() interface{} {
	h := *p
	l := len(h)
	r := h[l-1]
	*p = h[:l-1]
	return r
}
