package sticky

import "container/heap"

// Graph maps members to partitions they want to steal.
//
// The representation was chosen so as to avoid updating all members on any
// partition move; move updates are one map update.
type graph struct {
	// node => edges out
	// "from a node, which partitions could we steal?"
	out map[uint32]map[*topicPartition]struct{}

	// reference to balancer plan for determining node levels
	plan membersPartitions

	// edge => who owns this edge; built in balancer's assignUnassigned
	cxns map[*topicPartition]uint32
}

func newGraph(
	plan membersPartitions,
	partitionConsumers map[*topicPartition]uint32,
	partitionPotentials map[*topicPartition][]uint32,
) graph {
	g := graph{
		out:  make(map[uint32]map[*topicPartition]struct{}, len(plan)),
		plan: plan,
		cxns: partitionConsumers,
	}
	for member := range plan {
		// In the worst case, if every node is linked to each other,
		// each node will have nparts edges. We preallocate the worst
		// case. It is common for the graph to be highly connected.
		g.out[member] = make(map[*topicPartition]struct{}, len(partitionConsumers))
	}
	for partition, potentials := range partitionPotentials {
		for _, potential := range potentials {
			g.out[potential][partition] = struct{}{}
		}
	}
	return g
}

func (g graph) changeOwnership(edge *topicPartition, newDst uint32) {
	g.cxns[edge] = newDst
}

// findSteal uses A* search to find a path from the best node it can reach.
func (g graph) findSteal(from uint32) ([]stealSegment, bool) {
	done := make(map[uint32]struct{}, 10)

	scores := make(pathScores, 10)
	first, _ := scores.get(from, g.plan)

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
	h := func(p *pathScore) int { return first.level + 2 - p.level }

	first.gscore = 0
	first.fscore = h(first)
	done[first.node] = struct{}{}

	rem := &pathHeap{first}
	for rem.Len() > 0 {
		current := heap.Pop(rem).(*pathScore)
		if current.level > first.level+1 {
			var path []stealSegment
			for current.parent != nil {
				path = append(path, stealSegment{
					current.node,
					current.parent.node,
					current.srcEdge,
				})
				current = current.parent
			}
			return path, true
		}

		done[current.node] = struct{}{}

		for edge := range g.out[current.node] { // O(P) worst case, should be less
			neighborNode := g.cxns[edge]
			if _, isDone := done[neighborNode]; isDone {
				continue
			}

			gscore := current.gscore + 1
			neighbor, isNew := scores.get(neighborNode, g.plan)
			if gscore < neighbor.gscore {
				neighbor.parent = current
				neighbor.srcEdge = edge
				neighbor.gscore = gscore
				neighbor.fscore = gscore + h(neighbor)
				if isNew {
					heap.Push(rem, neighbor)
				}
			}
		}
	}

	return nil, false
}

type stealSegment struct {
	src  uint32
	dst  uint32
	part *topicPartition
}

type pathScore struct {
	node    uint32
	parent  *pathScore
	srcEdge *topicPartition
	level   int
	gscore  int
	fscore  int
}

type pathScores map[uint32]*pathScore

func (p pathScores) get(node uint32, plan membersPartitions) (*pathScore, bool) {
	r, exists := p[node]
	if !exists {
		r = &pathScore{
			node:   node,
			level:  len(plan[node]),
			gscore: 1 << 31,
			fscore: 1 << 31,
		}
		p[node] = r
	}
	return r, !exists
}

type pathHeap []*pathScore

func (p *pathHeap) Len() int      { return len(*p) }
func (p *pathHeap) Swap(i, j int) { (*p)[i], (*p)[j] = (*p)[j], (*p)[i] }

func (p *pathHeap) Less(i, j int) bool {
	l, r := (*p)[i], (*p)[j]
	return l.fscore < r.fscore ||
		l.fscore == r.fscore &&
			l.node < r.node
}

func (p *pathHeap) Push(x interface{}) { *p = append(*p, x.(*pathScore)) }
func (p *pathHeap) Pop() interface{} {
	l := len(*p)
	r := (*p)[l-1]
	*p = (*p)[:l-1]
	return r
}
