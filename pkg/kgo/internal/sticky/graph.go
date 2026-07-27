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
	cxns []partitionConsumer

	// scores are all node scores from a search node. The distance field
	// is reset on findSteal to infinityScore..
	scores pathScores

	// topicOwners is, per topic, the members currently holding at least
	// one of its partitions. Expansion walks this rather than every
	// partition of the topic: a member holding a thousand partitions of
	// one topic is one edge to the search, not a thousand.
	//
	// Entries are emptied but never removed, so an owner's index is
	// stable for the life of a balance.
	topicOwners [][]ownerParts

	// partPos is each partition's index within whichever bucket of its
	// owner currently holds it, so moving a partition is O(1) instead of
	// a scan. Which bucket is always recomputable from cxns.
	partPos []int32

	// origMoved is, per member, partitions it started with that someone
	// else now holds -- the only way an edge can be worth +1 to steal.
	// Append only; readers re-check current ownership, so duplicates and
	// partitions that came home are harmless.
	origMoved [][]int32

	// heapBuf and pathBuf are backing buffers that are reused every
	// findSteal; note that pathBuf must be done being used before
	// the next find steal, but it always is.
	heapBuf pathHeap
	pathBuf []stealSegment
}

func (b *balancer) newGraph(
	partitionConsumers []partitionConsumer,
	topicPotentials [][]uint16,
) graph {
	g := graph{
		b:       b,
		out:     make([][]uint32, len(b.members)),
		cxns:    partitionConsumers,
		scores:  make([]pathScore, len(b.members)),
		heapBuf: make([]*pathScore, len(b.members)),
	}
	// Same sizing argument as topicPotentials: one topic slot per member per
	// topic is exact only when every member subscribes to everything.
	var nsubs int
	for _, potentials := range topicPotentials {
		nsubs += len(potentials)
	}
	perMember := nsubs/len(b.members) + 1
	outBufs := make([]uint32, perMember*len(b.members))
	for memberNum := range b.plan {
		out := outBufs[:0:perMember]
		outBufs = outBufs[perMember:]
		// Out edges are per topic, not per partition: in the worst
		// case a member subscribes to every topic, so we preallocate
		// one topic slot per member. The partition edges themselves
		// are enumerated from topicInfos during findSteal.
		g.out[memberNum] = out
	}
	for topicNum, potentials := range topicPotentials {
		for _, potential := range potentials {
			g.out[potential] = append(g.out[potential], uint32(topicNum))
		}
	}

	g.topicOwners = make([][]ownerParts, len(b.topicInfos))
	g.partPos = make([]int32, len(partitionConsumers))
	g.origMoved = make([][]int32, len(b.members))
	for edge, cxn := range partitionConsumers {
		if cxn.memberNum == unassignedPart {
			continue
		}
		g.addEdge(int32(edge), cxn.memberNum)
	}
	return g
}

// ownerParts is one member's holdings of one topic, split by what taking a
// partition from them would cost: free means they did not start with it, so
// taking it loses no stickiness.
type ownerParts struct {
	member uint16
	free   []int32
	owned  []int32
}

func (g *graph) bucket(o *ownerParts, edge int32) *[]int32 {
	if g.cxns[edge].originalNum == o.member {
		return &o.owned
	}
	return &o.free
}

func (g *graph) ownerOf(topicNum uint32, member uint16) *ownerParts {
	owners := g.topicOwners[topicNum]
	for i := range owners {
		if owners[i].member == member {
			return &g.topicOwners[topicNum][i]
		}
	}
	g.topicOwners[topicNum] = append(owners, ownerParts{member: member})
	return &g.topicOwners[topicNum][len(g.topicOwners[topicNum])-1]
}

func (g *graph) addEdge(edge int32, member uint16) {
	o := g.ownerOf(g.b.partOwners[edge], member)
	b := g.bucket(o, edge)
	g.partPos[edge] = int32(len(*b))
	*b = append(*b, edge)
}

func (g *graph) removeEdge(edge int32, member uint16) {
	o := g.ownerOf(g.b.partOwners[edge], member)
	b := g.bucket(o, edge)
	s := *b
	i, last := g.partPos[edge], int32(len(s)-1)
	s[i] = s[last]
	g.partPos[s[i]] = i
	*b = s[:last]
}

func (g *graph) changeOwnership(edge int32, newDst uint16) {
	old := g.cxns[edge].memberNum
	// The bucket an edge lives in depends on its current owner, so it must
	// come out of the index before cxns changes and go back in after.
	g.removeEdge(edge, old)
	if g.cxns[edge].originalNum == old {
		g.origMoved[old] = append(g.origMoved[old], edge)
	}
	g.cxns[edge].memberNum = newDst
	g.addEdge(edge, newDst)
}

// findSteal uses Dijkstra search to find a path from the best node it can reach.
func (g *graph) findSteal(from uint16) ([]stealSegment, bool) {
	// First, we must reset our scores from any prior run. This is O(M),
	// but is fast and faster than making a map and extending it a lot.
	for i := range g.scores {
		g.scores[i].distance = infinityScore
		g.scores[i].done = false
	}

	first, _ := g.getScore(from)

	first.distance = 0
	// Marking the origin done is not an optimization. Neighbor handling
	// below rewrites parent and srcEdge on any node it reaches that is not
	// done; if the origin were reachable it could be given a parent, and
	// the path walk back from a stealable node would then never terminate
	// at the origin.
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

		// One edge per member holding any of a topic we want, taking the
		// cheapest partition they have of it. Walking every partition
		// instead would rediscover the same handful of members over and
		// over: this is the difference between the search costing the
		// partition count and costing the member count.
		for _, topicNum := range g.out[current.node] {
			for i := range g.topicOwners[topicNum] {
				o := &g.topicOwners[topicNum][i]
				edge, score := int32(0), int8(0)
				switch {
				case len(o.free) > 0:
					edge = o.free[0]
				case len(o.owned) > 0:
					edge, score = o.owned[0], -1
				default:
					continue // emptied, but kept so indices stay stable
				}
				g.relax(rem, current, o.member, edge, score)
			}
		}

		// A partition we started with that someone else now holds is the
		// only edge worth +1, and there are at most as many of those as
		// there have been moves, so they are cheap to check directly.
		for _, edge := range g.origMoved[current.node] {
			cxn := g.cxns[edge]
			if cxn.originalNum != current.node || cxn.memberNum == current.node {
				continue // stale entry: it came home, or was never ours
			}
			g.relax(rem, current, cxn.memberNum, edge, 1)
		}
	}

	return nil, false
}

// relax offers current a way to reach neighborNode by stealing edge. The
// first way we find to reach a node is the shortest, since we pop in order;
// afterwards we only revise if the new edge is a better one to take.
func (g *graph) relax(rem *pathHeap, current *pathScore, neighborNode uint16, edge int32, score int8) {
	neighbor, isNew := g.getScore(neighborNode)
	if neighbor.done {
		return
	}
	if !isNew && score <= neighbor.srcScore {
		return
	}
	neighbor.parent = current
	neighbor.srcScore = score
	neighbor.srcEdge = edge
	neighbor.distance = current.distance + 1
	if isNew {
		neighbor.heapIdx = len(*rem)
		heap.Push(rem, neighbor)
		return
	}
	heap.Fix(rem, neighbor.heapIdx)
}

type stealSegment struct {
	src  uint16 // member num
	dst  uint16 // member num
	part int32  // partNum
}

// As we traverse a graph, we assign each node a path score, which tracks a few
// numbers for what it would take to reach this node from our first node.
type pathScore struct {
	// Done is set to true when we pop a node off of the graph. Once we
	// pop a node, it means we have found a best path to that node and
	// we do not want to revisit it for processing if any other future
	// nodes reach back to this one.
	//
	// Every path out of the pop loop must either return or set this,
	// because popping invalidates heapIdx: the node is no longer in the
	// heap, so a later heap.Fix on its stale index would reorder some
	// unrelated node. Today the only paths out are "return a steal" and
	// "mark done and expand", which is why the search is safe.
	done bool

	// srcScore is what stealing srcEdge does to stickiness: +1 if the
	// partition returns to the member taking it, -1 if it is taken away
	// from the member that started with it, 0 if neither.
	//
	// The +1 case works around a very slim edge case where a partition is
	// stolen by B and then needs to be stolen back by A later. The -1 case
	// is why we prefer, among the partitions we could take from a member,
	// one that member did not start with: taking that costs nothing.
	srcScore int8

	node     uint16 // our member num
	distance int32  // how many steals it would take to get here
	srcEdge  int32  // the partition used to reach us
	level    int32  // partitions owned on this segment
	parent   *pathScore
	heapIdx  int
}

func stealScore(cxn partitionConsumer, stealer uint16) int8 {
	switch cxn.originalNum {
	case stealer:
		return 1
	case cxn.memberNum:
		return -1
	}
	return 0
}

type pathScores []pathScore

const infinityScore = 1<<31 - 1

func (g *graph) getScore(node uint16) (*pathScore, bool) {
	r := &g.scores[node]
	exists := r.distance != infinityScore
	if !exists {
		*r = pathScore{
			node:     node,
			level:    int32(len(g.b.plan[node])),
			distance: infinityScore,
		}
	}
	return r, !exists
}

type pathHeap []*pathScore

func (p *pathHeap) Len() int { return len(*p) }
func (p *pathHeap) Swap(i, j int) {
	h := *p
	l, r := h[i], h[j]
	l.heapIdx, r.heapIdx = r.heapIdx, l.heapIdx
	h[i], h[j] = r, l
}

// For our path, we always want to prioritize stealing a partition we
// originally owned. This may result in a longer steal path, but it will
// increase stickiness.
//
// Next, our real goal, which is to find a node we can steal from. Because of
// this, we always want to sort by the highest level. The pathHeap stores
// reachable paths, so by sorting by the highest level, we terminate quicker:
// we always check the most likely candidates to quit our search.
//
// Finally, we simply prefer searching through shorter paths and, barring that,
// just sort by node.
func (p *pathHeap) Less(i, j int) bool {
	l, r := (*p)[i], (*p)[j]
	lo, ro := l.srcScore > 0, r.srcScore > 0
	return lo && !ro || !lo && !ro &&
		(l.level > r.level || l.level == r.level &&
			(l.distance < r.distance || l.distance == r.distance &&
				l.node < r.node))
}

func (p *pathHeap) Push(x any) { *p = append(*p, x.(*pathScore)) }
func (p *pathHeap) Pop() any {
	h := *p
	l := len(h)
	r := h[l-1]
	*p = h[:l-1]
	return r
}
