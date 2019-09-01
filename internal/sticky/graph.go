package sticky

import "container/heap"

type graph struct {
	// node => edges out
	// "from a node, which partitions could we steal?"
	out map[string]map[topicPartition]struct{}

	// edge => who has this edge & dst
	cxns map[topicPartition]*nodeDst

	node2lvl map[string]int
	lvl2node map[int]map[string]struct{}
}

type nodeDst struct {
	dst string
	ins map[string]struct{}
}

func newGraph() graph {
	return graph{
		out:      make(map[string]map[topicPartition]struct{}),
		cxns:     make(map[topicPartition]*nodeDst),
		node2lvl: make(map[string]int),
		lvl2node: make(map[int]map[string]struct{}),
	}
}

func (g *graph) add(node string, partitions memberPartitions) {
	g.out[node] = make(map[topicPartition]struct{})
	g.node2lvl[node] = len(partitions)

	lvlNodes := g.lvl2node[len(partitions)]
	if lvlNodes == nil {
		lvlNodes = make(map[string]struct{})
		g.lvl2node[len(partitions)] = lvlNodes
	}
	lvlNodes[node] = struct{}{}

	for partition := range partitions {
		g.cxns[partition] = &nodeDst{
			dst: node,
			ins: make(map[string]struct{}),
		}
	}
}

func (g graph) link(src string, edge topicPartition) {
	g.out[src][edge] = struct{}{}
	g.cxns[edge].ins[src] = struct{}{}
}

func (g graph) changeOwnership(oldDst, newDst string, edge topicPartition) {
	oldLvl := g.node2lvl[oldDst]
	g.node2lvl[oldDst] = oldLvl - 1

	delete(g.lvl2node[oldLvl], oldDst)
	g.lvl2node[oldLvl-1][oldDst] = struct{}{}

	newLvl := g.node2lvl[newDst]
	g.node2lvl[newDst] = newLvl + 1

	delete(g.lvl2node[newLvl], newDst)
	g.lvl2node[newLvl+1][newDst] = struct{}{}

	g.cxns[edge].dst = newDst
}

// findSteal uses A* search to find a path from a source node to the first node
// it can reach two levels up.
func (g graph) findSteal(from string) ([]topicPartition, bool) {
	done := make(map[string]struct{}, len(g.node2lvl))

	scores := make(pathScores, len(g.node2lvl))
	first, _ := scores.get(from, g.node2lvl)
	h := func(p *pathScore) uint { return p.level + 2 - first.level }

	first.gscore = 0
	first.fscore = h(first)

	rem := &pathHeap{first}
	for rem.Len() > 0 {
		current := heap.Pop(rem).(*pathScore)
		if current.level > first.level+1 {
			var path []topicPartition
			for current.parent != nil {
				path = append(path, current.srcEdge)
				current = current.parent
			}
			return path, true
		}

		done[current.node] = struct{}{}

		for edge := range g.out[current.node] { // O(P) worst case, should be less
			neighborNode := g.cxns[edge].dst
			if _, isDone := done[neighborNode]; isDone {
				continue
			}

			gscore := current.gscore + 1
			neighbor, isNew := scores.get(neighborNode, g.node2lvl)
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

type pathScore struct {
	node    string
	parent  *pathScore
	srcEdge topicPartition
	level   uint
	gscore  uint
	fscore  uint
}

type pathScores map[string]*pathScore

func (p pathScores) get(node string, node2lvl map[string]int) (*pathScore, bool) {
	r, exists := p[node]
	if !exists {
		r = &pathScore{
			node:  node,
			level: uint(node2lvl[node]),
		}
		r.gscore--
		r.fscore--
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
