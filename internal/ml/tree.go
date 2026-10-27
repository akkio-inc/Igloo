package ml

import (
	"runtime"
	"sync"
	"sync/atomic"
)

// Node is a node of a tree. A leaf has Feature -1 and its Value; a split
// sends a row left when its feature's bin is in Left (categorical) or in
// 1..Threshold (numeric, with a missing value going left when MissingLeft).
type Node struct {
	Feature     int32   `json:"f"`
	Threshold   uint8   `json:"t,omitempty"`
	MissingLeft bool    `json:"m,omitempty"`
	Cats        []uint8 `json:"c,omitempty"` // a categorical split's bins that go left, sorted
	LeftChild   int32   `json:"l,omitempty"`
	RightChild  int32   `json:"r,omitempty"`
	Value       float64 `json:"v,omitempty"`
	// Cover is the number of training rows that reached the node, Gain
	// what its split gained.
	Cover float64 `json:"n"`
	Gain  float64 `json:"g,omitempty"`

	left *[4]uint64
}

// IsLeaf reports whether the node is a leaf.
func (n *Node) IsLeaf() bool { return n.Feature < 0 }

// prepare readies a node for prediction.
func (n *Node) prepare() {
	if len(n.Cats) > 0 && n.left == nil {
		var set [4]uint64
		for _, c := range n.Cats {
			set[c>>6] |= 1 << (c & 63)
		}
		n.left = &set
	}
}

// goesLeft reports whether a row whose bin of the node's feature is bin
// goes to the node's left child.
func (n *Node) goesLeft(bin uint8) bool {
	if n.left != nil {
		return n.left[bin>>6]&(1<<(bin&63)) != 0
	}
	if bin == missingBin {
		return n.MissingLeft
	}
	return bin <= n.Threshold
}

// Tree is a regression tree; Nodes[0] is its root.
type Tree struct {
	Nodes []Node `json:"nodes"`
	// Class is the class whose score the tree adds to, for multiclass
	// models; 0 otherwise.
	Class int `json:"class,omitempty"`

	maxDepth int
}

// leafOf returns the index of the leaf a row reaches, given its bins.
func (t *Tree) leafOf(row []uint8) int {
	i := 0
	for {
		n := &t.Nodes[i]
		if n.IsLeaf() {
			return i
		}
		if n.goesLeft(row[n.Feature]) {
			i = int(n.LeftChild)
		} else {
			i = int(n.RightChild)
		}
	}
}

// predict returns the tree's value for a row of bins.
func (t *Tree) predict(row []uint8) float64 { return t.Nodes[t.leafOf(row)].Value }

// leafOfCol is leafOf for the row at r of binned columns.
func (t *Tree) leafOfCol(b *binned, r int) int {
	i := 0
	for {
		n := &t.Nodes[i]
		if n.IsLeaf() {
			return i
		}
		if n.goesLeft(b.bins[n.Feature][r]) {
			i = int(n.LeftChild)
		} else {
			i = int(n.RightChild)
		}
	}
}

// depth returns the tree's depth, a lone leaf's being 0.
func (t *Tree) depth() int {
	var walk func(i int) int
	walk = func(i int) int {
		n := &t.Nodes[i]
		if n.IsLeaf() {
			return 0
		}
		return 1 + max(walk(int(n.LeftChild)), walk(int(n.RightChild)))
	}
	return walk(0)
}

// meanValue is the tree's value averaged over training rows, by cover.
func (t *Tree) meanValue() float64 {
	var sum, cover float64
	for i := range t.Nodes {
		if n := &t.Nodes[i]; n.IsLeaf() {
			sum += n.Value * n.Cover
			cover += n.Cover
		}
	}
	if cover == 0 {
		return 0
	}
	return sum / cover
}

func (t *Tree) prepare() {
	for i := range t.Nodes {
		t.Nodes[i].prepare()
	}
	t.maxDepth = t.depth()
}

// parallelFor calls fn(i) for each i in [0, n) on up to threads goroutines
// (0 means one per CPU Go may use). Each i is handled once; what fn
// writes for i alone is deterministic whatever the threads.
func parallelFor(n, threads int, fn func(i int)) {
	if threads <= 0 {
		threads = runtime.GOMAXPROCS(0)
	}
	threads = min(threads, n)
	if threads <= 1 {
		for i := range n {
			fn(i)
		}
		return
	}
	var next atomic.Int64
	var wg sync.WaitGroup
	for range threads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1) - 1)
				if i >= n {
					return
				}
				fn(i)
			}
		}()
	}
	wg.Wait()
}

// chunks splits [0, n) into ranges of a fixed size, so that sums over
// ranges then added in order do not depend on the number of threads.
const chunkRows = 16384

func numChunks(n int) int { return (n + chunkRows - 1) / chunkRows }

func chunkRange(c, n int) (int, int) { return c * chunkRows, min((c+1)*chunkRows, n) }
