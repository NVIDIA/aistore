package dfc

import (
	"container/heap"
	"time"
)

// An FileObject is something we manage in a priority queue.
type FileObject struct {
	// path refers to local file DFC Node
	path string

	// atime refers to access time of file object.
	// It's being used as property for minHeap implementation.
	atime time.Time

	size int64

	// The index is needed by update and is maintained by the heap.Interface methods.
	index int
}

// A PriorityQueue implements heap.Interface and holds FileObjects.
type PriorityQueue []*FileObject

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	// Pop to return highest access time fileobject.(MaxHeap)
	return pq[i].atime.Sub(pq[j].atime) > 0
}

// Swap object from index I with with object from index J.
func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

// Push element into PriorityQueue.
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	fobj := x.(*FileObject)
	fobj.index = n
	*pq = append(*pq, fobj)
}

// Pop element from Priority Queue.
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	fobj := old[n-1]
	fobj.index = -1 // for safety
	*pq = old[0 : n-1]
	return fobj
}

// Update modifies the priority and value of an FileObject in the queue.
func (pq *PriorityQueue) Update(fobj *FileObject, value string, priority time.Time) {
	fobj.path = value
	fobj.atime = priority
	heap.Fix(pq, fobj.index)
}
