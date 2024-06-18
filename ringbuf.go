package ringbuf

import (
	"errors"
	"runtime"
	"sync/atomic"
)

const (
	Disposed uint64 = 1
)

var (
	ErrClosed = errors.New(`ring buffer: closed`)
	ErrEmpty  = errors.New(`ring buffer: empty`)
	ErrFull   = errors.New(`ring buffer: full`)
)

// roundUp takes a uint64 greater than 0 and rounds it up to the next
// power of 2.
func roundUp(v uint64) uint64 {
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v |= v >> 32
	v++
	return v
}

type node struct {
	position uint64
	data     interface{}
}

type nodes []node

// RingBuffer is a MPMC buffer that achieves threadsafety with CAS operations
// only.  A put on full or get on empty call will block until an item
// is put or retrieved.  Calling Dispose on the RingBuffer will unblock
// any blocked threads with an error.  This buffer is similar to the buffer
// described here: http://www.1024cores.net/home/lock-free-algorithms/queues/bounded-mpmc-queue
// with some minor additions.
type RingBuffer struct {
	_padding0 [8]uint64
	queue     uint64
	_padding1 [8]uint64
	dequeue   uint64
	_padding2 [8]uint64
	closed    uint64
	_padding3 [8]uint64
	mask      uint64
	nodes     nodes
}

// NewRingBuffer will allocate, initialize, and return a ring buffer
// with the specified size.
func NewRingBuffer(size uint64) *RingBuffer {
	size = roundUp(size)
	rb := &RingBuffer{
		nodes: make(nodes, size),
		mask:  size - 1,
	}
	for i := uint64(0); i < size; i++ {
		rb.nodes[i] = node{position: i}
	}
	return rb
}

func (rb *RingBuffer) Put(item interface{}) error {
	var n *node

	for {
		// * Check if buffer is closed
		if atomic.LoadUint64(&rb.closed) == 1 {
			return ErrClosed
		}
		// * Check if buffer is full
		q := atomic.LoadUint64(&rb.queue)
		deq := atomic.LoadUint64(&rb.dequeue)
		if (q+1)&rb.mask == deq&rb.mask {
			return ErrFull
		}

		n = &rb.nodes[q&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - q; {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.queue, q, q+1) {
				n.data = item
				atomic.StoreUint64(&n.position, q+1)
				return nil
			}
		case dif < 0:
			panic(`Ring buffer in a compromised state during a put operation.`)
		default:
			q = atomic.LoadUint64(&rb.queue)
		}
		runtime.Gosched()
	}
}

func (rb *RingBuffer) Get() (interface{}, error) {
	var n *node

	for {
		// * Check if buffer is closed
		if atomic.LoadUint64(&rb.closed) == 1 {
			return nil, ErrClosed
		}

		// * Check if buffer is empty
		q := atomic.LoadUint64(&rb.queue)
		deq := atomic.LoadUint64(&rb.dequeue)
		if q&rb.mask == deq&rb.mask {
			return nil, ErrEmpty
		}

		n = &rb.nodes[deq&rb.mask]
		seq := atomic.LoadUint64(&n.position)
		switch dif := seq - (deq + 1); {
		case dif == 0:
			if atomic.CompareAndSwapUint64(&rb.dequeue, deq, deq+1) {
				data := n.data
				n.data = nil
				atomic.StoreUint64(&n.position, deq+rb.mask+1)
				return data, nil
			}
		case dif < 0:
			panic(`Ring buffer in compromised state during a get operation.`)
		default:
			deq = atomic.LoadUint64(&rb.dequeue)
		}
		runtime.Gosched()
	}
}

//func (rb *RingBuffer) Len() uint64 {
//	return atomic.LoadUint64(&rb.queue) - atomic.LoadUint64(&rb.dequeue)
//}

func (rb *RingBuffer) IsEmpty() bool {
	q := atomic.LoadUint64(&rb.queue)
	deq := atomic.LoadUint64(&rb.dequeue)
	return q&rb.mask == deq&rb.mask
}

func (rb *RingBuffer) IsFull() bool {
	q := atomic.LoadUint64(&rb.queue)
	deq := atomic.LoadUint64(&rb.dequeue)
	return (q+1)&rb.mask == deq&rb.mask
}

func (rb *RingBuffer) Len() uint64 {
	readIdx := atomic.LoadUint64(&rb.dequeue) & rb.mask
	writeIdx := atomic.LoadUint64(&rb.queue) & rb.mask

	if readIdx > writeIdx {
		return rb.mask + 1 + writeIdx - readIdx
	}
	if writeIdx > readIdx {
		return writeIdx - readIdx
	}
	return 0
}

// Cap returns the capacity of this ring buffer.
func (rb *RingBuffer) Cap() uint64 {
	return uint64(len(rb.nodes))
}

func (rb *RingBuffer) Close() {
	atomic.CompareAndSwapUint64(&rb.closed, 0, Disposed)
}

func (rb *RingBuffer) IsClosed() bool {
	return atomic.LoadUint64(&rb.closed) == Disposed
}
