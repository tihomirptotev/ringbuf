package ringbuf

import (
	"errors"
	"github.com/Workiva/go-datastructures/queue"
	"github.com/stretchr/testify/assert"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func TestRingBuffer_IsEmpty(t *testing.T) {
	t.Log(unsafe.Sizeof(node{}))
	tests := []struct {
		name     string
		setup    func(*RingBuffer)
		expected bool
	}{
		{
			name:     "Empty ring buffer",
			setup:    func(rb *RingBuffer) {},
			expected: true,
		},
		{
			name: "Non-empty ring buffer",
			setup: func(rb *RingBuffer) {
				assert.NoError(t, rb.Put(1))
			},
			expected: false,
		},
		{
			name: "Empty ring buffer after put and get",
			setup: func(rb *RingBuffer) {
				assert.NoError(t, rb.Put(1))
				_, err := rb.Get()
				assert.NoError(t, err)
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Given
			rb := NewRingBuffer(8)
			tt.setup(rb)

			// When
			isEmpty := rb.IsEmpty()

			// Then
			if isEmpty != tt.expected {
				t.Errorf("Expected %t, got %t", tt.expected, isEmpty)
			}
		})
	}
}

func TestRingBuffer_IsFull(t *testing.T) {
	testCases := []struct {
		name   string
		setup  func(buffer *RingBuffer)
		expect bool
	}{
		{
			name: "BufferInit",
			setup: func(buffer *RingBuffer) {
			},
			expect: false,
		},
		{
			name: "BufferFilled",
			setup: func(buffer *RingBuffer) {
				// Put enough items to fill the buffer
				for i := uint64(0); i < buffer.Cap()-1; i++ {
					assert.NoError(t, buffer.Put(1))
				}
			},
			expect: true,
		},
		{
			name: "AfterBufferEmptied",
			setup: func(buffer *RingBuffer) {
				// Fill and then empty the buffer
				for i := uint64(0); i < buffer.Cap()-1; i++ {
					assert.NoError(t, buffer.Put(1))
				}
				for i := uint64(0); i < buffer.Cap()-1; i++ {
					_, err := buffer.Get()
					assert.NoError(t, err)
				}
			},
			expect: false,
		},
	}

	// Running test cases
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			buffer := NewRingBuffer(8)
			tt.setup(buffer)
			if buffer.IsFull() != tt.expect {
				t.Errorf("IsFull test Failed. Expected %v Got %v", tt.expect, buffer.IsFull())
			}
		})
	}
}

func TestRingBuffer_Size(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *RingBuffer
		putItems    []interface{}
		expectedLen uint64
	}{
		{
			name: "empty ring buffer",
			setup: func() *RingBuffer {
				rb := NewRingBuffer(4)
				return rb
			},
			expectedLen: 0,
		},
		{
			name: "non-empty ring buffer",
			setup: func() *RingBuffer {
				rb := NewRingBuffer(4)
				assert.NoError(t, rb.Put(1))
				assert.NoError(t, rb.Put(2))
				return rb
			},
			expectedLen: 2,
		},
		{
			name: "full ring buffer",
			setup: func() *RingBuffer {
				rb := NewRingBuffer(4)
				assert.NoError(t, rb.Put(1))
				assert.NoError(t, rb.Put(2))
				assert.NoError(t, rb.Put(3))
				assert.ErrorIs(t, rb.Put(4), ErrFull)
				return rb
			},
			expectedLen: 3,
		},
		{
			name: "full/empty ring buffer",
			setup: func() *RingBuffer {
				rb := NewRingBuffer(4)
				assert.NoError(t, rb.Put(1))
				assert.NoError(t, rb.Put(2))
				assert.NoError(t, rb.Put(3))
				for range 3 {
					_, err := rb.Get()
					assert.NoError(t, err)
				}

				_, err := rb.Get()
				assert.ErrorIs(t, err, ErrEmpty)

				return rb
			},
			expectedLen: 0,
		},
		{
			name: "overflow ring buffer",
			setup: func() *RingBuffer {
				rb := NewRingBuffer(4)
				assert.NoError(t, rb.Put(1))
				assert.NoError(t, rb.Put(2))
				assert.NoError(t, rb.Put(3))
				for range 2 {
					_, err := rb.Get()
					assert.NoError(t, err)
				}
				assert.NoError(t, rb.Put(4))
				assert.NoError(t, rb.Put(5))
				assert.ErrorIs(t, rb.Put(6), ErrFull)

				return rb
			},
			expectedLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := tt.setup()
			size := rb.Len()
			if size != tt.expectedLen {
				t.Errorf("unexpected size. want: %v, got: %v", tt.expectedLen, size)
			}
		})
	}
}

// Adapted tests from workiva ring buffer
func TestPut(t *testing.T) {

	t.Run("single put", func(t *testing.T) {
		rb := NewRingBuffer(5)
		assert.Equal(t, uint64(8), rb.Cap())

		err := rb.Put(5)
		assert.NoError(t, err)

		result, err := rb.Get()
		assert.NoError(t, err)
		assert.Equal(t, 5, result)
	})

	t.Run("multiple put", func(t *testing.T) {
		rb := NewRingBuffer(5)
		assert.Equal(t, uint64(8), rb.Cap())

		err := rb.Put(1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), rb.Len())

		err = rb.Put(2)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), rb.Len())

		result, err := rb.Get()
		assert.NoError(t, err)
		assert.Equal(t, 1, result)
		assert.Equal(t, uint64(1), rb.Len())

		result, err = rb.Get()
		assert.NoError(t, err)
		assert.Equal(t, 2, result)
		assert.Equal(t, uint64(0), rb.Len())
	})

	t.Run("buffer full", func(t *testing.T) {
		rb := NewRingBuffer(5)
		assert.Equal(t, uint64(8), rb.Cap())

		for i := range rb.Cap() - 1 {
			err := rb.Put(i)
			assert.NoError(t, err)
			assert.Equal(t, uint64(i+1), rb.Len())
		}

		err := rb.Put(7)
		assert.ErrorIs(t, err, ErrFull)
	})

	t.Run("try to put", func(t *testing.T) {
		rb := NewRingBuffer(2)
		assert.Equal(t, uint64(2), rb.Cap())

		err := rb.Put("foo")
		assert.NoError(t, err)

		err = rb.Put("bar")
		assert.ErrorIs(t, err, ErrFull)

		item, err := rb.Get()
		assert.NoError(t, err)
		assert.Equal(t, "foo", item)

		assert.Equal(t, uint64(0), rb.Len())
		assert.Equal(t, uint64(2), rb.Cap())

		rb.Close()
		assert.Equal(t, true, rb.IsClosed())

		err = rb.Put("foo")
		assert.ErrorIs(t, err, ErrClosed)

		_, err = rb.Get()
		assert.ErrorIs(t, err, ErrClosed)
	})

}

func TestIntertwinedGetAndPut(t *testing.T) {
	rb := NewRingBuffer(5)
	err := rb.Put(1)
	assert.NoError(t, err)

	result, err := rb.Get()
	assert.NoError(t, err)
	assert.Equal(t, 1, result)

	err = rb.Put(2)
	assert.NoError(t, err)

	result, err = rb.Get()
	assert.NoError(t, err)
	assert.Equal(t, 2, result)
}

func TestPutToFull(t *testing.T) {
	rb := NewRingBuffer(5)

	for i := range int(rb.Cap() - 1) {
		err := rb.Put(i)
		assert.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		err := rb.Put(7)
		assert.ErrorIs(t, err, ErrFull)

		time.Sleep(time.Millisecond * 2)

		err = rb.Put(7)
		assert.NoError(t, err)
		wg.Done()
	}()

	go func() {
		defer wg.Done()
		time.Sleep(time.Millisecond)
		result, err := rb.Get()
		assert.NoError(t, err)
		assert.Equal(t, 0, result)
	}()

	wg.Wait()
}

func TestGet(t *testing.T) {
	t.Run("empty buffer", func(t *testing.T) {
		rb := NewRingBuffer(5)
		_, err := rb.Get()
		assert.ErrorIs(t, err, ErrEmpty)
	})

}

func TestLen(t *testing.T) {
	rb := NewRingBuffer(4)
	assert.Equal(t, uint64(0), rb.Len())
	assert.Equal(t, uint64(4), rb.Cap())

	err := rb.Put(1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), rb.Len())

	_, err = rb.Get()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), rb.Len())

	for i := 0; i < int(rb.Cap()-1); i++ {
		err = rb.Put(i)
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(3), rb.Len())

	_, err = rb.Get()
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), rb.Len())
}

func TestClose(t *testing.T) {

	t.Run("on get", func(t *testing.T) {
		numThreads := 8
		var wg sync.WaitGroup
		wg.Add(numThreads)
		rb := NewRingBuffer(4)
		var spunUp sync.WaitGroup
		spunUp.Add(numThreads)

		for i := 0; i < numThreads; i++ {
			go func() {
				spunUp.Done()
				defer wg.Done()
				_, err := rb.Get()
				assert.NotNil(t, err)
			}()
		}

		spunUp.Wait()
		rb.Close()

		wg.Wait()
		assert.True(t, rb.IsClosed())
	})

	t.Run("on put", func(t *testing.T) {
		numThreads := 8
		var wg sync.WaitGroup
		wg.Add(numThreads)
		rb := NewRingBuffer(4)
		var spunUp sync.WaitGroup
		spunUp.Add(numThreads)

		// fill up the queue
		for i := 0; i < 3; i++ {
			err := rb.Put(i)
			assert.NoError(t, err)
		}

		// it's now full
		for i := 0; i < numThreads; i++ {
			go func(i int) {
				spunUp.Done()
				defer wg.Done()
				err := rb.Put(i)
				assert.NotNil(t, err)
			}(i)
		}

		spunUp.Wait()

		rb.Close()

		wg.Wait()

		assert.True(t, rb.IsClosed())
	})

}

func BenchmarkLifeCycle(b *testing.B) {
	rb := NewRingBuffer(1024)

	counter := uint64(0)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			_, err := rb.Get()
			if err != nil {
				assert.ErrorIs(b, err, ErrEmpty)
				continue
			}

			if atomic.AddUint64(&counter, 1) == uint64(b.N) {
				return
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; {
		err := rb.Put(i)
		if err != nil {
			assert.ErrorIs(b, err, ErrFull)
		} else {
			i++
		}
	}

	wg.Wait()
}

func BenchmarkWorkivaRingBufferLifeCycle(b *testing.B) {
	rb := queue.NewRingBuffer(1024)

	counter := uint64(0)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			_, err := rb.Get()
			assert.NoError(b, err)

			if atomic.AddUint64(&counter, 1) == uint64(b.N) {
				return
			}
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		err := rb.Put(i)
		assert.NoError(b, err)
	}

	wg.Wait()
}

//goland:noinspection t
func BenchmarkLifeCycleContention(b *testing.B) {
	rb := NewRingBuffer(64)

	var wwg sync.WaitGroup
	var rwg sync.WaitGroup
	wwg.Add(10)
	rwg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				_, err := rb.Get()
				if err != nil {
					if errors.Is(err, ErrClosed) {
						rwg.Done()
						return
					}
					assert.ErrorIs(b, err, ErrEmpty)
				}
			}
		}()
	}

	b.ResetTimer()

	for i := 0; i < 10; i++ {
		go func() {
			defer wwg.Done()
			for j := 0; j < b.N; j++ {
				err := rb.Put(j)
				if err != nil {
					if errors.Is(err, ErrClosed) {
						return
					}
					assert.ErrorIs(b, err, ErrFull)
				}
			}
		}()
	}

	wwg.Wait()
	rb.Close()
	rwg.Wait()
}

func BenchmarkWorkivaRingBufferLifeCycleContention(b *testing.B) {
	rb := queue.NewRingBuffer(64)

	var wwg sync.WaitGroup
	var rwg sync.WaitGroup
	wwg.Add(10)
	rwg.Add(10)

	for i := 0; i < 10; i++ {
		go func() {
			for {
				_, err := rb.Get()
				if errors.Is(err, queue.ErrDisposed) {
					rwg.Done()
					return
				} else {
					assert.Nil(b, err)
				}
			}
		}()
	}

	b.ResetTimer()

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < b.N; j++ {
				err := rb.Put(j)
				assert.NoError(b, err)
			}
			wwg.Done()
		}()
	}

	wwg.Wait()
	rb.Dispose()
	rwg.Wait()
}

func BenchmarkPut(b *testing.B) {
	rb := NewRingBuffer(uint64(b.N))
	var err error

	b.ResetTimer()

	for i := 0; i < b.N-1; i++ {
		err = rb.Put(i)
		assert.NoError(b, err)
	}
}

func BenchmarkWorkivaRingBufferPut(b *testing.B) {
	rb := queue.NewRingBuffer(uint64(b.N))
	var err error

	b.ResetTimer()

	for i := 0; i < b.N-1; i++ {
		err = rb.Put(i)
		assert.NoError(b, err)
	}
}

func BenchmarkGet(b *testing.B) {
	rb := NewRingBuffer(uint64(b.N))
	var err error

	for i := 0; i < b.N-1; i++ {
		err = rb.Put(i)
		assert.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N-1; i++ {
		_, err = rb.Get()
		assert.NoError(b, err)
	}
}

func BenchmarkWorkivaRingBufferGet(b *testing.B) {
	rb := queue.NewRingBuffer(uint64(b.N))
	var err error

	for i := 0; i < b.N-1; i++ {
		err = rb.Put(i)
		assert.NoError(b, err)
	}

	b.ResetTimer()

	for i := 0; i < b.N-1; i++ {
		_, err = rb.Get()
		assert.NoError(b, err)
	}
}

func BenchmarkAllocation(b *testing.B) {
	for i := 0; i < b.N; i++ {
		NewRingBuffer(1024)
	}
}
