package ringbuf

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRingBuffer_IsEmpty(t *testing.T) {
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
