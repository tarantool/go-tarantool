package tarantool

// Allocator is an interface for allocating and deallocating byte slices.
type Allocator interface {
	// Get returns a pointer to a byte slice of at least the given length.
	// The caller should not assume anything about the slice's capacity.
	//
	// If the allocator cannot allocate a buffer (e.g., invalid length), it
	// returns nil. The caller must handle this case appropriately.
	Get(length int) *[]byte
	// Put returns a byte slice to the allocator for reuse.
	// After calling Put, the caller must not use the slice.
	//
	// The caller must ensure that the slice length remains unchanged between
	// Get and Put calls. Modifying the slice length before calling Put may
	// prevent the allocator from properly reusing the buffer.
	Put(buf *[]byte)
}
