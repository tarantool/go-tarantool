package tarantool

// SetStreamIdForTesting overrides a Stream's identifier for tests that need
// to exercise wire-encoding of arbitrary stream id values.
func SetStreamIdForTesting(s *Stream, id uint64) {
	s.id = id
}
