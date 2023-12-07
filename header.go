package tarantool

// Header is a response header.
type Header struct {
	// RequestId is an id of a corresponding request.
	RequestId uint32
	// Code is a response code. It could be used to check that response
	// has or hasn't an error.
	Code uint32
}
