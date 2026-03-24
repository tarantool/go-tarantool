package box

import (
	"context"

	"github.com/tarantool/go-tarantool/v3"
)

// Session struct represents a connection session to Tarantool.
type Session struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// newSession creates a new Session instance, taking a Tarantool connection as an argument.
func newSession(conn tarantool.Doer) *Session {
	return &Session{conn: conn} // Pass the connection to the Session structure.
}

// Session method returns a new Session object associated with the Box instance.
func (b *Box) Session() *Session {
	return newSession(b.conn)
}

// SessionSuRequest struct wraps a Tarantool call request specifically for session switching.
type SessionSuRequest struct {
	baseCallRequest
}

// NewSessionSuRequest creates a new SessionSuRequest for switching session to a specified username.
// It returns an error if any execute functions are provided, as they are not supported now.
func NewSessionSuRequest(username string) (SessionSuRequest, error) {
	args := []any{username} // Create args slice with the username.

	return SessionSuRequest{
		baseCallRequest: baseCallRequest{
			call: tarantool.NewCallRequest("box.session.su").Args(args),
		},
	}, nil
}

// Context sets a passed context to the request.
func (req SessionSuRequest) Context(ctx context.Context) SessionSuRequest {
	req.call = req.call.Context(ctx)
	return req
}

// Su method is used to switch the session to the specified username.
// It sends the request to Tarantool and returns an error.
func (s *Session) Su(ctx context.Context, username string) error {
	// Create a request and send it to Tarantool.
	req, err := NewSessionSuRequest(username)
	if err != nil {
		return err // Return any errors encountered while creating the request.
	}

	req = req.Context(ctx) // Attach the context to the request.

	// Execute the request and return the future response, or an error.
	fut := s.conn.Do(req)
	_, err = fut.GetResponse()
	return err
}
