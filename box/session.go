package box

import (
	"context"
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
)

// Session struct represents a connection session to Tarantool.
type Session struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// NewSession creates a new Session instance, taking a Tarantool connection as an argument.
func NewSession(conn tarantool.Doer) *Session {
	return &Session{conn: conn} // Pass the connection to the Session structure.
}

// Session method returns a new Session object associated with the Box instance.
func (b *Box) Session() *Session {
	return NewSession(b.conn)
}

// SessionSuRequest struct wraps a Tarantool call request specifically for session switching.
type SessionSuRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewSessionSuRequest creates a new SessionSuRequest for switching session to a specified username.
// It returns an error if any execute functions are provided, as they are not supported now.
func NewSessionSuRequest(username string, execute ...any) (SessionSuRequest, error) {
	args := []interface{}{username} // Create args slice with the username.

	// Check if any execute functions were provided and return an error if so.
	if len(execute) > 0 {
		return SessionSuRequest{},
			fmt.Errorf("user functions call inside su command is unsupported now," +
				" because Tarantool needs functions signature instead of name")
	}

	// Create a new call request for the box.session.su method with the given args.
	callReq := tarantool.NewCallRequest("box.session.su").Args(args)

	return SessionSuRequest{
		callReq, // Return the new SessionSuRequest containing the call request.
	}, nil
}

// Su method is used to switch the session to the specified username.
// It sends the request to Tarantool and returns a future response or an error.
func (s *Session) Su(ctx context.Context, username string,
	execute ...any) (*tarantool.Future, error) {
	// Create a request and send it to Tarantool.
	req, err := NewSessionSuRequest(username, execute...)
	if err != nil {
		return nil, err // Return any errors encountered while creating the request.
	}

	req.Context(ctx) // Attach the context to the request for cancellation and timeout.

	// Execute the request and return the future response, or an error.
	return s.conn.Do(req), nil
}
