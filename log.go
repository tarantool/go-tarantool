package tarantool

const (
	// LogMsgReconnectFailed is a log message emitted when a reconnection
	// attempt fails.
	LogMsgReconnectFailed = "reconnect failed"
	// LogMsgLastReconnectFailed is a log message emitted when the last
	// allowed reconnection attempt fails and the connection gives up.
	LogMsgLastReconnectFailed = "last reconnect failed, giving up"
	// LogMsgUnexpectedRequestId is a log message emitted when a response
	// with an unrecognized request ID is received.
	LogMsgUnexpectedRequestId = "unexpected request ID in response"
	// LogMsgWatchEventReadFailed is a log message emitted when decoding
	// of a watch event from the server fails.
	LogMsgWatchEventReadFailed = "failed to decode watch event"
	// LogMsgPushUnsupported is a log message emitted when the server sends
	// a push message but box.session.push() is not supported.
	LogMsgPushUnsupported = "unsupported box.session.push()"
)

const (
	// LogKeyAttempt is a log key for the current reconnection attempt number.
	LogKeyAttempt = "attempt"
	// LogKeyMaxAttempts is a log key for the maximum number of reconnection
	// attempts.
	LogKeyMaxAttempts = "max_attempts"
	// LogKeyError is a log key for an error value.
	LogKeyError = "error"
	// LogKeyRequestId is a log key for an IPROTO request ID.
	LogKeyRequestId = "request_id"
	// LogKeyAddress is a log key for a connection address.
	LogKeyAddress = "address"
)
