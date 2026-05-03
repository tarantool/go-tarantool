package pool

const (
	// LogMsgConnectFailed is a log message emitted when a connection
	// attempt to a pool instance fails.
	LogMsgConnectFailed = "connect failed"
	// LogMsgInitWatchersFailed is a log message emitted when initialization
	// of watchers for a pool instance connection fails.
	LogMsgInitWatchersFailed = "failed to initialize watchers"
	// LogMsgStoringConnectionCanceled is a log message emitted when storing
	// a discovered connection is canceled by the Handler.Discovered callback.
	LogMsgStoringConnectionCanceled = "storing connection canceled"
	// LogMsgDeactivatingFailed is a log message emitted when the
	// Handler.Deactivated callback returns an error during connection
	// deactivation.
	LogMsgDeactivatingFailed = "handler deactivation failed"
	// LogMsgStoringConnectionFailed is a log message emitted when storing
	// a connection fails due to a role detection error.
	LogMsgStoringConnectionFailed = "storing connection failed"
	// LogMsgReconnectFailed is a log message emitted when a reconnection
	// attempt for a pool instance fails.
	LogMsgReconnectFailed = "reconnect failed"
	// LogMsgReopenFailed is a log message emitted when reopening a
	// connection for a pool instance fails.
	LogMsgReopenFailed = "reopen connection failed"
)

const (
	// LogKeyInstance is a log key for a pool instance name.
	LogKeyInstance = "instance"
	// LogKeyError is a log key for an error value.
	// It intentionally mirrors tarantool.LogKeyError to avoid an
	// inter-package dependency.
	LogKeyError = "error"
)
