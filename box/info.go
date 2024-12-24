package box

import (
	"fmt"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"
)

var _ tarantool.Request = (*InfoRequest)(nil)

// Info represents detailed information about the Tarantool instance.
// It includes version, node ID, read-only status, process ID, cluster information, and more.
type Info struct {
	// The Version of the Tarantool instance.
	Version string `msgpack:"version"`
	// The node ID (nullable).
	ID *int `msgpack:"id"`
	// Read-only (RO) status of the instance.
	RO bool `msgpack:"ro"`
	// UUID - Unique identifier of the instance.
	UUID string `msgpack:"uuid"`
	// Process ID of the instance.
	PID int `msgpack:"pid"`
	// Status - Current status of the instance (e.g., running, unconfigured).
	Status string `msgpack:"status"`
	// LSN - Log sequence number of the instance.
	LSN uint64 `msgpack:"lsn"`
	// Replication - replication status.
	Replication map[int]Replication `msgpack:"replication,omitempty"`
}

// Replication section of box.info() is a table with statistics for all instances
// in the replica set that the current instance belongs to.
type Replication struct {
	// ID is a short numeric identifier of instance n within the replica set.
	ID int `msgpack:"id"`
	// UUID - Unique identifier of the instance.
	UUID string `msgpack:"uuid"`
	// LSN - Log sequence number of the instance.
	LSN uint64 `msgpack:"lsn"`
	// Upstream - information about upstream.
	Upstream Upstream `msgpack:"upstream,omitempty"`
	// Downstream - information about downstream.
	Downstream Downstream `msgpack:"downstream,omitempty"`
}

// Upstream information.
type Upstream struct {
	// Status is replication status of the connection with the instance.
	Status string `msgpack:"status"`
	// Idle is the time (in seconds) since the last event was received.
	Idle float64 `msgpack:"idle"`
	// Peer contains instance n’s URI.
	Peer string `msgpack:"peer"`
	// Lag is the time difference between the local time of instance n,
	// recorded when the event was received, and the local time at another master
	// recorded when the event was written to the write-ahead log on that master.
	Lag float64 `msgpack:"lag"`
	// Message contains an error message in case of a degraded state; otherwise, it is nil.
	Message string `msgpack:"message,omitempty"`
	// SystemMessage contains an error message in case of a degraded state; otherwise, it is nil.
	SystemMessage string `msgpack:"system_message,omitempty"`
}

// Downstream information.
type Downstream struct {
	// Status is replication status of the connection with the instance.
	Status string `msgpack:"status"`
	// Idle is the time (in seconds) since the last event was received.
	Idle float64 `msgpack:"idle"`
	// VClock contains the vector clock, which is a table of ‘id, lsn’ pairs.
	VClock map[int]uint64 `msgpack:"vclock"`
	// Lag is the time difference between the local time of instance n,
	// recorded when the event was received, and the local time at another master
	// recorded when the event was written to the write-ahead log on that master.
	Lag float64 `msgpack:"lag"`
	// Message contains an error message in case of a degraded state; otherwise, it is nil.
	Message string `msgpack:"message,omitempty"`
	// SystemMessage contains an error message in case of a degraded state; otherwise, it is nil.
	SystemMessage string `msgpack:"system_message,omitempty"`
}

// InfoResponse represents the response structure
// that holds the information of the Tarantool instance.
// It contains a single field: Info, which holds the instance details (version, UUID, PID, etc.).
type InfoResponse struct {
	Info Info
}

func (ir *InfoResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	if arrayLen != 1 {
		return fmt.Errorf("protocol violation; expected 1 array entry, got %d", arrayLen)
	}

	i := Info{}
	err = d.Decode(&i)
	if err != nil {
		return err
	}

	ir.Info = i

	return nil
}

// InfoRequest represents a request to retrieve information about the Tarantool instance.
// It implements the tarantool.Request interface.
type InfoRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewInfoRequest returns a new empty info request.
func NewInfoRequest() InfoRequest {
	callReq := tarantool.NewCallRequest("box.info")

	return InfoRequest{
		callReq,
	}
}
