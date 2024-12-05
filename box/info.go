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

// Info retrieves the current information of the Tarantool instance.
// It calls the "box.info" function and parses the result into the Info structure.
func (b *box) Info() (Info, error) {
	var infoResp InfoResponse

	// Call "box.info" to get instance information from Tarantool.
	fut := b.conn.Do(NewInfoRequest())

	// Parse the result into the Info structure.
	err := fut.GetTyped(&infoResp)
	if err != nil {
		return Info{}, err
	}

	// Return the parsed info and any potential error.
	return infoResp.Info, err
}

// InfoRequest represents a request to retrieve information about the Tarantool instance.
// It implements the tarantool.Request interface.
type InfoRequest struct {
	baseRequest
}

// Body method is used to serialize the request's body.
// It is part of the tarantool.Request interface implementation.
func (i InfoRequest) Body(res tarantool.SchemaResolver, enc *msgpack.Encoder) error {
	return i.impl.Body(res, enc)
}

// NewInfoRequest returns a new empty info request.
func NewInfoRequest() InfoRequest {
	req := InfoRequest{}
	req.impl = newCall("box.info")
	return req
}
