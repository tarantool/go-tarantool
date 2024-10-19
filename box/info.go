package box

import "github.com/tarantool/go-tarantool/v2"

// ClusterInfo represents information about the cluster.
// It contains the unique identifier (UUID) of the cluster.
type ClusterInfo struct {
	UUID string `msgpack:"uuid"`
}

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
	// Cluster information, including cluster UUID.
	Cluster ClusterInfo `msgpack:"cluster"`
}

// Info retrieves the current information of the Tarantool instance.
// It calls the "box.info" function and parses the result into the Info structure.
func (b *box) Info() (Info, error) {
	var info Info

	// Call "box.info" to get instance information from Tarantool.
	fut := b.conn.Do(tarantool.NewCallRequest("box.info"))

	// Parse the result into the Info structure.
	err := fut.GetTyped(&[]interface{}{&info})
	if err != nil {
		return Info{}, err
	}

	// Return the parsed info and any potential error.
	return info, err
}
