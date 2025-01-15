package box

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestInfo(t *testing.T) {
	id := 1
	cases := []struct {
		Name   string
		Struct Info
		Data   map[string]interface{}
	}{
		{
			Name: "Case: base info struct",
			Struct: Info{
				Version: "2.11.4-0-g8cebbf2cad",
				ID:      &id,
				RO:      false,
				UUID:    "69360e9b-4641-4ec3-ab51-297f46749849",
				PID:     1,
				Status:  "running",
				LSN:     8,
			},
			Data: map[string]interface{}{
				"version": "2.11.4-0-g8cebbf2cad",
				"id":      1,
				"ro":      false,
				"uuid":    "69360e9b-4641-4ec3-ab51-297f46749849",
				"pid":     1,
				"status":  "running",
				"lsn":     8,
			},
		},
		{
			Name: "Case: info struct with replication",
			Struct: Info{
				Version: "2.11.4-0-g8cebbf2cad",
				ID:      &id,
				RO:      false,
				UUID:    "69360e9b-4641-4ec3-ab51-297f46749849",
				PID:     1,
				Status:  "running",
				LSN:     8,
				Replication: map[int]Replication{
					1: {
						ID:   1,
						UUID: "69360e9b-4641-4ec3-ab51-297f46749849",
						LSN:  8,
					},
					2: {
						ID:   2,
						UUID: "75f5f5aa-89f0-4d95-b5a9-96a0eaa0ce36",
						LSN:  0,
						Upstream: Upstream{
							Status:        "follow",
							Idle:          2.4564633660484,
							Peer:          "other.tarantool:3301",
							Lag:           0.00011920928955078,
							Message:       "'getaddrinfo: Name or service not known'",
							SystemMessage: "Input/output error",
						},
						Downstream: Downstream{
							Status:        "follow",
							Idle:          2.8306158290943,
							VClock:        map[int]uint64{1: 8},
							Lag:           0,
							Message:       "'unexpected EOF when reading from socket'",
							SystemMessage: "Broken pipe",
						},
					},
				},
			},
			Data: map[string]interface{}{
				"version": "2.11.4-0-g8cebbf2cad",
				"id":      1,
				"ro":      false,
				"uuid":    "69360e9b-4641-4ec3-ab51-297f46749849",
				"pid":     1,
				"status":  "running",
				"lsn":     8,
				"replication": map[interface{}]interface{}{
					1: map[string]interface{}{
						"id":   1,
						"uuid": "69360e9b-4641-4ec3-ab51-297f46749849",
						"lsn":  8,
					},
					2: map[string]interface{}{
						"id":   2,
						"uuid": "75f5f5aa-89f0-4d95-b5a9-96a0eaa0ce36",
						"lsn":  0,
						"upstream": map[string]interface{}{
							"status":         "follow",
							"idle":           2.4564633660484,
							"peer":           "other.tarantool:3301",
							"lag":            0.00011920928955078,
							"message":        "'getaddrinfo: Name or service not known'",
							"system_message": "Input/output error",
						},
						"downstream": map[string]interface{}{
							"status":         "follow",
							"idle":           2.8306158290943,
							"vclock":         map[interface{}]interface{}{1: 8},
							"lag":            0,
							"message":        "'unexpected EOF when reading from socket'",
							"system_message": "Broken pipe",
						},
					},
				},
			},
		},
	}
	for _, tc := range cases {
		data, err := msgpack.Marshal(tc.Data)
		require.NoError(t, err, tc.Name)

		var result Info
		err = msgpack.Unmarshal(data, &result)
		require.NoError(t, err, tc.Name)

		require.Equal(t, tc.Struct, result)
	}
}
