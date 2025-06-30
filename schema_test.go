package tarantool_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

func TestGetSchema_ok(t *testing.T) {
	space1 := tarantool.Space{
		Id:          1,
		Name:        "name1",
		Indexes:     make(map[string]tarantool.Index),
		IndexesById: make(map[uint32]tarantool.Index),
		Fields:      make(map[string]tarantool.Field),
		FieldsById:  make(map[uint32]tarantool.Field),
	}
	index := tarantool.Index{
		Id:      1,
		SpaceId: 2,
		Name:    "index_name",
		Type:    "index_type",
		Unique:  true,
		Fields:  make([]tarantool.IndexField, 0),
	}
	space2 := tarantool.Space{
		Id:   2,
		Name: "name2",
		Indexes: map[string]tarantool.Index{
			"index_name": index,
		},
		IndexesById: map[uint32]tarantool.Index{
			1: index,
		},
		Fields:     make(map[string]tarantool.Field),
		FieldsById: make(map[uint32]tarantool.Field),
	}

	mockDoer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, [][]interface{}{
			{
				uint32(1),
				"skip",
				"name1",
				"",
				0,
			},
			{
				uint32(2),
				"skip",
				"name2",
				"",
				0,
			},
		}),
		test_helpers.NewMockResponse(t, [][]interface{}{
			{
				uint32(2),
				uint32(1),
				"index_name",
				"index_type",
				uint8(1),
				uint8(0),
			},
		}),
	)

	expectedSchema := tarantool.Schema{
		SpacesById: map[uint32]tarantool.Space{
			1: space1,
			2: space2,
		},
		Spaces: map[string]tarantool.Space{
			"name1": space1,
			"name2": space2,
		},
	}

	schema, err := tarantool.GetSchema(&mockDoer)
	require.NoError(t, err)
	require.Equal(t, expectedSchema, schema)
}

func TestGetSchema_spaces_select_error(t *testing.T) {
	mockDoer := test_helpers.NewMockDoer(t, fmt.Errorf("some error"))

	schema, err := tarantool.GetSchema(&mockDoer)
	require.EqualError(t, err, "some error")
	require.Equal(t, tarantool.Schema{}, schema)
}

func TestGetSchema_index_select_error(t *testing.T) {
	mockDoer := test_helpers.NewMockDoer(t,
		test_helpers.NewMockResponse(t, [][]interface{}{
			{
				uint32(1),
				"skip",
				"name1",
				"",
				0,
			},
		}),
		fmt.Errorf("some error"))

	schema, err := tarantool.GetSchema(&mockDoer)
	require.EqualError(t, err, "some error")
	require.Equal(t, tarantool.Schema{}, schema)
}

func TestResolverCalledWithoutNameSupport(t *testing.T) {
	resolver := ValidSchemeResolver{nameUseSupported: false}

	req := tarantool.NewSelectRequest("valid")
	req.Index("valid")

	var reqBuf bytes.Buffer
	reqEnc := msgpack.NewEncoder(&reqBuf)

	err := req.Body(&resolver, reqEnc)
	if err != nil {
		t.Errorf("An unexpected Response.Body() error: %q", err.Error())
	}

	if resolver.spaceResolverCalls != 1 {
		t.Errorf("ResolveSpace was called %d times instead of 1.",
			resolver.spaceResolverCalls)
	}
	if resolver.indexResolverCalls != 1 {
		t.Errorf("ResolveIndex was called %d times instead of 1.",
			resolver.indexResolverCalls)
	}
}

func TestResolverNotCalledWithNameSupport(t *testing.T) {
	resolver := ValidSchemeResolver{nameUseSupported: true}

	req := tarantool.NewSelectRequest("valid")
	req.Index("valid")

	var reqBuf bytes.Buffer
	reqEnc := msgpack.NewEncoder(&reqBuf)

	err := req.Body(&resolver, reqEnc)
	if err != nil {
		t.Errorf("An unexpected Response.Body() error: %q", err.Error())
	}

	if resolver.spaceResolverCalls != 0 {
		t.Errorf("ResolveSpace was called %d times instead of 0.",
			resolver.spaceResolverCalls)
	}
	if resolver.indexResolverCalls != 0 {
		t.Errorf("ResolveIndex was called %d times instead of 0.",
			resolver.indexResolverCalls)
	}
}

func TestErrConcurrentSchemaUpdate(t *testing.T) {
	assert.EqualError(t, tarantool.ErrConcurrentSchemaUpdate, "concurrent schema update")
}
