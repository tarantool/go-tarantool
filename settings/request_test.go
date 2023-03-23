package settings_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/settings"
)

type ValidSchemeResolver struct {
}

func (*ValidSchemeResolver) ResolveSpaceIndex(s, i interface{}) (spaceNo, indexNo uint32, err error) {
	if s == nil {
		if s == "_session_settings" {
			spaceNo = 380
		} else {
			spaceNo = uint32(s.(int))
		}
	} else {
		spaceNo = 0
	}
	if i != nil {
		indexNo = uint32(i.(int))
	} else {
		indexNo = 0
	}

	return spaceNo, indexNo, nil
}

var resolver ValidSchemeResolver

func TestRequestsAPI(t *testing.T) {
	tests := []struct {
		req   tarantool.Request
		async bool
		code  int32
	}{
		{req: NewErrorMarshalingEnabledSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewErrorMarshalingEnabledGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLDefaultEngineSetRequest("memtx"), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLDefaultEngineGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLDeferForeignKeysSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLDeferForeignKeysGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLFullColumnNamesSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLFullColumnNamesGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLFullMetadataSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLFullMetadataGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLParserDebugSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLParserDebugGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLRecursiveTriggersSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLRecursiveTriggersGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLReverseUnorderedSelectsSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLReverseUnorderedSelectsGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLSelectDebugSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLSelectDebugGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSQLVDBEDebugSetRequest(false), async: false, code: tarantool.UpdateRequestCode},
		{req: NewSQLVDBEDebugGetRequest(), async: false, code: tarantool.SelectRequestCode},
		{req: NewSessionSettingsGetRequest(), async: false, code: tarantool.SelectRequestCode},
	}

	for _, test := range tests {
		require.Equal(t, test.async, test.req.Async())
		require.Equal(t, test.code, test.req.Code())

		var reqBuf bytes.Buffer
		enc := NewEncoder(&reqBuf)
		require.Nilf(t, test.req.Body(&resolver, enc), "No errors on fill")
	}
}

func TestRequestsCtx(t *testing.T) {
	// tarantool.Request interface doesn't have Context()
	getTests := []struct {
		req *GetRequest
	}{
		{req: NewErrorMarshalingEnabledGetRequest()},
		{req: NewSQLDefaultEngineGetRequest()},
		{req: NewSQLDeferForeignKeysGetRequest()},
		{req: NewSQLFullColumnNamesGetRequest()},
		{req: NewSQLFullMetadataGetRequest()},
		{req: NewSQLParserDebugGetRequest()},
		{req: NewSQLRecursiveTriggersGetRequest()},
		{req: NewSQLReverseUnorderedSelectsGetRequest()},
		{req: NewSQLSelectDebugGetRequest()},
		{req: NewSQLVDBEDebugGetRequest()},
		{req: NewSessionSettingsGetRequest()},
	}

	for _, test := range getTests {
		var ctx context.Context
		require.Equal(t, ctx, test.req.Context(ctx).Ctx())
	}

	setTests := []struct {
		req *SetRequest
	}{
		{req: NewErrorMarshalingEnabledSetRequest(false)},
		{req: NewSQLDefaultEngineSetRequest("memtx")},
		{req: NewSQLDeferForeignKeysSetRequest(false)},
		{req: NewSQLFullColumnNamesSetRequest(false)},
		{req: NewSQLFullMetadataSetRequest(false)},
		{req: NewSQLParserDebugSetRequest(false)},
		{req: NewSQLRecursiveTriggersSetRequest(false)},
		{req: NewSQLReverseUnorderedSelectsSetRequest(false)},
		{req: NewSQLSelectDebugSetRequest(false)},
		{req: NewSQLVDBEDebugSetRequest(false)},
	}

	for _, test := range setTests {
		var ctx context.Context
		require.Equal(t, ctx, test.req.Context(ctx).Ctx())
	}
}
