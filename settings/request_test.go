package settings_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-iproto"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/tarantool/go-tarantool/v2"
	. "github.com/tarantool/go-tarantool/v2/settings"
)

type ValidSchemeResolver struct {
}

func (*ValidSchemeResolver) ResolveSpaceIndex(s, i interface{}) (uint32, uint32, error) {
	var spaceNo, indexNo uint32
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
		rtype iproto.Type
	}{
		{req: NewErrorMarshalingEnabledSetRequest(false), async: false,
			rtype: iproto.IPROTO_UPDATE},
		{req: NewErrorMarshalingEnabledGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLDefaultEngineSetRequest("memtx"), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLDefaultEngineGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLDeferForeignKeysSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLDeferForeignKeysGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLFullColumnNamesSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLFullColumnNamesGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLFullMetadataSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLFullMetadataGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLParserDebugSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLParserDebugGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLRecursiveTriggersSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLRecursiveTriggersGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLReverseUnorderedSelectsSetRequest(false), async: false,
			rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLReverseUnorderedSelectsGetRequest(), async: false,
			rtype: iproto.IPROTO_SELECT},
		{req: NewSQLSelectDebugSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLSelectDebugGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSQLVDBEDebugSetRequest(false), async: false, rtype: iproto.IPROTO_UPDATE},
		{req: NewSQLVDBEDebugGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
		{req: NewSessionSettingsGetRequest(), async: false, rtype: iproto.IPROTO_SELECT},
	}

	for _, test := range tests {
		require.Equal(t, test.async, test.req.Async())
		require.Equal(t, test.rtype, test.req.Type())

		var reqBuf bytes.Buffer
		enc := msgpack.NewEncoder(&reqBuf)
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
