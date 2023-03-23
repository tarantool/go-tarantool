// Package settings is a collection of requests to set a connection session setting
// or get current session configuration.
//
//	+============================+=========================+=========+===========================+
//	|           Setting          |         Meaning         | Default |       Supported in        |
//	|                            |                         |         |     Tarantool versions    |
//	+============================+=========================+=========+===========================+
//	|   ErrorMarshalingEnabled   | Defines whether error   |  false  | Since 2.4.1 till 2.10.0,  |
//	|                            | objectshave a special   |         | replaced with IPROTO_ID   |
//	|                            | structure.              |         | feature flag.             |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|      SQLDefaultEngine      | Defines default storage | "memtx" | Since 2.3.1.              |
//	|                            | engine for new SQL      |         |                           |
//	|                            | tables.                 |         |                           |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|     SQLDeferForeignKeys    | Defines whether         |  false  | Since 2.3.1 till master   |
//	|                            | foreign-key checks can  |         | commit 14618c4 (possible  |
//	|                            |  wait till commit.      |         | 2.10.5 or 2.11.0)         |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|     SQLFullColumnNames     | Defines whether full    |  false  | Since 2.3.1.              |
//	|                            | column names is         |         |                           |
//	|                            | displayed in SQL result |         |                           |
//	|                            | set metadata.           |         |                           |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|      SQLFullMetadata       | Defines whether SQL     |  false  | Since 2.3.1.              |
//	|                            | result set metadata     |         |                           |
//	|                            | will have more than     |         |                           |
//	|                            | just name and type.     |         |                           |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|       SQLParserDebug       | Defines whether to show |  false  | Since 2.3.1 (only if      |
//	|                            | parser steps for        |         | built with                |
//	|                            | following statements.   |         | -DCMAKE_BUILD_TYPE=Debug) |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|    SQLRecursiveTriggers    | Defines whether a       |  true   | Since 2.3.1.              |
//	|                            | triggered statement can |         |                           |
//	|                            | activate a trigger.     |         |                           |
//	+----------------------------+-------------------------+---------+---------------------------+
//	| SQLReverseUnorderedSelects | Defines defines whether |  false  | Since 2.3.1.              |
//	|                            | result rows are usually |         |                           |
//	|                            | in reverse order if     |         |                           |
//	|                            | there is no ORDER BY    |         |                           |
//	|                            | clause.                 |         |                           |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|       SQLSelectDebug       | Defines whether to show |  false  | Since 2.3.1 (only if      |
//	|                            | to show execution steps |         | built with                |
//	|                            | during SELECT.          |         | -DCMAKE_BUILD_TYPE=Debug) |
//	+----------------------------+-------------------------+---------+---------------------------+
//	|        SQLVDBEDebug        | Defines whether VDBE    |  false  | Since 2.3.1 (only if      |
//	|                            | debug mode is enabled.  |         | built with                |
//	|                            |                         |         | -DCMAKE_BUILD_TYPE=Debug) |
//	+----------------------------+-------------------------+---------+---------------------------+
//
// Since: 1.10.0
//
// See also:
//
// * Session settings https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_space/_session_settings/
package settings

import (
	"context"

	"github.com/ice-blockchain/go-tarantool"
)

// SetRequest helps to set session settings.
type SetRequest struct {
	impl *tarantool.UpdateRequest
}

func newSetRequest(setting string, value interface{}) *SetRequest {
	return &SetRequest{
		impl: tarantool.NewUpdateRequest(sessionSettingsSpace).
			Key(tarantool.StringKey{S: setting}).
			Operations(tarantool.NewOperations().Assign(sessionSettingValueField, value)),
	}
}

// Context sets a passed context to set session settings request.
func (req *SetRequest) Context(ctx context.Context) *SetRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// Code returns IPROTO code for set session settings request.
func (req *SetRequest) Code() int32 {
	return req.impl.Code()
}

// Body fills an encoder with set session settings request body.
func (req *SetRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	return req.impl.Body(res, enc)
}

// Ctx returns a context of set session settings request.
func (req *SetRequest) Ctx() context.Context {
	return req.impl.Ctx()
}

// Async returns is set session settings request expects a response.
func (req *SetRequest) Async() bool {
	return req.impl.Async()
}

// GetRequest helps to get session settings.
type GetRequest struct {
	impl *tarantool.SelectRequest
}

func newGetRequest(setting string) *GetRequest {
	return &GetRequest{
		impl: tarantool.NewSelectRequest(sessionSettingsSpace).
			Key(tarantool.StringKey{S: setting}).
			Limit(1),
	}
}

// Context sets a passed context to get session settings request.
func (req *GetRequest) Context(ctx context.Context) *GetRequest {
	req.impl = req.impl.Context(ctx)

	return req
}

// Code returns IPROTO code for get session settings request.
func (req *GetRequest) Code() int32 {
	return req.impl.Code()
}

// Body fills an encoder with get session settings request body.
func (req *GetRequest) Body(res tarantool.SchemaResolver, enc *encoder) error {
	return req.impl.Body(res, enc)
}

// Ctx returns a context of get session settings request.
func (req *GetRequest) Ctx() context.Context {
	return req.impl.Ctx()
}

// Async returns is get session settings request expects a response.
func (req *GetRequest) Async() bool {
	return req.impl.Async()
}

// NewErrorMarshalingEnabledSetRequest creates a request to
// update current session ErrorMarshalingEnabled setting.
func NewErrorMarshalingEnabledSetRequest(value bool) *SetRequest {
	return newSetRequest(errorMarshalingEnabled, value)
}

// NewErrorMarshalingEnabledGetRequest creates a request to get
// current session ErrorMarshalingEnabled setting in tuple format.
func NewErrorMarshalingEnabledGetRequest() *GetRequest {
	return newGetRequest(errorMarshalingEnabled)
}

// NewSQLDefaultEngineSetRequest creates a request to
// update current session SQLDefaultEngine setting.
func NewSQLDefaultEngineSetRequest(value string) *SetRequest {
	return newSetRequest(sqlDefaultEngine, value)
}

// NewSQLDefaultEngineGetRequest creates a request to get
// current session SQLDefaultEngine setting in tuple format.
func NewSQLDefaultEngineGetRequest() *GetRequest {
	return newGetRequest(sqlDefaultEngine)
}

// NewSQLDeferForeignKeysSetRequest creates a request to
// update current session SQLDeferForeignKeys setting.
func NewSQLDeferForeignKeysSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlDeferForeignKeys, value)
}

// NewSQLDeferForeignKeysGetRequest creates a request to get
// current session SQLDeferForeignKeys setting in tuple format.
func NewSQLDeferForeignKeysGetRequest() *GetRequest {
	return newGetRequest(sqlDeferForeignKeys)
}

// NewSQLFullColumnNamesSetRequest creates a request to
// update current session SQLFullColumnNames setting.
func NewSQLFullColumnNamesSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlFullColumnNames, value)
}

// NewSQLFullColumnNamesGetRequest creates a request to get
// current session SQLFullColumnNames setting in tuple format.
func NewSQLFullColumnNamesGetRequest() *GetRequest {
	return newGetRequest(sqlFullColumnNames)
}

// NewSQLFullMetadataSetRequest creates a request to
// update current session SQLFullMetadata setting.
func NewSQLFullMetadataSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlFullMetadata, value)
}

// NewSQLFullMetadataGetRequest creates a request to get
// current session SQLFullMetadata setting in tuple format.
func NewSQLFullMetadataGetRequest() *GetRequest {
	return newGetRequest(sqlFullMetadata)
}

// NewSQLParserDebugSetRequest creates a request to
// update current session SQLParserDebug setting.
func NewSQLParserDebugSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlParserDebug, value)
}

// NewSQLParserDebugGetRequest creates a request to get
// current session SQLParserDebug setting in tuple format.
func NewSQLParserDebugGetRequest() *GetRequest {
	return newGetRequest(sqlParserDebug)
}

// NewSQLRecursiveTriggersSetRequest creates a request to
// update current session SQLRecursiveTriggers setting.
func NewSQLRecursiveTriggersSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlRecursiveTriggers, value)
}

// NewSQLRecursiveTriggersGetRequest creates a request to get
// current session SQLRecursiveTriggers setting in tuple format.
func NewSQLRecursiveTriggersGetRequest() *GetRequest {
	return newGetRequest(sqlRecursiveTriggers)
}

// NewSQLReverseUnorderedSelectsSetRequest creates a request to
// update current session SQLReverseUnorderedSelects setting.
func NewSQLReverseUnorderedSelectsSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlReverseUnorderedSelects, value)
}

// NewSQLReverseUnorderedSelectsGetRequest creates a request to get
// current session SQLReverseUnorderedSelects setting in tuple format.
func NewSQLReverseUnorderedSelectsGetRequest() *GetRequest {
	return newGetRequest(sqlReverseUnorderedSelects)
}

// NewSQLSelectDebugSetRequest creates a request to
// update current session SQLSelectDebug setting.
func NewSQLSelectDebugSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlSelectDebug, value)
}

// NewSQLSelectDebugGetRequest creates a request to get
// current session SQLSelectDebug setting in tuple format.
func NewSQLSelectDebugGetRequest() *GetRequest {
	return newGetRequest(sqlSelectDebug)
}

// NewSQLVDBEDebugSetRequest creates a request to
// update current session SQLVDBEDebug setting.
func NewSQLVDBEDebugSetRequest(value bool) *SetRequest {
	return newSetRequest(sqlVDBEDebug, value)
}

// NewSQLVDBEDebugGetRequest creates a request to get
// current session SQLVDBEDebug setting in tuple format.
func NewSQLVDBEDebugGetRequest() *GetRequest {
	return newGetRequest(sqlVDBEDebug)
}

// NewSessionSettingsGetRequest creates a request to get all
// current session settings in tuple format.
func NewSessionSettingsGetRequest() *GetRequest {
	return &GetRequest{
		impl: tarantool.NewSelectRequest(sessionSettingsSpace).
			Limit(selectAllLimit),
	}
}
