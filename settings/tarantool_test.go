package settings_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tarantool/go-tarantool/v2"
	. "github.com/tarantool/go-tarantool/v2/settings"
	"github.com/tarantool/go-tarantool/v2/test_helpers"
)

// There is no way to skip tests in testing.M,
// so we use this variable to pass info
// to each testing.T that it should skip.
var isSettingsSupported = false

var server = "127.0.0.1:3013"
var dialer = tarantool.NetDialer{
	Address:  server,
	User:     "test",
	Password: "test",
}
var opts = tarantool.Opts{
	Timeout: 5 * time.Second,
}

func skipIfSettingsUnsupported(t *testing.T) {
	t.Helper()

	if isSettingsSupported == false {
		t.Skip("Skipping test for Tarantool without session settings support")
	}
}

func skipIfErrorMarshalingEnabledSettingUnsupported(t *testing.T) {
	t.Helper()

	test_helpers.SkipIfFeatureUnsupported(t, "error_marshaling_enabled session setting", 2, 4, 1)
	test_helpers.SkipIfFeatureDropped(t, "error_marshaling_enabled session setting", 2, 10, 0)
}

func skipIfSQLDeferForeignKeysSettingUnsupported(t *testing.T) {
	t.Helper()

	test_helpers.SkipIfFeatureUnsupported(t, "sql_defer_foreign_keys session setting", 2, 3, 1)
	test_helpers.SkipIfFeatureDropped(t, "sql_defer_foreign_keys session setting", 2, 10, 5)
}

func TestErrorMarshalingEnabledSetting(t *testing.T) {
	skipIfErrorMarshalingEnabledSettingUnsupported(t)

	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Disable receiving box.error as MP_EXT 3.
	data, err := conn.Do(NewErrorMarshalingEnabledSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewErrorMarshalingEnabledGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", false}}, data)

	// Get a box.Error value.
	eval := tarantool.NewEvalRequest("return box.error.new(box.error.UNKNOWN)")
	data, err = conn.Do(eval).Get()
	require.Nil(t, err)
	require.IsType(t, "string", data[0])

	// Enable receiving box.error as MP_EXT 3.
	data, err = conn.Do(NewErrorMarshalingEnabledSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewErrorMarshalingEnabledGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", true}}, data)

	// Get a box.Error value.
	data, err = conn.Do(eval).Get()
	require.Nil(t, err)
	_, ok := data[0].(*tarantool.BoxError)
	require.True(t, ok)
}

func TestSQLDefaultEngineSetting(t *testing.T) {
	// https://github.com/tarantool/tarantool/blob/680990a082374e4790539215f69d9e9ee39c3307/test/sql/engine.test.lua
	skipIfSettingsUnsupported(t)

	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Set default SQL "CREATE TABLE" engine to "vinyl".
	data, err := conn.Do(NewSQLDefaultEngineSetRequest("vinyl")).Get()
	require.Nil(t, err)
	require.EqualValues(t, []interface{}{[]interface{}{"sql_default_engine", "vinyl"}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLDefaultEngineGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "vinyl"}}, data)

	// Create a space with "CREATE TABLE".
	exec := tarantool.NewExecuteRequest("CREATE TABLE T1_VINYL(a INT PRIMARY KEY, b INT, c INT);")
	resp, err := conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Check new space engine.
	eval := tarantool.NewEvalRequest("return box.space['T1_VINYL'].engine")
	data, err = conn.Do(eval).Get()
	require.Nil(t, err)
	require.Equal(t, "vinyl", data[0])

	// Set default SQL "CREATE TABLE" engine to "memtx".
	data, err = conn.Do(NewSQLDefaultEngineSetRequest("memtx")).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "memtx"}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLDefaultEngineGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "memtx"}}, data)

	// Create a space with "CREATE TABLE".
	exec = tarantool.NewExecuteRequest("CREATE TABLE T2_MEMTX(a INT PRIMARY KEY, b INT, c INT);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.True(t, ok, "wrong response type")
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Check new space engine.
	eval = tarantool.NewEvalRequest("return box.space['T2_MEMTX'].engine")
	data, err = conn.Do(eval).Get()
	require.Nil(t, err)
	require.Equal(t, "memtx", data[0])
}

func TestSQLDeferForeignKeysSetting(t *testing.T) {
	// https://github.com/tarantool/tarantool/blob/eafadc13425f14446d7aaa49dea67dfc1d5f45e9/test/sql/transitive-transactions.result
	skipIfSQLDeferForeignKeysSettingUnsupported(t)

	var resp tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Create a parent space.
	exec := tarantool.NewExecuteRequest("CREATE TABLE parent(id INT PRIMARY KEY, y INT UNIQUE);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Create a space with reference to the parent space.
	exec = tarantool.NewExecuteRequest(
		"CREATE TABLE child(id INT PRIMARY KEY, x INT REFERENCES parent(y));")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	deferEval := `
	    box.begin()
	    local _, err = box.execute('INSERT INTO child VALUES (2, 2);')
	    if err ~= nil then
	   	    box.rollback()
	        error(err)
	    end
	    box.execute('INSERT INTO parent VALUES (2, 2);')
	    box.commit()
	    return true
	`

	// Disable foreign key constraint checks before commit.
	data, err := conn.Do(NewSQLDeferForeignKeysSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLDeferForeignKeysGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", false}}, data)

	// Evaluate a scenario when foreign key not exists
	// on INSERT, but exists on commit.
	_, err = conn.Do(tarantool.NewEvalRequest(deferEval)).Get()
	require.NotNil(t, err)
	require.ErrorContains(t, err, "Failed to execute SQL statement: FOREIGN KEY constraint failed")

	data, err = conn.Do(NewSQLDeferForeignKeysSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLDeferForeignKeysGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", true}}, data)

	// Evaluate a scenario when foreign key not exists
	// on INSERT, but exists on commit.
	data, err = conn.Do(tarantool.NewEvalRequest(deferEval)).Get()
	require.Nil(t, err)
	require.Equal(t, true, data[0])
}

func TestSQLFullColumnNamesSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Create a space.
	exec := tarantool.NewExecuteRequest("CREATE TABLE FKNAME(ID INT PRIMARY KEY, X INT);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Fill it with some data.
	exec = tarantool.NewExecuteRequest("INSERT INTO FKNAME VALUES (1, 1);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Disable displaying full column names in metadata.
	data, err := conn.Do(NewSQLFullColumnNamesSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLFullColumnNamesGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", false}}, data)

	// Get a data with short column names in metadata.
	exec = tarantool.NewExecuteRequest("SELECT X FROM FKNAME WHERE ID = 1;")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	metaData, err := exResp.MetaData()
	require.Nil(t, err)
	require.Equal(t, "X", metaData[0].FieldName)

	// Enable displaying full column names in metadata.
	data, err = conn.Do(NewSQLFullColumnNamesSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLFullColumnNamesGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", true}}, data)

	// Get a data with full column names in metadata.
	exec = tarantool.NewExecuteRequest("SELECT X FROM FKNAME WHERE ID = 1;")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	metaData, err = exResp.MetaData()
	require.Nil(t, err)
	require.Equal(t, "FKNAME.X", metaData[0].FieldName)
}

func TestSQLFullMetadataSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Create a space.
	exec := tarantool.NewExecuteRequest("CREATE TABLE fmt(id INT PRIMARY KEY, x INT);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Fill it with some data.
	exec = tarantool.NewExecuteRequest("INSERT INTO fmt VALUES (1, 1);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Disable displaying additional fields in metadata.
	data, err := conn.Do(NewSQLFullMetadataSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLFullMetadataGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", false}}, data)

	// Get a data without additional fields in metadata.
	exec = tarantool.NewExecuteRequest("SELECT x FROM fmt WHERE id = 1;")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	metaData, err := exResp.MetaData()
	require.Nil(t, err)
	require.Equal(t, "", metaData[0].FieldSpan)

	// Enable displaying full column names in metadata.
	data, err = conn.Do(NewSQLFullMetadataSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLFullMetadataGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", true}}, data)

	// Get a data with additional fields in metadata.
	exec = tarantool.NewExecuteRequest("SELECT x FROM fmt WHERE id = 1;")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	metaData, err = exResp.MetaData()
	require.Nil(t, err)
	require.Equal(t, "x", metaData[0].FieldSpan)
}

func TestSQLParserDebugSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Disable parser debug mode.
	data, err := conn.Do(NewSQLParserDebugSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLParserDebugGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", false}}, data)

	// Enable parser debug mode.
	data, err = conn.Do(NewSQLParserDebugSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLParserDebugGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", true}}, data)

	// To test real effect we need a Tarantool instance built with
	// `-DCMAKE_BUILD_TYPE=Debug`.
}

func TestSQLRecursiveTriggersSetting(t *testing.T) {
	// https://github.com/tarantool/tarantool/blob/d11fb3061e15faf4e0eb5375fb8056b4e64348ae/test/sql-tap/triggerC.test.lua
	skipIfSettingsUnsupported(t)

	var resp tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Create a space.
	exec := tarantool.NewExecuteRequest("CREATE TABLE rec(id INTEGER PRIMARY KEY, a INT, b INT);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Fill it with some data.
	exec = tarantool.NewExecuteRequest("INSERT INTO rec VALUES(1, 1, 2);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Create a recursive trigger (with infinite depth).
	exec = tarantool.NewExecuteRequest(`
		CREATE TRIGGER tr12 AFTER UPDATE ON rec FOR EACH ROW BEGIN
          UPDATE rec SET a=new.a+1, b=new.b+1;
        END;`)
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Enable SQL recursive triggers.
	data, err := conn.Do(NewSQLRecursiveTriggersSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLRecursiveTriggersGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", true}}, data)

	// Trigger the recursion.
	exec = tarantool.NewExecuteRequest("UPDATE rec SET a=a+1, b=b+1;")
	_, err = conn.Do(exec).Get()
	require.NotNil(t, err)
	require.ErrorContains(t, err,
		"Failed to execute SQL statement: too many levels of trigger recursion")

	// Disable SQL recursive triggers.
	data, err = conn.Do(NewSQLRecursiveTriggersSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLRecursiveTriggersGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", false}}, data)

	// Trigger the recursion.
	exec = tarantool.NewExecuteRequest("UPDATE rec SET a=a+1, b=b+1;")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)
}

func TestSQLReverseUnorderedSelectsSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Create a space.
	exec := tarantool.NewExecuteRequest("CREATE TABLE data(id STRING PRIMARY KEY);")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok := resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err := exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Fill it with some data.
	exec = tarantool.NewExecuteRequest("INSERT INTO data VALUES('1');")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	exec = tarantool.NewExecuteRequest("INSERT INTO data VALUES('2');")
	resp, err = conn.Do(exec).GetResponse()
	require.Nil(t, err)
	require.NotNil(t, resp)
	exResp, ok = resp.(*tarantool.ExecuteResponse)
	require.True(t, ok, "wrong response type")
	sqlInfo, err = exResp.SQLInfo()
	require.Nil(t, err)
	require.Equal(t, uint64(1), sqlInfo.AffectedCount)

	// Disable reverse order in unordered selects.
	data, err := conn.Do(NewSQLReverseUnorderedSelectsSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", false}},
		data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLReverseUnorderedSelectsGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", false}},
		data)

	// Select multiple records.
	query := "SELECT * FROM seqscan data;"
	if isSeqScanOld, err := test_helpers.IsTarantoolVersionLess(3, 0, 0); err != nil {
		t.Fatalf("Could not check the Tarantool version: %s", err)
	} else if isSeqScanOld {
		query = "SELECT * FROM data;"
	}

	data, err = conn.Do(tarantool.NewExecuteRequest(query)).Get()
	require.Nil(t, err)
	require.EqualValues(t, []interface{}{"1"}, data[0])
	require.EqualValues(t, []interface{}{"2"}, data[1])

	// Enable reverse order in unordered selects.
	data, err = conn.Do(NewSQLReverseUnorderedSelectsSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", true}},
		data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLReverseUnorderedSelectsGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", true}},
		data)

	// Select multiple records.
	data, err = conn.Do(tarantool.NewExecuteRequest(query)).Get()
	require.Nil(t, err)
	require.EqualValues(t, []interface{}{"2"}, data[0])
	require.EqualValues(t, []interface{}{"1"}, data[1])
}

func TestSQLSelectDebugSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Disable select debug mode.
	data, err := conn.Do(NewSQLSelectDebugSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLSelectDebugGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", false}}, data)

	// Enable select debug mode.
	data, err = conn.Do(NewSQLSelectDebugSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLSelectDebugGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", true}}, data)

	// To test real effect we need a Tarantool instance built with
	// `-DCMAKE_BUILD_TYPE=Debug`.
}

func TestSQLVDBEDebugSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Disable VDBE debug mode.
	data, err := conn.Do(NewSQLVDBEDebugSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", false}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLVDBEDebugGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", false}}, data)

	// Enable VDBE debug mode.
	data, err = conn.Do(NewSQLVDBEDebugSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", true}}, data)

	// Fetch current setting value.
	data, err = conn.Do(NewSQLVDBEDebugGetRequest()).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", true}}, data)

	// To test real effect we need a Tarantool instance built with
	// `-DCMAKE_BUILD_TYPE=Debug`.
}

func TestSessionSettings(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var err error

	conn := test_helpers.ConnectWithValidation(t, dialer, opts)
	defer conn.Close()

	// Set some settings values.
	data, err := conn.Do(NewSQLDefaultEngineSetRequest("memtx")).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "memtx"}}, data)

	data, err = conn.Do(NewSQLFullColumnNamesSetRequest(true)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", true}}, data)

	// Fetch current settings values.
	data, err = conn.Do(NewSessionSettingsGetRequest()).Get()
	require.Nil(t, err)
	require.Subset(t, data,
		[]interface{}{
			[]interface{}{"sql_default_engine", "memtx"},
			[]interface{}{"sql_full_column_names", true},
		})
}

// runTestMain is a body of TestMain function
// (see https://pkg.go.dev/testing#hdr-Main).
// Using defer + os.Exit is not works so TestMain body
// is a separate function, see
// https://stackoverflow.com/questions/27629380/how-to-exit-a-go-program-honoring-deferred-calls
func runTestMain(m *testing.M) int {
	isLess, err := test_helpers.IsTarantoolVersionLess(2, 3, 1)
	if err != nil {
		log.Fatalf("Failed to extract tarantool version: %s", err)
	}

	if isLess {
		log.Println("Skipping session settings tests...")
		isSettingsSupported = false
		return m.Run()
	}

	isSettingsSupported = true

	inst, err := test_helpers.StartTarantool(test_helpers.StartOpts{
		Dialer:       dialer,
		InitScript:   "testdata/config.lua",
		Listen:       server,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 10,
		RetryTimeout: 500 * time.Millisecond,
	})

	if err != nil {
		log.Fatalf("Failed to prepare test tarantool: %s", err)
	}

	defer test_helpers.StopTarantoolWithCleanup(inst)

	return m.Run()
}

func TestMain(m *testing.M) {
	code := runTestMain(m)
	os.Exit(code)
}
