package settings_test

import (
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/go-tarantool"
	. "github.com/ice-blockchain/go-tarantool/settings"
	"github.com/ice-blockchain/go-tarantool/test_helpers"
)

// There is no way to skip tests in testing.M,
// so we use this variable to pass info
// to each testing.T that it should skip.
var isSettingsSupported = false

var server = "127.0.0.1:3013"
var opts = tarantool.Opts{
	Timeout: 500 * time.Millisecond,
	User:    "test",
	Pass:    "test",
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

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Disable receiving box.error as MP_EXT 3.
	resp, err = conn.Do(NewErrorMarshalingEnabledSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewErrorMarshalingEnabledGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", false}}, resp.Data)

	// Get a box.Error value.
	resp, err = conn.Eval("return box.error.new(box.error.UNKNOWN)", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.IsType(t, "string", resp.Data[0])

	// Enable receiving box.error as MP_EXT 3.
	resp, err = conn.Do(NewErrorMarshalingEnabledSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewErrorMarshalingEnabledGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"error_marshaling_enabled", true}}, resp.Data)

	// Get a box.Error value.
	resp, err = conn.Eval("return box.error.new(box.error.UNKNOWN)", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	_, ok := toBoxError(resp.Data[0])
	require.True(t, ok)
}

func TestSQLDefaultEngineSetting(t *testing.T) {
	// https://github.com/tarantool/tarantool/blob/680990a082374e4790539215f69d9e9ee39c3307/test/sql/engine.test.lua
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Set default SQL "CREATE TABLE" engine to "vinyl".
	resp, err = conn.Do(NewSQLDefaultEngineSetRequest("vinyl")).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.EqualValues(t, []interface{}{[]interface{}{"sql_default_engine", "vinyl"}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLDefaultEngineGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "vinyl"}}, resp.Data)

	// Create a space with "CREATE TABLE".
	resp, err = conn.Execute("CREATE TABLE t1_vinyl(a INT PRIMARY KEY, b INT, c INT);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Check new space engine.
	resp, err = conn.Eval("return box.space['T1_VINYL'].engine", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "vinyl", resp.Data[0])

	// Set default SQL "CREATE TABLE" engine to "memtx".
	resp, err = conn.Do(NewSQLDefaultEngineSetRequest("memtx")).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "memtx"}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLDefaultEngineGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "memtx"}}, resp.Data)

	// Create a space with "CREATE TABLE".
	resp, err = conn.Execute("CREATE TABLE t2_memtx(a INT PRIMARY KEY, b INT, c INT);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Check new space engine.
	resp, err = conn.Eval("return box.space['T2_MEMTX'].engine", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "memtx", resp.Data[0])
}

func TestSQLDeferForeignKeysSetting(t *testing.T) {
	// https://github.com/tarantool/tarantool/blob/eafadc13425f14446d7aaa49dea67dfc1d5f45e9/test/sql/transitive-transactions.result
	skipIfSQLDeferForeignKeysSettingUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Create a parent space.
	resp, err = conn.Execute("CREATE TABLE parent(id INT PRIMARY KEY, y INT UNIQUE);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Create a space with reference to the parent space.
	resp, err = conn.Execute("CREATE TABLE child(id INT PRIMARY KEY, x INT REFERENCES parent(y));", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

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
	resp, err = conn.Do(NewSQLDeferForeignKeysSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLDeferForeignKeysGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", false}}, resp.Data)

	// Evaluate a scenario when foreign key not exists
	// on INSERT, but exists on commit.
	_, err = conn.Eval(deferEval, []interface{}{})
	require.NotNil(t, err)
	require.ErrorContains(t, err, "Failed to execute SQL statement: FOREIGN KEY constraint failed")

	resp, err = conn.Do(NewSQLDeferForeignKeysSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLDeferForeignKeysGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_defer_foreign_keys", true}}, resp.Data)

	// Evaluate a scenario when foreign key not exists
	// on INSERT, but exists on commit.
	resp, err = conn.Eval(deferEval, []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, true, resp.Data[0])
}

func TestSQLFullColumnNamesSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Create a space.
	resp, err = conn.Execute("CREATE TABLE fkname(id INT PRIMARY KEY, x INT);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Fill it with some data.
	resp, err = conn.Execute("INSERT INTO fkname VALUES (1, 1);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Disable displaying full column names in metadata.
	resp, err = conn.Do(NewSQLFullColumnNamesSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLFullColumnNamesGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", false}}, resp.Data)

	// Get a data with short column names in metadata.
	resp, err = conn.Execute("SELECT x FROM fkname WHERE id = 1;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "X", resp.MetaData[0].FieldName)

	// Enable displaying full column names in metadata.
	resp, err = conn.Do(NewSQLFullColumnNamesSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLFullColumnNamesGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", true}}, resp.Data)

	// Get a data with full column names in metadata.
	resp, err = conn.Execute("SELECT x FROM fkname WHERE id = 1;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "FKNAME.X", resp.MetaData[0].FieldName)
}

func TestSQLFullMetadataSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Create a space.
	resp, err = conn.Execute("CREATE TABLE fmt(id INT PRIMARY KEY, x INT);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Fill it with some data.
	resp, err = conn.Execute("INSERT INTO fmt VALUES (1, 1);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Disable displaying additional fields in metadata.
	resp, err = conn.Do(NewSQLFullMetadataSetRequest(false)).Get()
	require.Nil(t, err)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLFullMetadataGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", false}}, resp.Data)

	// Get a data without additional fields in metadata.
	resp, err = conn.Execute("SELECT x FROM fmt WHERE id = 1;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "", resp.MetaData[0].FieldSpan)

	// Enable displaying full column names in metadata.
	resp, err = conn.Do(NewSQLFullMetadataSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLFullMetadataGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_metadata", true}}, resp.Data)

	// Get a data with additional fields in metadata.
	resp, err = conn.Execute("SELECT x FROM fmt WHERE id = 1;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, "x", resp.MetaData[0].FieldSpan)
}

func TestSQLParserDebugSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Disable parser debug mode.
	resp, err = conn.Do(NewSQLParserDebugSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLParserDebugGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", false}}, resp.Data)

	// Enable parser debug mode.
	resp, err = conn.Do(NewSQLParserDebugSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLParserDebugGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_parser_debug", true}}, resp.Data)

	// To test real effect we need a Tarantool instance built with
	// `-DCMAKE_BUILD_TYPE=Debug`.
}

func TestSQLRecursiveTriggersSetting(t *testing.T) {
	// https://github.com/tarantool/tarantool/blob/d11fb3061e15faf4e0eb5375fb8056b4e64348ae/test/sql-tap/triggerC.test.lua
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Create a space.
	resp, err = conn.Execute("CREATE TABLE rec(id INTEGER PRIMARY KEY, a INT, b INT);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Fill it with some data.
	resp, err = conn.Execute("INSERT INTO rec VALUES(1, 1, 2);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Create a recursive trigger (with infinite depth).
	resp, err = conn.Execute(`
		CREATE TRIGGER tr12 AFTER UPDATE ON rec FOR EACH ROW BEGIN
          UPDATE rec SET a=new.a+1, b=new.b+1;
        END;`, []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Enable SQL recursive triggers.
	resp, err = conn.Do(NewSQLRecursiveTriggersSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLRecursiveTriggersGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", true}}, resp.Data)

	// Trigger the recursion.
	_, err = conn.Execute("UPDATE rec SET a=a+1, b=b+1;", []interface{}{})
	require.NotNil(t, err)
	require.ErrorContains(t, err, "Failed to execute SQL statement: too many levels of trigger recursion")

	// Disable SQL recursive triggers.
	resp, err = conn.Do(NewSQLRecursiveTriggersSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLRecursiveTriggersGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_recursive_triggers", false}}, resp.Data)

	// Trigger the recursion.
	resp, err = conn.Execute("UPDATE rec SET a=a+1, b=b+1;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)
}

func TestSQLReverseUnorderedSelectsSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Create a space.
	resp, err = conn.Execute("CREATE TABLE data(id STRING PRIMARY KEY);", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Fill it with some data.
	resp, err = conn.Execute("INSERT INTO data VALUES('1');", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	resp, err = conn.Execute("INSERT INTO data VALUES('2');", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(1), resp.SQLInfo.AffectedCount)

	// Disable reverse order in unordered selects.
	resp, err = conn.Do(NewSQLReverseUnorderedSelectsSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLReverseUnorderedSelectsGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", false}}, resp.Data)

	// Select multiple records.
	resp, err = conn.Execute("SELECT * FROM data;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.EqualValues(t, []interface{}{"1"}, resp.Data[0])
	require.EqualValues(t, []interface{}{"2"}, resp.Data[1])

	// Enable reverse order in unordered selects.
	resp, err = conn.Do(NewSQLReverseUnorderedSelectsSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLReverseUnorderedSelectsGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_reverse_unordered_selects", true}}, resp.Data)

	// Select multiple records.
	resp, err = conn.Execute("SELECT * FROM data;", []interface{}{})
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.EqualValues(t, []interface{}{"2"}, resp.Data[0])
	require.EqualValues(t, []interface{}{"1"}, resp.Data[1])
}

func TestSQLSelectDebugSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Disable select debug mode.
	resp, err = conn.Do(NewSQLSelectDebugSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLSelectDebugGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", false}}, resp.Data)

	// Enable select debug mode.
	resp, err = conn.Do(NewSQLSelectDebugSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLSelectDebugGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_select_debug", true}}, resp.Data)

	// To test real effect we need a Tarantool instance built with
	// `-DCMAKE_BUILD_TYPE=Debug`.
}

func TestSQLVDBEDebugSetting(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Disable VDBE debug mode.
	resp, err = conn.Do(NewSQLVDBEDebugSetRequest(false)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", false}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLVDBEDebugGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", false}}, resp.Data)

	// Enable VDBE debug mode.
	resp, err = conn.Do(NewSQLVDBEDebugSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", true}}, resp.Data)

	// Fetch current setting value.
	resp, err = conn.Do(NewSQLVDBEDebugGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_vdbe_debug", true}}, resp.Data)

	// To test real effect we need a Tarantool instance built with
	// `-DCMAKE_BUILD_TYPE=Debug`.
}

func TestSessionSettings(t *testing.T) {
	skipIfSettingsUnsupported(t)

	var resp *tarantool.Response
	var err error

	conn := test_helpers.ConnectWithValidation(t, server, opts)
	defer conn.Close()

	// Set some settings values.
	resp, err = conn.Do(NewSQLDefaultEngineSetRequest("memtx")).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_default_engine", "memtx"}}, resp.Data)

	resp, err = conn.Do(NewSQLFullColumnNamesSetRequest(true)).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Equal(t, []interface{}{[]interface{}{"sql_full_column_names", true}}, resp.Data)

	// Fetch current settings values.
	resp, err = conn.Do(NewSessionSettingsGetRequest()).Get()
	require.Nil(t, err)
	require.NotNil(t, resp)
	require.Subset(t, resp.Data,
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
		InitScript:   "testdata/config.lua",
		Listen:       server,
		User:         opts.User,
		Pass:         opts.Pass,
		WaitStart:    100 * time.Millisecond,
		ConnectRetry: 3,
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
