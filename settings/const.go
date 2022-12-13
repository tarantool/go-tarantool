package settings

const sessionSettingsSpace string = "_session_settings"

// In Go and IPROTO_UPDATE count starts with 0.
const sessionSettingValueField int = 1

const (
	errorMarshalingEnabled     string = "error_marshaling_enabled"
	sqlDefaultEngine           string = "sql_default_engine"
	sqlDeferForeignKeys        string = "sql_defer_foreign_keys"
	sqlFullColumnNames         string = "sql_full_column_names"
	sqlFullMetadata            string = "sql_full_metadata"
	sqlParserDebug             string = "sql_parser_debug"
	sqlRecursiveTriggers       string = "sql_recursive_triggers"
	sqlReverseUnorderedSelects string = "sql_reverse_unordered_selects"
	sqlSelectDebug             string = "sql_select_debug"
	sqlVDBEDebug               string = "sql_vdbe_debug"
)

const selectAllLimit uint32 = 1000
