package pgwire

import (
	"encoding/json"
	"testing"
)

// TestDelimRows checks the rows DuckDB's plan estimates reach its dependent
// joins, against the rows of the most rows' table under each.
func TestDelimRows(t *testing.T) {
	var plan any
	if err := json.Unmarshal([]byte(`[{"name": "PROJECTION", "children": [
		{"name": "LEFT_DELIM_JOIN", "extra_info": {"Estimated Cardinality": "1200000"}, "children": [
			{"name": "HASH_JOIN", "extra_info": {"Estimated Cardinality": "1700000"}, "children": [
				{"name": "TABLE_SCAN", "extra_info": {"Estimated Cardinality": "40000000"}, "children": []},
				{"name": "TABLE_SCAN", "extra_info": {"Estimated Cardinality": "420000"}, "children": []}]},
			{"name": "UNNEST", "children": [{"name": "DELIM_SCAN", "children": []}]}]},
		{"name": "RIGHT_DELIM_JOIN", "children": [
			{"name": "HASH_JOIN", "extra_info": {"Estimated Cardinality": "38000000"}, "children": [
				{"name": "TABLE_SCAN", "extra_info": {"Estimated Cardinality": "40000000"}, "children": []}]}]}]}]`), &plan); err != nil {
		t.Fatal(err)
	}
	if reach, tables := delimRows(plan); reach != 39_700_000 || tables != 80_000_000 {
		t.Errorf("delimRows = %d, %d", reach, tables)
	}
	if reach, tables := delimRows(map[string]any{"name": "HASH_JOIN"}); reach != 0 || tables != 0 {
		t.Errorf("without dependent joins: %d, %d", reach, tables)
	}
}
