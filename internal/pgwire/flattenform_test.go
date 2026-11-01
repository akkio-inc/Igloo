package pgwire

import (
	"encoding/json"
	"testing"
	"time"
)

// TestFlattenForms checks the form each statement shape's FLATTENs take:
// the estimate's for the first statement, asked for once; the other for
// the next; either while neither has ended; and from then on the one whose
// latest runs took the less CPU time at best.
func TestFlattenForms(t *testing.T) {
	var f flattenForms
	asked := 0
	estimate := func() bool { asked++; return true }
	for i, want := range []bool{true, false, true} {
		if got := f.choose("a", estimate); got != want {
			t.Errorf("statement %d: lists %v, want %v", i, got, want)
		}
	}
	if asked != 1 {
		t.Errorf("asked DuckDB %d times", asked)
	}
	f.observe("a", false, 2*time.Second)
	if f.choose("a", estimate) {
		t.Error("with only the lateral form's run ended, not the lateral form")
	}
	f.observe("a", true, 5*time.Second)
	if f.choose("a", estimate) {
		t.Error("the slower form")
	}
	f.observe("a", true, time.Second)
	if !f.choose("a", estimate) {
		t.Error("not the form that became faster")
	}
	// A run beside others takes longer: it changes nothing until the faster
	// run is no longer among the latest.
	f.observe("a", true, 5*time.Second)
	if !f.choose("a", estimate) {
		t.Error("a slower run of the faster form moved the shape off it")
	}
	for range formRuns {
		f.observe("a", true, 5*time.Second)
	}
	if f.choose("a", estimate) {
		t.Error("the form that became slower")
	}
	f.observe("unseen", true, time.Second)
	if f.choose("b", func() bool { return false }) || !f.choose("b", estimate) {
		t.Error("another shape: not the estimate's, then the other form")
	}
}

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
