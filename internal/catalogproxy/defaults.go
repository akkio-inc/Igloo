package catalogproxy

import (
	"encoding/json"
	"strings"
)

// Igloo gives each table its SQL creates a target data file size, unless
// the statement set one. DuckDB's Iceberg writer otherwise aims for 8 MiB
// and closes a file at the first row group past that, so an insert writes
// a file per row group: 17 files of 45 MB for two million rows of thirteen
// columns, and files of 4 MB for narrower rows. Every file costs a request
// to read and an entry in the table's manifests, and compaction, which
// rewrites them later, gets through 8 GB a table an hour. 512 MiB is the
// Iceberg spec's default, the size compaction makes files, and, as
// workers get whole files, a split as fine as their gigabyte each needs.
//
// It also marks the table as Igloo's to size the row groups of
// (rowGroupsProperty): table maintenance sets the row group size DuckDB's
// inserts write from the table's bytes a row (see package maintenance).
const (
	targetFileSizeProperty = "write.target-file-size-bytes"
	targetFileSize         = "536870912"
	rowGroupsProperty      = "igloo.row-groups"
	rowGroupsAuto          = "auto"
)

// defaults are the properties Igloo gives each table its SQL creates.
var defaults = map[string]string{targetFileSizeProperty: targetFileSize, rowGroupsProperty: rowGroupsAuto}

// withDefaults returns body, a change DuckDB asked of the catalog at rest,
// with Igloo's default properties added to each table it creates: in the
// create request, and in the commit that asserts the table does not exist
// yet (DuckDB stages a create, then commits it). Properties the statement
// set stay as they are. Any other change, or a body it cannot read, comes
// back unchanged, as does one that already has the defaults, so a change
// DuckDB sends again still joins the first.
func withDefaults(rest string, body []byte) []byte {
	switch {
	case createTablePath.MatchString(rest):
		var req map[string]json.RawMessage
		if json.Unmarshal(body, &req) != nil {
			return body
		}
		props := map[string]string{}
		if raw, ok := req["properties"]; ok && json.Unmarshal(raw, &props) != nil {
			return body
		}
		if !addDefaults(props) {
			return body
		}
		return remarshal(body, req, "properties", props)
	case commitPath.MatchString(rest):
		if strings.Contains(rest, "transactions/commit") {
			var tx map[string]json.RawMessage
			var changes []json.RawMessage
			if json.Unmarshal(body, &tx) != nil || json.Unmarshal(tx["table-changes"], &changes) != nil {
				return body
			}
			changed := false
			for i, ch := range changes {
				if out, ok := createDefaults(ch); ok {
					changes[i], changed = out, true
				}
			}
			if !changed {
				return body
			}
			return remarshal(body, tx, "table-changes", changes)
		}
		if out, ok := createDefaults(body); ok {
			return out
		}
	}
	return body
}

// createDefaults adds Igloo's default properties to one table's change if
// it creates the table, to its set-properties update or a new one. It
// reports whether it changed anything.
func createDefaults(change json.RawMessage) (json.RawMessage, bool) {
	var ch map[string]json.RawMessage
	if json.Unmarshal(change, &ch) != nil {
		return nil, false
	}
	var reqs []struct {
		Type string `json:"type"`
	}
	var updates []map[string]json.RawMessage
	if json.Unmarshal(ch["requirements"], &reqs) != nil || json.Unmarshal(ch["updates"], &updates) != nil {
		return nil, false
	}
	creates := false
	for _, r := range reqs {
		creates = creates || r.Type == "assert-create"
	}
	if !creates {
		return nil, false
	}
	// The defaults go in the last set-properties update, unless one sets
	// them already.
	var last map[string]string
	at := -1
	for i, u := range updates {
		var action string
		if json.Unmarshal(u["action"], &action) != nil {
			return nil, false
		}
		if action != "set-properties" {
			continue
		}
		set := map[string]string{}
		if raw, ok := u["updates"]; ok && json.Unmarshal(raw, &set) != nil {
			return nil, false
		}
		if hasDefaults(set) {
			return nil, false
		}
		last, at = set, i
	}
	if at < 0 {
		updates = append(updates, map[string]json.RawMessage{"action": json.RawMessage(`"set-properties"`)})
		last, at = map[string]string{}, len(updates)-1
	}
	addDefaults(last)
	raw, err := json.Marshal(last)
	if err != nil {
		return nil, false
	}
	updates[at]["updates"] = raw
	return remarshal(change, ch, "updates", updates), true
}

// hasDefaults reports whether props sets all of Igloo's default properties.
func hasDefaults(props map[string]string) bool {
	for k := range defaults {
		if _, ok := props[k]; !ok {
			return false
		}
	}
	return true
}

// addDefaults adds Igloo's default properties that props lacks to it, and
// reports whether there were any.
func addDefaults(props map[string]string) bool {
	added := false
	for k, v := range defaults {
		if _, ok := props[k]; !ok {
			props[k] = v
			added = true
		}
	}
	return added
}

// remarshal returns obj, read from body, with its key set to v, or body if
// that cannot be encoded.
func remarshal(body []byte, obj map[string]json.RawMessage, key string, v any) []byte {
	raw, err := json.Marshal(v)
	if err != nil {
		return body
	}
	obj[key] = raw
	out, err := json.Marshal(obj)
	if err != nil {
		return body
	}
	return out
}
