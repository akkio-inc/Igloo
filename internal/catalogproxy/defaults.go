package catalogproxy

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"

	"github.com/akkio-inc/igloo/internal/filerewrite"
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
// inserts write from the table's bytes a row (see package maintenance). But
// maintenance can do that only once the table has rows, and the statement
// that loads a table writes most of the rows it will ever have, which then
// stay in DuckDB's 122,880 rows a row group, a request each for a cold scan,
// as files of that size are not compacted. So a table starts with row groups
// of firstRowGroups rows, before anyone knows its bytes a row.
const (
	targetFileSizeProperty = "write.target-file-size-bytes"
	targetFileSize         = "536870912"
	rowGroupsProperty      = "igloo.row-groups"
	rowGroupsAuto          = "auto"
	rowGroupSizeProperty   = "write.parquet.row-group-size"
)

// firstRowGroupRows and firstRowGroupMemory bound the rows of a new table's
// row groups. A type's width says little about its bytes in Parquet, 2.5 to
// 43 times fewer in the tables tried, so the rows are flat rather than aimed
// at compaction's 16 MiB from the schema: half a million made 4 to 26 MiB of
// those tables, and 1.5 MiB of one of 3 bytes a row. Over copies of TPC-H at
// scale factor 100, a cold pass took 115 s where DuckDB's row groups took
// 197 s, warm passes within 2% and inserts as long. DuckDB holds a row group,
// uncompressed, on each thread while an insert writes, so wide rows get
// fewer: at most firstRowGroupMemory a thread as filerewrite.RowWidth counts
// them, against 33 MiB for DuckDB's row groups of TPC-H's lineitem.
const (
	firstRowGroupRows   = 1 << 19
	firstRowGroupMemory = 128 << 20
)

// firstRowGroups returns the rows of a row group a new table of the schema
// and partition spec given, as a catalog request spells them, starts with,
// or 0 if it should start with DuckDB's: if that is as many, if either
// cannot be read, or if the table is partitioned, as an insert holds a row
// group of every partition open.
func firstRowGroups(schema, spec json.RawMessage) int64 {
	if len(schema) == 0 {
		return 0
	}
	var s iceberg.Schema
	if json.Unmarshal(schema, &s) != nil {
		return 0
	}
	if len(spec) > 0 {
		var parts struct {
			Fields []json.RawMessage `json:"fields"`
		}
		if json.Unmarshal(spec, &parts) != nil || len(parts.Fields) > 0 {
			return 0
		}
	}
	width := filerewrite.RowWidth(s.Fields())
	if width <= 0 {
		return 0
	}
	rows := min(firstRowGroupRows, firstRowGroupMemory/width)
	if rows <= filerewrite.DefaultRowGroupRows {
		return 0
	}
	return rows
}

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
		if !addDefaults(props, firstRowGroups(req["schema"], req["partition-spec"])) {
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
	var schema, spec json.RawMessage
	at := -1
	for i, u := range updates {
		var action string
		if json.Unmarshal(u["action"], &action) != nil {
			return nil, false
		}
		switch action {
		case "add-schema":
			schema = u["schema"]
		case "add-spec":
			spec = u["spec"]
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
	addDefaults(last, firstRowGroups(schema, spec))
	raw, err := json.Marshal(last)
	if err != nil {
		return nil, false
	}
	updates[at]["updates"] = raw
	return remarshal(change, ch, "updates", updates), true
}

// hasDefaults reports whether props sets all of Igloo's default properties.
func hasDefaults(props map[string]string) bool {
	_, size := props[targetFileSizeProperty]
	_, groups := props[rowGroupsProperty]
	return size && groups
}

// addDefaults adds Igloo's default properties that props lacks to it, and
// reports whether there were any. firstRows is the size of the table's row
// groups to start with, 0 for DuckDB's; a size the statement set stays, and
// the marker without a size says it is not one of Igloo's, which maintenance
// then leaves alone.
func addDefaults(props map[string]string, firstRows int64) bool {
	added := false
	if _, ok := props[targetFileSizeProperty]; !ok {
		props[targetFileSizeProperty] = targetFileSize
		added = true
	}
	if _, ok := props[rowGroupsProperty]; !ok {
		props[rowGroupsProperty] = rowGroupsAuto
		if _, set := props[rowGroupSizeProperty]; !set && firstRows > 0 {
			rows := strconv.FormatInt(firstRows, 10)
			props[rowGroupSizeProperty] = rows
			props[rowGroupsProperty] = rowGroupsAuto + ":" + rows
		}
		added = true
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
