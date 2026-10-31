package erasure

import (
	"context"
	"regexp"
	"slices"
	"strings"

	"github.com/akkio-inc/igloo/internal/tasks"
)

// maxHops bounds how far lineage follows tables written from tables.
const maxHops = 3

// writeTarget matches the start of a statement that writes a table, and
// the table's name.
var writeTarget = regexp.MustCompile(`(?is)^\s*(?:INSERT\s+(?:OR\s+\w+\s+)?INTO|MERGE\s+INTO|` +
	`CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|MATERIALIZED\s+VIEW)(?:\s+IF\s+NOT\s+EXISTS)?)\s+` +
	`((?:"(?:[^"]|"")+"|[A-Za-z_][\w$]*)(?:\s*\.\s*(?:"(?:[^"]|"")+"|[A-Za-z_][\w$]*)){0,2})`)

var namePart = regexp.MustCompile(`"(?:[^"]|"")+"|[A-Za-z_][\w$]*`)

// downstream finds the tables written from r's tables, and from those, as
// far as the query history and the definitions of materialized views and
// tasks tell. Statements Igloo checked against grants record what they
// read; for others, naming a table counts.
func (s *Service) downstream(ctx context.Context, r *Request) ([]Downstream, error) {
	ours := map[string]bool{}
	var frontier []string
	for _, t := range r.Tables {
		if t.Remote {
			continue
		}
		ours[strings.ToLower(t.Name)] = true
		frontier = append(frontier, t.Name)
	}
	found := map[string]*Downstream{}
	var order []string
	for hop := 0; hop < maxHops && len(frontier) > 0; hop++ {
		var next []string
		add := func(target string, from []string, via string) {
			// A statement that names the table it writes, as all do,
			// does not make the table its own source.
			from = slices.DeleteFunc(slices.Clone(from), func(f string) bool { return strings.EqualFold(f, target) })
			key := strings.ToLower(target)
			if target == "" || len(from) == 0 {
				return
			}
			if d := found[key]; d != nil {
				for _, f := range from {
					if !slices.Contains(d.From, f) {
						d.From = append(d.From, f)
					}
				}
				return
			}
			found[key] = &Downstream{Table: target, From: from, Via: via, Erased: ours[key]}
			order = append(order, key)
			next = append(next, target)
		}
		if s.opts.Lineage != nil {
			full := make([]string, len(frontier))
			short := make([]string, len(frontier))
			for i, t := range frontier {
				full[i] = s.opts.CatalogName + "." + t
				short[i] = shortName(t)
			}
			writes, err := s.opts.Lineage.Writes(ctx, full, short)
			if err != nil {
				return nil, err
			}
			for _, q := range writes {
				var from []string
				for i, t := range frontier {
					if slices.ContainsFunc(q.TablesRead, func(read string) bool { return strings.EqualFold(read, full[i]) }) ||
						q.TablesRead == nil && names(q.Text, t) {
						from = append(from, t)
					}
				}
				targets := []string{s.target(q.Text)}
				if q.TablesWritten != nil {
					targets = targets[:0]
					for _, w := range q.TablesWritten {
						targets = append(targets, strings.TrimPrefix(w, s.opts.CatalogName+"."))
					}
				}
				for _, target := range targets {
					add(target, from, "query history")
				}
			}
		}
		if s.opts.Jobs != nil {
			for _, k := range []struct{ kind, via string }{{tasks.KindMaterializedView, "materialized view"}, {tasks.KindDynamicTable, "dynamic table"}, {tasks.KindTask, "task"}} {
				kind, via := k.kind, k.via
				jobs, err := s.opts.Jobs.Store().Jobs(ctx, kind)
				if err != nil {
					return nil, err
				}
				for _, j := range jobs {
					var from []string
					for _, t := range frontier {
						if names(j.Statement, t) {
							from = append(from, t)
						}
					}
					target := j.Name
					if kind == tasks.KindTask {
						target = s.target(j.Statement)
					}
					add(target, from, via)
				}
			}
		}
		frontier = next
	}
	out := make([]Downstream, len(order))
	for i, key := range order {
		out[i] = *found[key]
	}
	return out, nil
}

// target is the catalog table, as schema.table, that a statement writes,
// or "". Unqualified names are taken to be in the default schema.
func (s *Service) target(stmt string) string {
	m := writeTarget.FindStringSubmatch(stmt)
	if m == nil {
		return ""
	}
	var parts []string
	for _, p := range namePart.FindAllString(m[1], -1) {
		if strings.HasPrefix(p, `"`) {
			parts = append(parts, strings.ReplaceAll(p[1:len(p)-1], `""`, `"`))
		} else {
			parts = append(parts, strings.ToLower(p))
		}
	}
	switch len(parts) {
	case 1:
		return s.opts.Schema + "." + parts[0]
	case 2:
		if slices.Contains([]string{"temp", "system", "memory", "main"}, strings.ToLower(parts[0])) {
			return ""
		}
		return parts[0] + "." + parts[1]
	case 3:
		if strings.EqualFold(parts[0], s.opts.CatalogName) {
			return parts[1] + "." + parts[2]
		}
	}
	return ""
}

// names reports whether text names table, schema.table, by its name.
func names(text, table string) bool {
	return regexp.MustCompile(`(?i)(^|[^\w$])` + regexp.QuoteMeta(shortName(table)) + `($|[^\w$])`).MatchString(text)
}

// shortName is a table's name without its namespace.
func shortName(table string) string { return table[strings.LastIndex(table, ".")+1:] }
