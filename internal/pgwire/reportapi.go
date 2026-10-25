package pgwire

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/sqllex"
)

// The report API (docs/media-analytics.md#report-api), version 1: counts of
// the people of labelled cohorts in each value of dimensions of a table,
// which audience reports otherwise write as SQL that joins the cohorts to
// the table, expands each row into its dimensions and counts distinct
// people per value.
//
//	SELECT * FROM igloo.audience_profile('<spec>')
//	SELECT igloo.audience_profile_sql('<spec>')
//	SELECT igloo.report_api_versions()
//	SELECT * FROM igloo.audience_profile_check('<spec>', '<sql>')
//
// A call is replaced by the SQL it stands for (reportSQL) before the
// statement is checked or prepared (reportCalls, at the top of
// prepareChecked), so the statement that runs is that SQL as far as grants,
// policies, errors and execution go: no check of the API's own decides who
// may read what. The SQL depends on the spec alone, so the statement's text,
// and its plan key, are the same for the same spec.

const (
	reportAPIVersion  = 1
	reportMaxSpec     = 64 << 10
	reportMaxCohorts  = 8
	reportMaxDims     = 64
	reportMaxMetrics  = 16
	reportCTE         = `"__igloo_report_`
	reportCompareCTE  = `"__igloo_report_check_`
	reportAPIFunction = "igloo.audience_profile"
)

// reportCall finds a call of the report API in SQL code (string literals,
// quoted names and comments blanked out). The name is written unquoted, as
// igloo.<function>, and not as a part of a longer name.
var reportCall = regexp.MustCompile(`(?i)(^|[^\w.$])(igloo\s*\.\s*(audience_profile_check|audience_profile_sql|audience_profile|report_api_versions))\s*\(`)

// reportError is a spec the API does not accept: SQLSTATE 22023.
func reportError(format string, args ...any) error {
	return pgerror.WithCode(fmt.Errorf("igloo.audience_profile: "+format, args...), codes.InvalidParameterValue)
}

// reportCalls returns text with each call of the report API replaced by
// what it stands for, and the member tables whose ids it counts through, at
// the snapshots it reads them at (see reportIDs).
func (s *Server) reportCalls(ctx context.Context, sess *session, text string) (string, []idPin, []indexPin, error) {
	return s.expandReportCalls(ctx, sess, text, false)
}

// reportWouldIndex reports whether bitmap indexes would now answer a call
// in text, without counting it.
func (s *Server) reportWouldIndex(ctx context.Context, sess *session, text string) bool {
	_, _, pins, err := s.expandReportCalls(ctx, sess, text, true)
	return err == nil && len(pins) > 0
}

// expandReportCalls is reportCalls; probe leaves the counts of calls by
// path alone.
func (s *Server) expandReportCalls(ctx context.Context, sess *session, text string, probe bool) (string, []idPin, []indexPin, error) {
	lower := strings.ToLower(text)
	if !strings.Contains(lower, "igloo") || !strings.Contains(lower, "audience_profile") && !strings.Contains(lower, "report_api_versions") {
		return text, nil, nil, nil
	}
	var pins []idPin
	var indexPins []indexPin
	masked := maskCode(text)
	var edits []edit
	// The same call again in the statement, as a report of two blocks
	// repeats its calls, is the same text, written once: which the
	// statement then computes once (see shareSubqueries).
	type written struct {
		q    string
		ip   []indexPin
		read []idPin
	}
	calls := map[string]written{}
	for _, m := range reportCall.FindAllStringSubmatchIndex(masked, -1) {
		start, open := m[4], m[1]-1
		fn := strings.ToLower(masked[m[6]:m[7]])
		end := matchParen(text, open)
		if end < 0 {
			return "", nil, nil, reportError("%s has no closing parenthesis", fn)
		}
		args, err := literalArgs(text[open+1 : end])
		if err != nil {
			return "", nil, nil, reportError("igloo.%s takes string literals: %v", fn, err)
		}
		var out string
		switch fn {
		case "report_api_versions":
			if len(args) != 0 {
				return "", nil, nil, reportError("igloo.report_api_versions takes no arguments")
			}
			out = "CAST([" + strconv.Itoa(reportAPIVersion) + "] AS INTEGER[])"
		case "audience_profile", "audience_profile_sql":
			if len(args) != 1 {
				return "", nil, nil, reportError("igloo.%s takes one argument, the spec", fn)
			}
			spec, err := parseReportSpec(ctx, sess, args[0])
			if err != nil {
				return "", nil, nil, err
			}
			if fn == "audience_profile" {
				w, seen := calls[args[0]]
				if !seen {
					w.q, w.ip, w.read = s.reportStatement(ctx, sess, spec, probe)
					calls[args[0]] = w
					pins, indexPins = append(pins, w.read...), append(indexPins, w.ip...)
				}
				out = "(" + w.q + ")"
			} else {
				out = quoteLiteral(spec.sql())
			}
		case "audience_profile_check":
			if len(args) != 2 {
				return "", nil, nil, reportError("igloo.audience_profile_check takes two arguments, the spec and the SQL to compare")
			}
			spec, err := parseReportSpec(ctx, sess, args[0])
			if err != nil {
				return "", nil, nil, err
			}
			if err := reportOwnNames(args[1]); err != nil {
				return "", nil, nil, err
			}
			api, ip, read := s.reportStatement(ctx, sess, spec, probe)
			pins, indexPins = append(pins, read...), append(indexPins, ip...)
			q, err := spec.checkSQL(ctx, sess, args[1], api)
			if err != nil {
				return "", nil, nil, err
			}
			out = "(" + q + ")"
		}
		stop := end + 1
		if fn == "audience_profile" || fn == "audience_profile_check" {
			// Snowflake's form of a table function, TABLE(f(...)), which
			// its dialect's translation leaves as it is: the whole of it.
			prev := func(i int) int {
				for i >= 0 && isSpace(masked[i]) {
					i--
				}
				return i
			}
			if o := prev(start - 1); o >= 0 && masked[o] == '(' {
				w := prev(o - 1)
				if w >= 4 && strings.EqualFold(masked[w-4:w+1], "table") && (w < 5 || !isIdentChar(masked[w-5])) {
					if c := matchParen(text, o); c > end && strings.TrimSpace(masked[end+1:c]) == "" {
						start, stop = w-4, c+1
					}
				}
			}
		}
		edits = append(edits, edit{start, stop, out})
	}
	out, err := applyEdits(text, edits)
	return out, pins, indexPins, err
}

// reportStatement returns the statement that answers spec's call: from
// bitmap indexes where they can (reportindex.go), else from the tables,
// through ids where it may (reportids.go); and what it pinned.
func (s *Server) reportStatement(ctx context.Context, sess *session, spec *reportSpec, probe bool) (string, []indexPin, []idPin) {
	var plan *reportIndexPlan
	if spec.wholeFromIndexes() {
		plan = s.reportIndexes(ctx, sess, spec)
	}
	if plan != nil && !plan.hybrid {
		// All from the bitmaps.
		if !probe {
			s.reportFromIndexes.Add(1)
			s.metrics.reportCall("index")
		}
		return s.reportIndexSQL(spec, plan), plan.pins, nil
	}
	ids, read := s.reportIDs(ctx, sess, spec)
	if ids != nil {
		ids.pairs, ids.split = s.pairBitmaps(ctx, sess, spec, ids.source), s.splitPairs
	}
	if s.preferHybrid {
		ids = nil
	}
	// Through ids, or of the cohorts' sizes alone: the cohorts from their
	// bitmaps, computed first, once for the statement however many calls
	// read them (their variables are named by what they hold). Through ids,
	// the scan joins each cohort's members, listed from its bitmap, as it
	// joins those of the cohort's table, and counts people itself: with the
	// scan there anyway, that costs less than reading the dimensions'
	// bitmaps for them, and a join on integer ids less than testing each
	// row against each bitmap (see the hybrid, below).
	if ids != nil || len(spec.Dimensions) == 0 {
		if cp := s.reportCohortIndexes(ctx, sess, spec); cp != nil {
			if probe {
				return spec.sqlWith(ids), cp.pins, read
			}
			err := s.hybridBitmaps(ctx, sess, spec, cp)
			if err == nil && ids == nil {
				err = s.hybridPeople(ctx, sess, spec, cp)
			}
			if err == nil {
				s.reportFromIndexes.Add(1)
				if ids == nil {
					s.metrics.reportCall("index")
					return s.reportCohortSQL(spec, cp), cp.pins, read
				}
				s.metrics.reportCall("index+scan")
				ids.cohortFrom = s.cohortMembers(spec, cp)
				return spec.sqlWith(ids), cp.pins, read
			}
			s.log.Debug("report call's cohorts read from their tables", "reason", err)
		}
	}
	if plan == nil && ids == nil && !spec.wholeFromIndexes() {
		plan = s.reportIndexes(ctx, sess, spec)
	}
	if plan != nil && ids == nil {
		// A hybrid, where the source cannot be counted through ids: people
		// from the bitmaps, the rest from a scan that tests each row
		// against the cohorts' bitmaps.
		if !probe {
			err := s.hybridBitmaps(ctx, sess, spec, plan)
			if err == nil {
				err = s.hybridPeople(ctx, sess, spec, plan)
			}
			if err != nil {
				s.log.Debug("report call counted from the tables", "reason", err)
				plan = nil
			}
		}
		if plan != nil {
			plan.pairs, plan.split = s.pairBitmaps(ctx, sess, spec, plan.source), s.splitPairs
			if !probe {
				s.reportFromIndexes.Add(1)
				s.metrics.reportCall("index+scan")
			}
			return s.reportIndexSQL(spec, plan), plan.pins, nil
		}
	}
	if !probe {
		if ids != nil {
			s.reportThroughIDs.Add(1)
			s.metrics.reportCall("ids")
		} else {
			s.metrics.reportCall("scan")
		}
	}
	return spec.sqlWith(ids), nil, read
}

// cohortMembers is, for each of spec's cohorts that plan reads from its
// bitmap, the SELECT of its members' ids from its variable, which the ids
// path joins in place of the cohort's table: under the conditions that the
// variable is there and its latest_from reads as it did (see cohortOf).
func (s *Server) cohortMembers(spec *reportSpec, plan *reportIndexPlan) []string {
	out := make([]string, len(spec.Cohorts))
	for i := range spec.Cohorts {
		out[i] = "SELECT unnest(rb_to_list(x.bm)) AS member FROM (" + s.cohortOf(spec, plan, i) + ") AS x"
	}
	return out
}

// reportPlannedPath returns the path a call of spec would take now, as
// reportStatement chooses it and the path column and metrics name it:
// "index", "index+scan", "ids" or "scan". It reads what choosing does (the
// indexes' state and the tables' snapshots, not their bitmaps), computes
// no bitmap and sets no variable. A call may still be counted from the
// tables when its bitmaps do not fit in memory.
func (s *Server) reportPlannedPath(ctx context.Context, sess *session, spec *reportSpec) string {
	var plan *reportIndexPlan
	if spec.wholeFromIndexes() {
		plan = s.reportIndexes(ctx, sess, spec)
	}
	if plan != nil && !plan.hybrid {
		return "index"
	}
	ids, _ := s.reportIDs(ctx, sess, spec)
	if s.preferHybrid {
		ids = nil
	}
	if ids != nil || len(spec.Dimensions) == 0 {
		if s.reportCohortIndexes(ctx, sess, spec) != nil {
			if len(spec.Dimensions) == 0 {
				return "index"
			}
			return "index+scan"
		}
	}
	if plan == nil && ids == nil && !spec.wholeFromIndexes() {
		plan = s.reportIndexes(ctx, sess, spec)
	}
	switch {
	case plan != nil && ids == nil:
		return "index+scan"
	case ids != nil:
		return "ids"
	}
	return "scan"
}

// (Pair counts are written as workers split them (bitmapPairCounts) only
// for tests, splitPairs, for now. The planner splits that form, but its
// workers keep a bitmap for each (value, cohort, day) of their share: on
// three workers of 8 GB, media signals at 10x (361M rows, 6,500 titles,
// a month of days) ran them out of memory, where one machine counts it in
// rb_count_distinct in about 105 s; and auto mode, from its estimates,
// chose the workers.)

// pairBitmaps returns the distinct_with metrics of spec whose pairs roaring
// bitmaps count, where the member is an ID domain's id: those whose column
// in the source (src, at its snapshot) is a date or an integer, which
// rb_count_distinct(id, key) keeps a bitmap of ids for each value of. They
// take a bitmap of each (key, value) group's members a day, say, where
// DuckDB's count(DISTINCT (member, day)) keeps every pair in a hash table:
// on a table of a hundred million rows of list elements, more memory than
// the machine has. None with bitmaps off, or after bitmaps did not fit in
// memory (see rerunWithoutBitmaps).
func (s *Server) pairBitmaps(ctx context.Context, sess *session, spec *reportSpec, src reportIDTable) map[int]bool {
	if !s.bitmaps || sess == nil || sess.conn == nil || sess.noBitmaps {
		return nil
	}
	var cols []string
	var which []int
	for i, m := range spec.Metrics {
		if m.DistinctWith != "" {
			cols, which = append(cols, "s."+quoteIdent(m.DistinctWith)), append(which, i)
		}
	}
	if len(cols) == 0 {
		return nil
	}
	defer sess.phase("report_pairs")()
	rows, err := queryRows(ctx, sess.conn, "DESCRIBE SELECT "+strings.Join(cols, ", ")+" FROM "+src.from+" AS s AT (VERSION => "+strconv.FormatInt(src.at, 10)+")")
	if err != nil || len(rows) != len(cols) {
		return nil
	}
	pairs := map[int]bool{}
	for i, r := range rows {
		if len(r) > 1 {
			if t, _ := r[1].(string); pairKeyTypes[t] {
				pairs[which[i]] = true
			}
		}
	}
	return pairs
}

// pairKeyTypes are the types of the keys rb_count_distinct(id, key) takes.
var pairKeyTypes = map[string]bool{
	"DATE": true, "TINYINT": true, "SMALLINT": true, "INTEGER": true, "BIGINT": true,
	"UTINYINT": true, "USMALLINT": true, "UINTEGER": true,
}

// reportOwnNames refuses SQL a caller gave the API that names what only
// Igloo's own text may: the columns of ids, and bitmap indexes' tables. The
// statement the API writes names them where it counts through ids, so these
// checks run on the caller's own parts of it.
func reportOwnNames(sql string) error {
	if err := hiddenReferences(sql); err != nil {
		return err
	}
	return indexReferences(sql)
}

// literalArgs returns the values of a call's arguments, which must be
// string literals ('...', E'...' or $$...$$), separated by commas.
func literalArgs(text string) ([]string, error) {
	var out []string
	commas := 0
	for _, seg := range sqllex.Segments(text) {
		switch seg.Kind {
		case sqllex.Comment:
		case sqllex.String:
			vals := textValues(seg.Text)
			if len(vals) != 1 || len(out) != commas {
				return nil, errors.New("arguments must be string literals separated by commas")
			}
			out = append(out, vals[0].value)
		case sqllex.Code:
			for _, c := range seg.Text {
				switch {
				case c == ',':
					commas++
				case c == ' ' || c == '\t' || c == '\n' || c == '\r':
				default:
					return nil, errors.New("arguments must be string literals")
				}
			}
		default:
			return nil, errors.New("arguments must be string literals")
		}
	}
	if len(out) > 0 && commas != len(out)-1 || len(out) == 0 && commas > 0 {
		return nil, errors.New("arguments must be string literals separated by commas")
	}
	return out, nil
}

// --- the spec ---------------------------------------------------------------

type reportSpec struct {
	Version    *int           `json:"version"`
	Source     *reportSource  `json:"source"`
	Cohorts    []reportCohort `json:"cohorts"`
	Dimensions []reportDim    `json:"dimensions"`
	Metrics    []reportMetric `json:"metrics"`
	Totals     bool           `json:"totals"`
	Extent     string         `json:"extent"`
}

type reportSource struct {
	Table  string     `json:"table"`
	Member string     `json:"member"`
	Where  conditions `json:"where"`
}

type reportCohort struct {
	Label   string         `json:"label"`
	Table   string         `json:"table"`
	Member  string         `json:"member"`
	Where   conditions     `json:"where"`
	Version *reportVersion `json:"version"`
	SQL     string         `json:"sql"`
}

type reportVersion struct {
	Column     string          `json:"column"`
	Equals     json.RawMessage `json:"equals"`
	LatestFrom *struct {
		Table string     `json:"table"`
		Where conditions `json:"where"`
	} `json:"latest_from"`
}

type reportDim struct {
	Name        string             `json:"name"`
	Column      string             `json:"column"`
	Elements    string             `json:"elements"`
	ElementText string             `json:"element_text"`
	Path        *string            `json:"path"`
	Constant    *string            `json:"constant"`
	Map         map[string]*string `json:"map"`
	Group       *string            `json:"group"`
	SkipBlank   bool               `json:"skip_blank"`
	Metric      string             `json:"metric"`
	MetricWhere json.RawMessage    `json:"metric_where"`
}

type reportMetric struct {
	Name         string          `json:"name"`
	Column       string          `json:"column"`
	Coalesce     json.RawMessage `json:"coalesce"`
	DistinctWith string          `json:"distinct_with"`
}

// conditions are a where: columns' conditions, all of which must hold.
type conditions map[string]json.RawMessage

// metricName is the name of a metric, which its columns' names start with.
var metricName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// reportFixed are the result's columns every call has.
var reportFixed = []string{"dimension", "dim_group", "value", "cohort", "is_total", "people", "rows", "cohort_size",
	"metric_sum", "metric_count", "metric_avg", "extent_min", "extent_max", "path"}

// parseReportSpec reads and checks a spec. The SQL of a cohort's sql must
// parse as one SELECT, which the session's connection tells without
// binding it.
func parseReportSpec(ctx context.Context, sess *session, text string) (*reportSpec, error) {
	if len(text) > reportMaxSpec {
		return nil, reportError("the spec takes %d bytes, more than the %d allowed", len(text), reportMaxSpec)
	}
	dec := json.NewDecoder(strings.NewReader(text))
	dec.DisallowUnknownFields()
	var spec reportSpec
	if err := dec.Decode(&spec); err != nil {
		return nil, reportError("the spec is not a JSON object this version reads: %s", strings.TrimPrefix(err.Error(), "json: "))
	}
	if dec.More() {
		return nil, reportError("the spec has more than one JSON value")
	}
	if spec.Version == nil {
		return nil, reportError("the spec needs \"version\": %d", reportAPIVersion)
	}
	if *spec.Version != reportAPIVersion {
		return nil, reportError("version %d is not supported (igloo.report_api_versions() lists those that are)", *spec.Version)
	}
	if err := spec.check(ctx, sess); err != nil {
		return nil, err
	}
	return &spec, nil
}

func (s *reportSpec) check(ctx context.Context, sess *session) error {
	if len(s.Cohorts) == 0 {
		return reportError("cohorts: at least one is needed")
	}
	if len(s.Cohorts) > reportMaxCohorts {
		return reportError("cohorts: %d, more than the %d allowed", len(s.Cohorts), reportMaxCohorts)
	}
	labels := map[string]bool{}
	for i, c := range s.Cohorts {
		at := fmt.Sprintf("cohorts[%d]", i)
		if c.Label == "" {
			return reportError("%s.label: needed", at)
		}
		if labels[c.Label] {
			return reportError("%s.label: %q is the label of another cohort", at, c.Label)
		}
		labels[c.Label] = true
		if (c.Table == "") == (c.SQL == "") {
			return reportError("%s: needs either table or sql", at)
		}
		if c.SQL != "" {
			if c.Member != "" || c.Where != nil || c.Version != nil {
				return reportError("%s: a cohort given as sql takes no member, where or version", at)
			}
			if err := reportOwnNames(c.SQL); err != nil {
				return err
			}
			if err := checkSelect(ctx, sess, c.SQL, 1); err != nil {
				return reportError("%s.sql: %v", at, err)
			}
			continue
		}
		if c.Member == "" {
			return reportError("%s.member: needed", at)
		}
		if err := checkName(c.Table); err != nil {
			return reportError("%s.table: %v", at, err)
		}
		if err := c.Where.check(at + ".where"); err != nil {
			return err
		}
		if v := c.Version; v != nil {
			if v.Column == "" {
				return reportError("%s.version.column: needed", at)
			}
			if (v.Equals == nil) == (v.LatestFrom == nil) {
				return reportError("%s.version: needs either equals or latest_from", at)
			}
			if v.Equals != nil {
				if _, err := sqlLiteral(v.Equals); err != nil {
					return reportError("%s.version.equals: %v", at, err)
				}
			} else {
				if err := checkName(v.LatestFrom.Table); err != nil {
					return reportError("%s.version.latest_from.table: %v", at, err)
				}
				if err := v.LatestFrom.Where.check(at + ".version.latest_from.where"); err != nil {
					return err
				}
			}
		}
	}
	if len(s.Dimensions) == 0 {
		if s.Source != nil || len(s.Metrics) > 0 || s.Totals || s.Extent != "" {
			return reportError("a spec without dimensions counts the cohorts alone: it takes no source, metrics, totals or extent")
		}
		return nil
	}
	if s.Source == nil {
		return reportError("source: needed with dimensions")
	}
	if err := checkName(s.Source.Table); err != nil {
		return reportError("source.table: %v", err)
	}
	if s.Source.Member == "" {
		return reportError("source.member: needed")
	}
	if err := s.Source.Where.check("source.where"); err != nil {
		return err
	}
	if len(s.Dimensions) > reportMaxDims {
		return reportError("dimensions: %d, more than the %d allowed", len(s.Dimensions), reportMaxDims)
	}
	for i, d := range s.Dimensions {
		at := fmt.Sprintf("dimensions[%d]", i)
		if d.Name == "" {
			return reportError("%s.name: needed", at)
		}
		n := 0
		for _, set := range []bool{d.Column != "", d.Elements != "", d.Constant != nil} {
			if set {
				n++
			}
		}
		if n != 1 {
			return reportError("%s: needs exactly one of column, elements and constant", at)
		}
		if d.Map != nil && d.Column == "" {
			return reportError("%s.map: only a column dimension maps its values", at)
		}
		if d.Path != nil && d.Elements == "" {
			return reportError("%s.path: only an elements dimension reads a path", at)
		}
		if d.ElementText != "" {
			switch {
			case d.Elements == "":
				return reportError("%s.element_text: only an elements dimension reads its elements' text", at)
			case d.Path != nil:
				return reportError("%s.element_text: not with a path, whose elements are JSON already", at)
			case d.ElementText != "json":
				return reportError("%s.element_text: %q; the one there is, is \"json\"", at, d.ElementText)
			}
		}
		if d.MetricWhere != nil && d.Metric == "" {
			return reportError("%s.metric_where: needs a metric", at)
		}
		if d.MetricWhere != nil {
			if v, err := decodeValue(d.MetricWhere); err != nil {
				return reportError("%s.metric_where: %v", at, err)
			} else if _, ok := v.(map[string]any); !ok {
				return reportError("%s.metric_where: an object of operators, such as {\"gt\": 0}", at)
			}
			if _, err := conditionSQL("m", d.MetricWhere); err != nil {
				return reportError("%s.metric_where: %v", at, err)
			}
		}
	}
	if len(s.Metrics) > reportMaxMetrics {
		return reportError("metrics: %d, more than the %d allowed", len(s.Metrics), reportMaxMetrics)
	}
	taken := map[string]bool{}
	for _, c := range reportFixed {
		taken[c] = true
	}
	for i, m := range s.Metrics {
		at := fmt.Sprintf("metrics[%d]", i)
		if !metricName.MatchString(m.Name) {
			return reportError("%s.name: %q is not a lower-case name of letters, digits and _", at, m.Name)
		}
		if (m.Column == "") == (m.DistinctWith == "") {
			return reportError("%s: needs either column or distinct_with", at)
		}
		if m.Coalesce != nil {
			if m.Column == "" {
				return reportError("%s.coalesce: only a column metric reads NULLs as a value", at)
			}
			if _, err := sqlLiteral(m.Coalesce); err != nil {
				return reportError("%s.coalesce: %v", at, err)
			}
		}
		for _, c := range m.columns() {
			if taken[c] {
				return reportError("%s.name: %q gives the column %s, which the result has already", at, m.Name, c)
			}
			taken[c] = true
		}
	}
	return nil
}

// columns are the result's columns metric m gives.
func (m reportMetric) columns() []string {
	if m.DistinctWith != "" {
		return []string{m.Name + "_distinct"}
	}
	return []string{m.Name + "_sum", m.Name + "_avg"}
}

// checkName checks a table's name: parts separated by dots.
func checkName(name string) error {
	if name == "" {
		return errors.New("needed")
	}
	for _, p := range strings.Split(name, ".") {
		if strings.TrimSpace(p) == "" {
			return fmt.Errorf("%q has an empty part", name)
		}
	}
	return nil
}

// tableName quotes a table's name, each of its parts.
func tableName(name string) string {
	parts := strings.Split(name, ".")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return quoteName(parts)
}

// checkSelect checks that text is one SELECT (or WITH ... SELECT) with cols
// columns (any number for 0), each named, and returns their names. The
// parse does not bind the statement: what it reads is checked with the
// statement it becomes part of.
func checkSelect(ctx context.Context, sess *session, text string, cols int) error {
	_, err := selectNames(ctx, sess, text, cols)
	return err
}

func selectNames(ctx context.Context, sess *session, text string, cols int) ([]string, error) {
	if sess == nil || sess.conn == nil {
		return nil, errors.New("no session to parse it")
	}
	tree, err := parseStatement(ctx, sess.conn, trimStatement(text))
	if err != nil {
		return nil, fmt.Errorf("not one SELECT: %v", err)
	}
	stmt, _ := tree.(map[string]any)
	node, _ := stmt["node"].(map[string]any)
	for node != nil && str(node["type"]) == "SET_OPERATION_NODE" {
		node, _ = node["left"].(map[string]any)
	}
	if node == nil || str(node["type"]) != "SELECT_NODE" {
		return nil, errors.New("not a SELECT")
	}
	var names []string
	for _, it := range asList(node["select_list"]) {
		im, _ := it.(map[string]any)
		name := str(im["alias"])
		if name == "" && str(im["class"]) == "COLUMN_REF" {
			cn := asList(im["column_names"])
			name = str(cn[len(cn)-1])
		}
		if name == "" {
			if cols == 1 && str(im["class"]) != "STAR" {
				name = "?"
			} else {
				return nil, errors.New("each column needs a name (AS name), and * is not one")
			}
		}
		names = append(names, name)
	}
	if cols > 0 && len(names) != cols {
		return nil, fmt.Errorf("selects %d columns, not %d", len(names), cols)
	}
	return names, nil
}

// trimStatement drops the semicolons and space a statement ends with.
func trimStatement(text string) string {
	return strings.TrimRight(strings.TrimSpace(text), "; \t\r\n")
}

// check checks a where's conditions.
func (w conditions) check(at string) error {
	for _, col := range slices.Sorted(mapKeys(w)) {
		if col == "" {
			return reportError("%s: a condition needs a column", at)
		}
		if _, err := conditionSQL(quoteIdent(col), w[col]); err != nil {
			return reportError("%s.%s: %v", at, col, err)
		}
	}
	return nil
}

func mapKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}

// sql returns the conditions on the columns of alias, ANDed, or "" for
// none.
func (w conditions) sql(alias string) string {
	var parts []string
	for _, col := range slices.Sorted(mapKeys(w)) {
		c, _ := conditionSQL(alias+"."+quoteIdent(col), w[col])
		parts = append(parts, c)
	}
	return strings.Join(parts, " AND ")
}

// conditionSQL is the SQL of a condition on the expression x.
func conditionSQL(x string, raw json.RawMessage) (string, error) {
	v, err := decodeValue(raw)
	if err != nil {
		return "", err
	}
	switch v := v.(type) {
	case nil:
		return "(" + x + " IS NULL)", nil
	case []any:
		if len(v) == 0 {
			return "", errors.New("an empty list matches nothing")
		}
		var lits []string
		for _, e := range v {
			if e == nil {
				return "", errors.New("a list of values holds no null")
			}
			l, err := valueSQL(e)
			if err != nil {
				return "", err
			}
			lits = append(lits, l)
		}
		return "(" + x + " IN (" + strings.Join(lits, ", ") + "))", nil
	case map[string]any:
		if len(v) == 0 {
			return "", errors.New("an empty condition")
		}
		ops := map[string]string{"eq": "=", "ne": "<>", "lt": "<", "le": "<=", "gt": ">", "ge": ">="}
		var parts []string
		for _, op := range slices.Sorted(mapKeys(v)) {
			arg := v[op]
			switch op {
			case "eq", "ne", "lt", "le", "gt", "ge", "like":
				if arg == nil {
					return "", fmt.Errorf("%s takes a value, not null", op)
				}
				l, err := valueSQL(arg)
				if err != nil {
					return "", err
				}
				if op == "like" {
					if _, ok := arg.(string); !ok {
						return "", errors.New("like takes a string")
					}
					parts = append(parts, "("+x+" LIKE "+l+")")
				} else {
					parts = append(parts, "("+x+" "+ops[op]+" "+l+")")
				}
			case "between":
				pair, ok := arg.([]any)
				if !ok || len(pair) != 2 || pair[0] == nil || pair[1] == nil {
					return "", errors.New("between takes two values, [low, high]")
				}
				lo, err := valueSQL(pair[0])
				if err != nil {
					return "", err
				}
				hi, err := valueSQL(pair[1])
				if err != nil {
					return "", err
				}
				parts = append(parts, "("+x+" BETWEEN "+lo+" AND "+hi+")")
			case "is_null":
				b, ok := arg.(bool)
				if !ok {
					return "", errors.New("is_null takes true or false")
				}
				if b {
					parts = append(parts, "("+x+" IS NULL)")
				} else {
					parts = append(parts, "("+x+" IS NOT NULL)")
				}
			default:
				return "", fmt.Errorf("unknown operator %q (eq, ne, lt, le, gt, ge, between, like, is_null)", op)
			}
		}
		return strings.Join(parts, " AND "), nil
	default:
		l, err := valueSQL(v)
		if err != nil {
			return "", err
		}
		return "(" + x + " = " + l + ")", nil
	}
}

// decodeValue reads a JSON value, numbers as json.Number.
func decodeValue(raw json.RawMessage) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// sqlLiteral is the SQL of a literal JSON value.
func sqlLiteral(raw json.RawMessage) (string, error) {
	v, err := decodeValue(raw)
	if err != nil {
		return "", err
	}
	return valueSQL(v)
}

func valueSQL(v any) (string, error) {
	switch v := v.(type) {
	case nil:
		return "NULL", nil
	case string:
		return quoteLiteral(v), nil
	case bool:
		if v {
			return "true", nil
		}
		return "false", nil
	case json.Number:
		if _, err := strconv.ParseFloat(string(v), 64); err != nil {
			return "", fmt.Errorf("%s is not a number", v)
		}
		return string(v), nil
	}
	return "", errors.New("a value is a string, number, boolean or null")
}

// --- the SQL ----------------------------------------------------------------

func cte(name string) string { return reportCTE + name + `"` }

// sql returns the statement the spec stands for: a SELECT of the result's
// columns, in their order, reading the tables as named and counting their
// members' values.
func (s *reportSpec) sql() string { return s.sqlWith(nil) }

// sqlWith is sql, reading the tables ids holds instead, at its snapshots,
// and counting their members' ids, when ids is not nil.
func (s *reportSpec) sqlWith(ids *reportIDs) string {
	var b strings.Builder
	b.WriteString("WITH ")
	var labels []string
	for i, c := range s.Cohorts {
		name := cte("cohort_" + strconv.Itoa(i+1))
		b.WriteString(name + " AS (")
		if c.SQL != "" {
			b.WriteString("SELECT DISTINCT q.member FROM (\n" + trimStatement(c.SQL) + "\n) AS q(member) WHERE q.member IS NOT NULL")
		} else {
			if ids != nil && i < len(ids.cohortFrom) && ids.cohortFrom[i] != "" {
				b.WriteString(ids.cohortFrom[i] + "),\n")
				labels = append(labels, "SELECT "+quoteLiteral(c.Label)+" AS cohort, member FROM "+name)
				continue
			}
			member, from := "t."+quoteIdent(c.Member), tableName(c.Table)+" AS t"
			if ids != nil {
				x := ids.cohorts[i]
				member, from = "CAST(t."+x.hidden+" AS UINTEGER)", x.from+" AS t AT (VERSION => "+strconv.FormatInt(x.at, 10)+")"
			}
			conds := []string{member + " IS NOT NULL"}
			if w := c.Where.sql("t"); w != "" {
				conds = append(conds, w)
			}
			if v := c.Version; v != nil {
				col := "t." + quoteIdent(v.Column)
				if v.Equals != nil {
					l, _ := sqlLiteral(v.Equals)
					conds = append(conds, "("+col+" = "+l+")")
				} else {
					sub := "SELECT max(v." + quoteIdent(v.Column) + ") FROM " + tableName(v.LatestFrom.Table) + " AS v"
					if w := v.LatestFrom.Where.sql("v"); w != "" {
						sub += " WHERE " + w
					}
					conds = append(conds, "("+col+" = ("+sub+"))")
				}
			}
			b.WriteString("SELECT DISTINCT " + member + " AS member FROM " + from + " WHERE " + strings.Join(conds, " AND "))
		}
		b.WriteString("),\n")
		labels = append(labels, "SELECT "+quoteLiteral(c.Label)+" AS cohort, member FROM "+name)
	}
	b.WriteString(cte("cohorts") + " AS (" + strings.Join(labels, " UNION ALL ") + "),\n")
	b.WriteString(cte("sizes") + " AS (SELECT cohort, count(DISTINCT member) AS cohort_size FROM " + cte("cohorts") + " GROUP BY cohort)\n")
	if len(s.Dimensions) == 0 {
		b.WriteString("SELECT CAST(NULL AS VARCHAR) AS dimension, CAST(NULL AS VARCHAR) AS dim_group, CAST(NULL AS VARCHAR) AS value, cohort, true AS is_total, " +
			"cohort_size AS people, CAST(NULL AS BIGINT) AS \"rows\", cohort_size, CAST(NULL AS DOUBLE) AS metric_sum, CAST(NULL AS BIGINT) AS metric_count, " +
			"CAST(NULL AS DOUBLE) AS metric_avg, 'scan' AS path FROM " + cte("sizes"))
		return b.String()
	}

	member, from := "s."+quoteIdent(s.Source.Member), tableName(s.Source.Table)+" AS s"
	if ids != nil {
		member, from = "CAST(s."+ids.source.hidden+" AS UINTEGER)", ids.source.from+" AS s AT (VERSION => "+strconv.FormatInt(ids.source.at, 10)+")"
	}
	from += " JOIN " + cte("cohorts") + " AS c ON c.member = " + member
	if w := s.Source.Where.sql("s"); w != "" {
		from += " WHERE " + w
	}
	var pairs map[int]bool
	split := false
	if ids != nil {
		pairs, split = ids.pairs, ids.split
	}
	counts, metricCols, extent := s.scanCounts(from, "c.cohort", member, true, pairs, split)
	b.WriteString(counts)
	b.WriteString("SELECT n.dimension, n.dim_group, n.value, n.cohort, n.total = 1 AS is_total, n.people, n.\"rows\", z.cohort_size, " +
		"n.metric_sum, n.metric_count, n.metric_avg")
	for _, c := range metricCols {
		b.WriteString(", " + c)
	}
	if s.Extent != "" {
		b.WriteString(", x.extent_min, x.extent_max")
	}
	path := "scan"
	if ids != nil && len(ids.cohortFrom) > 0 {
		path = "index+scan"
	}
	b.WriteString(", " + quoteLiteral(path) + " AS path FROM " + cte("counts") + " AS n JOIN " + cte("sizes") + " AS z ON z.cohort = n.cohort")
	if extent != "" {
		b.WriteString(" CROSS JOIN (" + extent + ") AS x")
	}
	b.WriteString(" WHERE n.everything = 0 AND n.\"rows\" > 0")
	return b.String()
}

// scanCounts writes the CTEs that count the source's rows by value: each
// of its rows whose member is in a cohort (from, the FROM of the source as
// s with each row's cohort, cohort, and member, member), once for each
// cohort, with its dimensions' entries, one for each value it has and one
// that counts in no value, so that every row is in the extent; and their
// counts by value and, for totals, by dimension. people counts their
// distinct members too. rbPairs are the distinct_with metrics whose pairs
// roaring bitmaps count (rb_count_distinct(member, key)): member is an ID
// domain's id and the metric's column a date or an integer (see
// pairBitmaps). split writes one such metric's pairs in aggregates the
// distributed planner can split among workers (see bitmapPairCounts), which
// on one machine take longer than rb_count_distinct. It returns the CTEs, the columns of the metrics as the
// counts' (alias n) and the SELECT of the extent, if any.
func (s *reportSpec) scanCounts(from, cohort, member string, people bool, rbPairs map[int]bool, split bool) (string, []string, string) {
	var b strings.Builder
	// Each row of the source whose member is in a cohort, once for each
	// cohort, with its dimensions' entries: one for each value it has, and
	// one that counts in no value, so that every row is in the extent.
	var rowCols []string
	for i, m := range s.Metrics {
		g := "g" + strconv.Itoa(i)
		if m.DistinctWith != "" {
			rowCols = append(rowCols, "s."+quoteIdent(m.DistinctWith)+" AS "+g)
		} else if m.Coalesce != nil {
			l, _ := sqlLiteral(m.Coalesce)
			rowCols = append(rowCols, "COALESCE(s."+quoteIdent(m.Column)+", "+l+") AS "+g)
		} else {
			rowCols = append(rowCols, "s."+quoteIdent(m.Column)+" AS "+g)
		}
	}
	if s.Extent != "" {
		rowCols = append(rowCols, "s."+quoteIdent(s.Extent)+" AS ext")
	}
	var lists []string
	var fixed []string
	for _, d := range s.Dimensions {
		if d.Elements != "" {
			lists = append(lists, d.elementsSQL())
		} else {
			fixed = append(fixed, d.entrySQL(d.valueSQL()))
		}
	}
	fixed = append(fixed, "{'d': CAST(NULL AS VARCHAR), 'g': CAST(NULL AS VARCHAR), 'v': CAST(NULL AS VARCHAR), 'm': CAST(NULL AS DOUBLE), 'h': false, 'k': false}")
	entries := "[" + strings.Join(fixed, ", ") + "]"
	for _, l := range lists {
		entries = "list_concat(" + entries + ", " + l + ")"
	}
	b.WriteString(", " + cte("rows") + " AS (SELECT " + cohort + " AS cohort, " + member + " AS member, ")
	for _, c := range rowCols {
		b.WriteString(c + ", ")
	}
	b.WriteString("unnest(" + entries + ") AS e FROM " + from + ")\n")

	// The counts, by value and, for totals, by dimension, and the extent
	// over every row.
	b.WriteString(", " + cte("values") + " AS (SELECT cohort, member, e.d AS dimension, e.g AS dim_group, e.v AS value, e.m AS metric, e.h AS has_metric, e.k AS keep")
	for i := range s.Metrics {
		b.WriteString(", g" + strconv.Itoa(i))
	}
	if s.Extent != "" {
		b.WriteString(", ext")
	}
	b.WriteString(" FROM " + cte("rows") + ")\n")
	var metricCols []string
	var hashed []int // distinct_with metrics that count their pairs by hashing
	for i, m := range s.Metrics {
		if m.DistinctWith != "" && !rbPairs[i] {
			hashed = append(hashed, i)
		}
		for _, c := range m.columns() {
			metricCols = append(metricCols, "n."+quoteIdent(c))
		}
	}
	var distinct []int
	for i, m := range s.Metrics {
		if m.DistinctWith != "" {
			distinct = append(distinct, i)
		}
	}
	if split && len(distinct) == 1 && rbPairs[distinct[0]] {
		return s.bitmapPairCounts(&b, people, distinct[0], metricCols)
	}
	if len(hashed) == 0 {
		// One pass over the rows: values, totals and the extent as
		// grouping sets, with distinct pairs, if any, in roaring bitmaps.
		aggs := "count(*) FILTER (WHERE keep) AS \"rows\", " +
			"sum(metric) FILTER (WHERE keep) AS metric_sum, " +
			// NULL, not 0, where no entry has a metric.
			"CASE WHEN bool_or(has_metric) FILTER (WHERE keep) THEN count(metric) FILTER (WHERE keep) END AS metric_count, " +
			"avg(metric) FILTER (WHERE keep) AS metric_avg"
		if people {
			aggs = "count(DISTINCT CASE WHEN keep THEN member END) AS people, " + aggs
		}
		for i, m := range s.Metrics {
			g := "g" + strconv.Itoa(i)
			if m.DistinctWith != "" {
				aggs += ", rb_count_distinct(member, " + g + ") FILTER (WHERE keep) AS " + quoteIdent(m.Name+"_distinct")
			} else {
				aggs += ", sum(" + g + ") FILTER (WHERE keep) AS " + quoteIdent(m.Name+"_sum") + ", avg(" + g + ") FILTER (WHERE keep) AS " + quoteIdent(m.Name+"_avg")
			}
		}
		extent := ""
		ext := ""
		if s.Extent != "" {
			ext = ", min(ext) AS extent_min, max(ext) AS extent_max"
		}
		sets := []string{"(dimension, dim_group, value, cohort)"}
		if s.Totals {
			sets = append(sets, "(dimension, dim_group, cohort)")
		}
		if s.Extent != "" {
			sets = append(sets, "()")
			extent = "SELECT extent_min, extent_max FROM " + cte("counts") + " WHERE everything = 1"
		}
		b.WriteString(", " + cte("counts") + " AS (SELECT dimension, dim_group, value, cohort, GROUPING(value) AS total, GROUPING(dimension) AS everything, " +
			aggs + ext + " FROM " + cte("values") + " GROUP BY GROUPING SETS (" + strings.Join(sets, ", ") + "))\n")
		return b.String(), metricCols, extent
	}

	// Distinct pairs counted in two steps, as Igloo rewrites the pair
	// counts of SQL (see paircount.go): the rows grouped by the keys, the
	// member and the pair's columns first, which makes each pair one row,
	// with the parts of the other aggregates; then counted, summed and
	// combined by key. DuckDB's DISTINCT aggregates keep a hash table of
	// structs for each group, which on large tables takes more memory than
	// the machine has, where a GROUP BY spills.
	var pairCols []string
	for i, m := range s.Metrics {
		if m.DistinctWith != "" {
			pairCols = append(pairCols, "g"+strconv.Itoa(i))
		}
	}
	inner := "count(*) FILTER (WHERE keep) AS r, sum(metric) FILTER (WHERE keep) AS ms, count(metric) FILTER (WHERE keep) AS mc, " +
		"bool_or(has_metric) FILTER (WHERE keep) AS hm, bool_or(keep) AS k"
	outer := "CAST(sum(r) AS BIGINT) AS \"rows\", sum(ms) AS metric_sum, CASE WHEN bool_or(hm) THEN CAST(sum(mc) AS BIGINT) END AS metric_count, " +
		"CASE WHEN sum(mc) > 0 THEN sum(ms) / sum(mc) END AS metric_avg"
	if people {
		outer = "count(DISTINCT member) FILTER (WHERE k) AS people, " + outer
	}
	for i, m := range s.Metrics {
		g := "g" + strconv.Itoa(i)
		switch {
		case m.DistinctWith == "":
			inner += ", sum(" + g + ") FILTER (WHERE keep) AS s" + g + ", count(" + g + ") FILTER (WHERE keep) AS c" + g
			outer += ", sum(s" + g + ") AS " + quoteIdent(m.Name+"_sum") + ", CASE WHEN sum(c" + g + ") > 0 THEN sum(s" + g + ") / sum(c" + g + ") END AS " + quoteIdent(m.Name+"_avg")
		case len(pairCols) == 1:
			// Each row of a key is one of its pairs.
			outer += ", count(*) FILTER (WHERE k AND " + g + " IS NOT NULL) AS " + quoteIdent(m.Name+"_distinct")
		case rbPairs[i]:
			outer += ", rb_count_distinct(member, " + g + ") FILTER (WHERE k) AS " + quoteIdent(m.Name+"_distinct")
		default:
			outer += ", count(DISTINCT row(member, " + g + ")) FILTER (WHERE k AND " + g + " IS NOT NULL) AS " + quoteIdent(m.Name+"_distinct")
		}
	}
	ext, extent := "", ""
	if s.Extent != "" {
		inner += ", min(ext) AS emin, max(ext) AS emax"
		ext = ", min(emin) AS extent_min, max(emax) AS extent_max"
		extent = "SELECT extent_min, extent_max FROM " + cte("counts") + " WHERE everything = 1"
	}
	keys := strings.Join(append([]string{"member"}, pairCols...), ", ")
	sets := []string{"(dimension, dim_group, value, cohort, " + keys + ")"}
	if s.Totals {
		sets = append(sets, "(dimension, dim_group, cohort, "+keys+")")
	}
	b.WriteString(", " + cte("pairs") + " AS (SELECT dimension, dim_group, value, cohort, " + keys + ", GROUPING(value) AS total, " + inner +
		" FROM " + cte("values") + " GROUP BY GROUPING SETS (" + strings.Join(sets, ", ") + "))\n")
	// The extent over every row, of either grouping set: min and max
	// take a row again as they took it once.
	outerSets := "(dimension, dim_group, value, cohort, total)"
	if s.Extent != "" {
		outerSets += ", ()"
	}
	b.WriteString(", " + cte("counts") + " AS (SELECT dimension, dim_group, value, cohort, total, GROUPING(dimension) AS everything, " + outer + ext +
		" FROM " + cte("pairs") + " GROUP BY GROUPING SETS (" + outerSets + "))\n")
	return b.String(), metricCols, extent
}

// bitmapPairCounts writes scanCounts' counts for one distinct_with metric,
// d, whose pairs roaring bitmaps count: the rows grouped by the finest keys
// (each value's) and the pair's column first (a day, say), with a bitmap of
// the members of each (value, day) (rb_agg) and the parts of the other
// aggregates; then those grouped by the grouping sets (totals, the extent);
// then combined by key, the pairs as the sum of the days' members and the
// people, if counted, as the members of their union. Each step is an
// aggregate the distributed planner splits: workers write their groups'
// bitmaps (rb_or_agg partials), which no count of pairs in one aggregate,
// rb_count_distinct(member, day), can be split into; and the finest groups
// first, in a plain GROUP BY, are combined on workers, so that the
// coordinator gets one row for each rather than one for each task.
func (s *reportSpec) bitmapPairCounts(b *strings.Builder, people bool, d int, metricCols []string) (string, []string, string) {
	g := "g" + strconv.Itoa(d)
	// fine: by the finest keys, the value and the day, with the aggregates'
	// parts; pairs: those combined by the grouping sets; counts: by key.
	fine := "count(*) FILTER (WHERE keep) AS r, sum(metric) FILTER (WHERE keep) AS ms, count(metric) FILTER (WHERE keep) AS mc, " +
		"bool_or(has_metric) FILTER (WHERE keep) AS hm, rb_agg(CASE WHEN keep AND " + g + " IS NOT NULL THEN member END) AS bm"
	sets := "sum(r) AS r, sum(ms) AS ms, sum(mc) AS mc, bool_or(hm) AS hm, rb_or_agg(bm) AS bm"
	outer := "CAST(sum(r) AS BIGINT) AS \"rows\", sum(ms) AS metric_sum, CASE WHEN bool_or(hm) THEN CAST(sum(mc) AS BIGINT) END AS metric_count, " +
		"CASE WHEN sum(mc) > 0 THEN sum(ms) / sum(mc) END AS metric_avg"
	if people {
		fine += ", rb_agg(CASE WHEN keep THEN member END) AS pm"
		sets += ", rb_or_agg(pm) AS pm"
		outer = "rb_cardinality(rb_or_agg(pm)) AS people, " + outer
	}
	for i, m := range s.Metrics {
		gi := "g" + strconv.Itoa(i)
		if i == d {
			outer += ", CAST(sum(rb_cardinality(bm)) AS BIGINT) AS " + quoteIdent(m.Name+"_distinct")
			continue
		}
		fine += ", sum(" + gi + ") FILTER (WHERE keep) AS s" + gi + ", count(" + gi + ") FILTER (WHERE keep) AS c" + gi
		sets += ", sum(s" + gi + ") AS s" + gi + ", sum(c" + gi + ") AS c" + gi
		outer += ", sum(s" + gi + ") AS " + quoteIdent(m.Name+"_sum") + ", CASE WHEN sum(c" + gi + ") > 0 THEN sum(s" + gi + ") / sum(c" + gi + ") END AS " + quoteIdent(m.Name+"_avg")
	}
	extent := ""
	if s.Extent != "" {
		fine += ", min(ext) AS emin, max(ext) AS emax"
		sets += ", min(emin) AS emin, max(emax) AS emax"
		outer += ", min(emin) AS extent_min, max(emax) AS extent_max"
		extent = "SELECT extent_min, extent_max FROM " + cte("counts") + " WHERE everything = 1"
	}
	grouping := []string{"(dimension, dim_group, value, cohort, " + g + ")"}
	if s.Totals {
		grouping = append(grouping, "(dimension, dim_group, cohort, "+g+")")
	}
	if s.Extent != "" {
		grouping = append(grouping, "()")
	}
	b.WriteString(", " + cte("fine") + " AS (SELECT dimension, dim_group, value, cohort, " + g + ", " + fine +
		" FROM " + cte("values") + " GROUP BY dimension, dim_group, value, cohort, " + g + ")\n")
	b.WriteString(", " + cte("pairs") + " AS (SELECT dimension, dim_group, value, cohort, GROUPING(value) AS total, GROUPING(dimension) AS everything, " + sets +
		" FROM " + cte("fine") + " GROUP BY GROUPING SETS (" + strings.Join(grouping, ", ") + "))\n")
	b.WriteString(", " + cte("counts") + " AS (SELECT dimension, dim_group, value, cohort, total, everything, " + outer +
		" FROM " + cte("pairs") + " GROUP BY dimension, dim_group, value, cohort, total, everything)\n")
	return b.String(), metricCols, extent
}

// valueSQL is the value of a column or constant dimension, of the source s.
func (d reportDim) valueSQL() string { return d.valueOf("s") }

// valueOf is a column or constant dimension's value, of the rows of alias.
func (d reportDim) valueOf(alias string) string {
	if d.Constant != nil {
		return quoteLiteral(*d.Constant)
	}
	v := "CAST(" + alias + "." + quoteIdent(d.Column) + " AS VARCHAR)"
	if d.Map == nil {
		return v
	}
	var b strings.Builder
	b.WriteString("CASE " + v)
	for _, k := range slices.Sorted(mapKeys(d.Map)) {
		to := "NULL"
		if t := d.Map[k]; t != nil {
			to = quoteLiteral(*t)
		}
		b.WriteString(" WHEN " + quoteLiteral(k) + " THEN " + to)
	}
	b.WriteString(" END")
	return b.String()
}

// entrySQL is a dimension's entry for the value v.
func (d reportDim) entrySQL(v string) string {
	group := "CAST(NULL AS VARCHAR)"
	if d.Group != nil {
		group = quoteLiteral(*d.Group)
	}
	metric := "CAST(NULL AS DOUBLE)"
	if d.Metric != "" {
		metric = "CAST(s." + quoteIdent(d.Metric) + " AS DOUBLE)"
	}
	keep := []string{}
	if d.SkipBlank {
		keep = append(keep, "("+v+") IS NOT NULL", "trim("+v+") <> ''")
	}
	if d.MetricWhere != nil {
		c, _ := conditionSQL(metric, d.MetricWhere)
		keep = append(keep, c)
	}
	k := "true"
	if len(keep) > 0 {
		k = "COALESCE(" + strings.Join(keep, " AND ") + ", false)"
	}
	return "{'d': " + quoteLiteral(d.Name) + ", 'g': " + group + ", 'v': CAST(" + v + " AS VARCHAR), 'm': " + metric +
		", 'h': " + strconv.FormatBool(d.Metric != "") + ", 'k': " + k + "}"
}

// elementText is how an elements dimension reads an element, e, as text:
// as it is, or, with element_text "json", one that is a JSON object or
// array as text as JSON, as Snowflake gives the text of a semi-structured
// array's elements. It is a function of the element alone, so an index of
// a list's elements answers for it too, its groups relabeled (see
// peopleCTEs).
func (d reportDim) elementText(e string) string {
	if d.ElementText != "json" {
		return e
	}
	return "CASE WHEN (starts_with(" + e + ", '{') OR starts_with(" + e + ", '[')) AND json_valid(" + e + ") " +
		"THEN json_extract_string(" + e + ", '$') ELSE " + e + " END"
}

// elementsSQL is the entries of an elements dimension: one for each
// element of the list (or JSON array) it reads.
func (d reportDim) elementsSQL() string {
	list := "s." + quoteIdent(d.Elements)
	elem := d.elementText("CAST(x AS VARCHAR)")
	if d.Path != nil {
		list = "CAST(json_extract(" + list + ", " + quoteLiteral(*d.Path) + ") AS JSON[])"
		elem = "json_extract_string(x, '$')"
	}
	return "COALESCE(list_transform(" + list + ", lambda x: " + d.entrySQL(elem) + "), [])"
}

// --- the parity check -------------------------------------------------------

// columns are the result's columns, in order.
func (s *reportSpec) columns() []string {
	cols := []string{"dimension", "dim_group", "value", "cohort", "is_total", "people", "rows", "cohort_size", "metric_sum", "metric_count", "metric_avg"}
	for _, m := range s.Metrics {
		cols = append(cols, m.columns()...)
	}
	if s.Extent != "" {
		cols = append(cols, "extent_min", "extent_max")
	}
	return append(cols, "path")
}

// approximate reports whether column c of the result is a sum or an average,
// which the check compares to 9 significant digits.
func approximate(c string) bool {
	return c == "metric_sum" || c == "metric_avg" || strings.HasSuffix(c, "_sum") || strings.HasSuffix(c, "_avg")
}

// checkSQL returns the statement of a parity check: the rows that differ
// between the spec's result and the caller's SQL, by the columns they
// share, with the side they are on.
func (s *reportSpec) checkSQL(ctx context.Context, sess *session, text, apiSQL string) (string, error) {
	names, err := selectNames(ctx, sess, text, 0)
	if err != nil {
		return "", reportError("the SQL to compare: %v", err)
	}
	have := map[string]bool{}
	for _, n := range names {
		have[strings.ToLower(n)] = true
	}
	for _, need := range []string{"dimension", "value", "cohort"} {
		if !have[need] {
			return "", reportError("the SQL to compare needs a column named %s", need)
		}
	}
	var cols []string
	for _, c := range s.columns() {
		if c != "path" && have[c] {
			cols = append(cols, c)
		}
	}
	side := func(from string) string {
		var out []string
		for _, c := range cols {
			x := from + "." + quoteIdent(c)
			if approximate(c) {
				x = "CAST(" + x + " AS DOUBLE)"
				x = "CASE WHEN " + x + " IS NULL OR " + x + " = 0 OR isnan(" + x + ") OR isinf(" + x + ") THEN " + x +
					" ELSE round(" + x + ", 8 - CAST(floor(log10(abs(" + x + "))) AS INTEGER)) END"
			}
			out = append(out, x+" AS "+quoteIdent(c))
		}
		return strings.Join(out, ", ")
	}
	api, mine := reportCompareCTE+`api"`, reportCompareCTE+`sql"`
	a, b := reportCompareCTE+`a"`, reportCompareCTE+`b"`
	return "WITH " + api + " AS (" + apiSQL + "),\n" + mine + " AS (\n" + trimStatement(text) + "\n),\n" +
		a + " AS (SELECT " + side("p") + " FROM " + api + " AS p),\n" +
		b + " AS (SELECT " + side("q") + " FROM " + mine + " AS q)\n" +
		"SELECT 'api_only' AS side, * FROM (SELECT * FROM " + a + " EXCEPT ALL SELECT * FROM " + b + ")\n" +
		"UNION ALL SELECT 'sql_only' AS side, * FROM (SELECT * FROM " + b + " EXCEPT ALL SELECT * FROM " + a + ")", nil
}
