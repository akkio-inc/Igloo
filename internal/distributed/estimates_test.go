package distributed

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The estimate corpus: statements of TPC-H, report shapes, a dashboard's
// tiles and a dashboard burst's synthetic stand-ins, at two scales, each run
// alone on an Igloo replica by bench/estimates.py, with DuckDB's plan of it
// and what DuckDB held for it at its peak. TestEstimateCorpus replays the
// working-set estimate (planOf) over them.

// corpusTable is a table of the corpus, as bench/estimates.py writes it.
type corpusTable struct {
	Suite   string     `json:"suite"`
	Scale   float64    `json:"scale"`
	Kind    string     `json:"kind"`
	Name    string     `json:"name"`
	Rows    int64      `json:"rows"`
	Bytes   int64      `json:"bytes"`
	Columns [][]string `json:"columns"`
	// Distinct and Bounds are its columns' statistics, in the stats-*
	// files (see bench/estimates.py's --stats).
	Distinct map[string]int64      `json:"distinct"`
	Bounds   map[string][2]float64 `json:"bounds"`
	// ColumnBytes are its columns' compressed bytes in its data files.
	ColumnBytes map[string]int64 `json:"column_bytes"`
}

// corpusStatement is a statement of the corpus.
type corpusStatement struct {
	Suite   string          `json:"suite"`
	Scale   float64         `json:"scale"`
	Name    string          `json:"name"`
	Query   string          `json:"query"`
	Tables  []string        `json:"tables"`
	Threads int             `json:"threads"`
	Plan    json.RawMessage `json:"plan"`
	Peak    float64         `json:"peak"`
	Seconds float64         `json:"seconds"`
	Error   string          `json:"error"`
	// Operators are the operators of a run's profile, with the rows each
	// made, as the plan's tree.
	Operators []*corpusOperator `json:"operators"`
}

// corpusOperator is an operator of a run's profile.
type corpusOperator struct {
	Name     string            `json:"name"`
	Rows     float64           `json:"rows"`
	Children []*corpusOperator `json:"children"`
}

// withActualRows is a corpus statement's plan with each operator's
// estimate of its rows replaced by the rows it made in the run, where the
// profile's tree is the plan's: what the estimate would be, were DuckDB's
// estimates of rows right.
func withActualRows(s corpusStatement) (json.RawMessage, bool) {
	var roots []*planNode
	if json.Unmarshal(s.Plan, &roots) != nil || len(roots) != len(s.Operators) {
		return nil, false
	}
	same := true
	var walk func(n *planNode, o *corpusOperator)
	walk = func(n *planNode, o *corpusOperator) {
		if !strings.EqualFold(n.Name, o.Name) || len(n.Children) != len(o.Children) {
			same = false
			return
		}
		if n.Extra == nil {
			n.Extra = map[string]json.RawMessage{}
		}
		rows, _ := json.Marshal(strconv.FormatFloat(o.Rows, 'f', 0, 64))
		n.Extra["Estimated Cardinality"] = rows
		for i := range n.Children {
			walk(n.Children[i], o.Children[i])
		}
	}
	for i := range roots {
		walk(roots[i], s.Operators[i])
	}
	if !same {
		return nil, false
	}
	out, err := json.Marshal(roots)
	return out, err == nil
}

// analyzedCounts are the distinct counts of the corpus's tables' columns,
// as ANALYZE would keep them, by table (see corpus).
var analyzedCounts = map[*Table]map[string]int64{}

// corpus reads the corpus's statements, with each one's tables.
func corpus(t *testing.T) ([]corpusStatement, map[string]*Table) {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "estimates", "*.jsonl.gz"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no corpus: %v", err)
	}
	var out []corpusStatement
	tables := map[string]*Table{}
	// The statistics after the tables they are of.
	stats := slices.DeleteFunc(slices.Clone(files), func(f string) bool { return !strings.HasPrefix(filepath.Base(f), "stats-") })
	files = append(slices.DeleteFunc(files, func(f string) bool { return strings.HasPrefix(filepath.Base(f), "stats-") }), stats...)
	for _, f := range files {
		fh, err := os.Open(f)
		if err != nil {
			t.Fatal(err)
		}
		gz, err := gzip.NewReader(fh)
		if err != nil {
			t.Fatal(err)
		}
		sc := bufio.NewScanner(gz)
		sc.Buffer(make([]byte, 1<<20), 64<<20)
		for sc.Scan() {
			var kind struct{ Kind string }
			if err := json.Unmarshal(sc.Bytes(), &kind); err != nil {
				t.Fatal(err)
			}
			if kind.Kind == "table" {
				var ct corpusTable
				if err := json.Unmarshal(sc.Bytes(), &ct); err != nil {
					t.Fatal(err)
				}
				if tb := tables[corpusKey(ct.Suite, ct.Scale, ct.Name)]; tb != nil && (ct.Bounds != nil || ct.Distinct != nil) {
					// A table's statistics: its bounds, which its data files'
					// manifests keep; and its distinct counts, as ANALYZE keeps
					// them, for estimates of analyzed tables (see analyzed).
					tb.Bounds = ct.Bounds
					analyzedCounts[tb] = ct.Distinct
					continue
				}
				tb := &Table{Schema: strings.ReplaceAll(corpusKey(ct.Suite, ct.Scale, ""), "/", "_"), Name: ct.Name, Rows: ct.Rows, Bytes: ct.Bytes,
					ColumnBytes: ct.ColumnBytes, Bounds: ct.Bounds}
				if strings.HasSuffix(ct.Suite, "+analyzed") {
					// Its statements ran with the tables analyzed, which DuckDB's
					// plans of them read too.
					tb.Distinct = ct.Distinct
				}
				for _, c := range ct.Columns {
					tb.Columns = append(tb.Columns, Column{Name: c[0], Type: c[1]})
				}
				tables[corpusKey(ct.Suite, ct.Scale, ct.Name)] = tb
				continue
			}
			var s corpusStatement
			if err := json.Unmarshal(sc.Bytes(), &s); err != nil {
				t.Fatal(err)
			}
			if s.Error == "" {
				out = append(out, s)
			}
		}
		if err := sc.Err(); err != nil {
			t.Fatal(err)
		}
		fh.Close()
	}
	return out, tables
}

// corpusKey names a table of a suite at a scale; the reports read the TPC-H
// suite's tables.
func corpusKey(suite string, scale float64, name string) string {
	if rest, ok := strings.CutPrefix(suite, "reports"); ok {
		suite = "tpch" + rest
	}
	return fmt.Sprintf("%s/%g/%s", suite, scale, name)
}

// estimateOf replays the working-set estimate of a corpus statement, with
// what l learned before, if not nil.
func estimateOf(s corpusStatement, tables map[string]*Table, l *learned) (planEstimate, error) {
	st := corpusStats(s, tables)
	st.learned = l
	return planOf(string(s.Plan), st)
}

// corpusStats are a corpus statement's tables' statistics.
func corpusStats(s corpusStatement, tables map[string]*Table) planStats {
	var in []Input
	for _, name := range s.Tables {
		if tb := tables[corpusKey(s.Suite, s.Scale, name)]; tb != nil {
			in = append(in, Input{Table: tb})
		}
	}
	return newPlanStats([]*Stage{{Inputs: in}}, s.Threads)
}

// learnFrom teaches l what a corpus statement's operators made in its run.
func learnFrom(s corpusStatement, tables map[string]*Table, l *learned) {
	plan, ok := withActualRows(s)
	if !ok {
		return
	}
	st := corpusStats(s, tables)
	st.learned, st.observe = l, true
	_, _ = planOf(string(plan), st)
}

// operatorClasses are the kinds of operator a plan has that hold rows, as
// the corpus's errors are reported by.
func operatorClasses(plan json.RawMessage) []string {
	var roots []*planNode
	if json.Unmarshal(plan, &roots) != nil {
		return nil
	}
	seen := map[string]bool{}
	var walk func(n *planNode)
	walk = func(n *planNode) {
		name := strings.ToUpper(n.Name)
		switch {
		case strings.Contains(name, "JOIN"), name == "CROSS_PRODUCT":
			seen["join"] = true
		case strings.HasSuffix(name, "GROUP_BY"), name == "UNGROUPED_AGGREGATE":
			seen["aggregate"] = true
		case name == "ORDER_BY", name == "TOP_N":
			seen["sort"] = true
		case name == "WINDOW", name == "STREAMING_WINDOW":
			seen["window"] = true
		case name == "UNNEST":
			seen["unnest"] = true
		case name == "CTE":
			seen["cte"] = true
		case name == "UNION":
			seen["union"] = true
		}
		for _, a := range n.strs("Aggregates") {
			if distinctArg.MatchString(a) {
				seen["distinct"] = true
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	for _, r := range roots {
		walk(r)
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// estimateFloor is the least a statement is taken to hold, estimated or
// held: below it what DuckDB holds is its readers' and buffers' few
// megabytes, and a ratio of two such says nothing.
const estimateFloor = 64 << 20

// estimateErrors summarizes the ratios of estimate to what was held.
type estimateErrors struct {
	ratios []float64
}

func (e *estimateErrors) add(r float64) { e.ratios = append(e.ratios, r) }

// shares are the share of the ratios within twice either way, and how many
// are more than four times short.
func (e estimateErrors) shares() (within float64, short int) {
	n := 0
	for _, x := range e.ratios {
		if x >= 0.5 && x <= 2 {
			n++
		}
		if x < 0.25 {
			short++
		}
	}
	return float64(n) / float64(max(len(e.ratios), 1)), short
}

func (e estimateErrors) String() string {
	if len(e.ratios) == 0 {
		return "none"
	}
	r := slices.Clone(e.ratios)
	slices.Sort(r)
	within, short4 := 0, 0
	for _, x := range r {
		if x >= 0.5 && x <= 2 {
			within++
		}
		if x < 0.25 {
			short4++
		}
	}
	return fmt.Sprintf("%3d statements, median %5.2f, %3.0f%% within 2x, %2d over 4x short, least %6.3f, most %7.2f",
		len(r), r[len(r)/2], 100*float64(within)/float64(len(r)), short4, r[0], r[len(r)-1])
}

// TestEstimateCorpus replays the working-set estimate over the corpus and
// reports how far it is from what each statement held, by suite and scale
// and by the kinds of operator their plans have; and how far it would be
// were DuckDB's estimates of each operator's rows the rows it made.
func TestEstimateCorpus(t *testing.T) {
	stmts, tables := corpus(t)
	type worst struct {
		name      string
		ratio     float64
		est, peak float64
		detail    string
	}
	// mode: "first" estimates each statement as on its first run, with
	// nothing learned; "analyzed" so, its tables analyzed (with distinct
	// counts); "learning" as the corpus's statements run in turn, each
	// learning from those before it; "actual" with each operator's actual
	// rows.
	report := func(label, mode string) ([]worst, estimateErrors) {
		if mode == "analyzed" {
			for tb, d := range analyzedCounts {
				tb.Distinct = d
			}
			defer func() {
				for tb := range analyzedCounts {
					tb.Distinct = nil
				}
			}()
		}
		all := estimateErrors{}
		bySuite, byClass := map[string]*estimateErrors{}, map[string]*estimateErrors{}
		var worsts []worst
		var l *learned
		if mode == "learning" {
			l = newLearned()
		}
		for _, s := range stmts {
			orig := s
			if mode == "actual" {
				plan, ok := withActualRows(s)
				if !ok {
					continue
				}
				s.Plan = plan
			}
			p, err := estimateOf(s, tables, l)
			if l != nil {
				learnFrom(orig, tables, l)
			}
			if err != nil {
				t.Logf("%s %g %s: %v", s.Suite, s.Scale, s.Name, err)
				continue
			}
			est, peak := max(p.set, estimateFloor), max(s.Peak, estimateFloor)
			r := est / peak
			all.add(r)
			key := fmt.Sprintf("%s %g", s.Suite, s.Scale)
			if bySuite[key] == nil {
				bySuite[key] = &estimateErrors{}
			}
			bySuite[key].add(r)
			for _, c := range operatorClasses(s.Plan) {
				if byClass[c] == nil {
					byClass[c] = &estimateErrors{}
				}
				byClass[c].add(r)
			}
			parts := slices.Clone(p.parts)
			slices.SortFunc(parts, func(a, b heldPart) int { return int(math.Copysign(1, b.bytes-a.bytes)) })
			var d []string
			for i, h := range parts {
				if i == 3 {
					break
				}
				d = append(d, fmt.Sprintf("%s %.3g rows %.0f MB", h.op, h.rows, h.bytes/1e6))
			}
			worsts = append(worsts, worst{name: key + " " + s.Name, ratio: r, est: p.set, peak: s.Peak, detail: strings.Join(d, "; ")})
		}
		t.Logf("%s", label)
		t.Logf("  all       %s", all)
		for _, k := range sortedKeys(bySuite) {
			t.Logf("  %-9s %s", k, bySuite[k])
		}
		for _, k := range sortedKeys(byClass) {
			t.Logf("  %-9s %s", k, byClass[k])
		}
		slices.SortFunc(worsts, func(a, b worst) int {
			return int(math.Copysign(1, math.Abs(math.Log(b.ratio))-math.Abs(math.Log(a.ratio))))
		})
		if floor, ok := estimateFloors[mode]; ok {
			if within, short := all.shares(); within < floor.within || short > floor.short {
				t.Errorf("%s: %.0f%% within twice what they held, %d more than four times short; want at least %.0f%% and at most %d",
					label, 100*within, short, 100*floor.within, floor.short)
			}
		}
		return worsts, all
	}
	worsts, _ := report("estimated on a first run", "first")
	report("estimated on a first run, the tables analyzed", "analyzed")
	learning, _ := report("estimated after what the statements before it made", "learning")
	actual, _ := report("with each operator's actual rows", "actual")
	byName, learnt := map[string]worst{}, map[string]worst{}
	for _, w := range actual {
		byName[w.name] = w
	}
	for _, w := range learning {
		learnt[w.name] = w
	}
	for i, w := range worsts {
		if i == worstShown {
			break
		}
		a := byName[w.name]
		t.Logf("%6.2fx (%6.2fx learning, %6.2fx by actual rows)  estimated %6.0f MB, held %6.0f MB  %s: %s | by actual rows: %s",
			w.ratio, learnt[w.name].ratio, a.ratio, w.est/1e6, w.peak/1e6, w.name, w.detail, a.detail)
	}
	for i, w := range learning {
		if i == worstShown {
			break
		}
		t.Logf("learning %6.2fx  estimated %6.0f MB, held %6.0f MB  %s: %s", w.ratio, w.est/1e6, w.peak/1e6, w.name, w.detail)
	}
	for i, w := range actual {
		if i == worstShown {
			break
		}
		t.Logf("by actual rows %6.2fx  estimated %6.0f MB, held %6.0f MB  %s: %s", w.ratio, w.est/1e6, w.peak/1e6, w.name, w.detail)
	}
}

// worstShown is how many of the statements furthest off the test logs.
const worstShown = 30

// estimateFloors are the least share of the corpus's statements estimated
// within twice what they held, and the most more than four times short,
// that TestEstimateCorpus accepts of each mode: what the estimate does
// now, so that a change that does worse fails.
var estimateFloors = map[string]struct {
	within float64
	short  int
}{
	"first":    {0.66, 2},
	"learning": {0.66, 2},
	"actual":   {0.85, 2},
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
