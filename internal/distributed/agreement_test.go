package distributed

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// agreementModes are the ways TestAgreement runs each query on workers: forced on
// with the planner's own choice of joins, forced on with every join partitioned,
// forced on with partial results written a row at a time wherever they may be (see
// rowSpec), and in auto mode.
var agreementModes = []string{"on", "partitioned", "rows", "auto"}

// agreement is what one query came to in each mode: ran distributed, or why not.
type agreement struct {
	name    string
	ran     map[string]bool
	why     map[string]string
	differs []string
}

// agree runs query on one machine, then in each of agreementModes on workers, and
// compares each result with one machine's exactly, but for floating-point values'
// last digits. A result that differs, or a run on workers that fails, fails t.
func agree(t *testing.T, h *harness, name, query string) agreement {
	t.Helper()
	ctx := context.Background()
	a := agreement{name: name, ran: map[string]bool{}, why: map[string]string{}}
	want, wantCols, err := h.query(ctx, query)
	if err != nil {
		t.Fatalf("%s on one machine: %v", name, err)
	}
	broadcast := h.co.maxBroadcast
	defer func() { h.co.maxBroadcast, h.co.partialRows = broadcast, "" }()
	for _, mode := range agreementModes {
		h.co.maxBroadcast, h.co.partialRows = broadcast, ""
		m := ModeOn
		switch mode {
		case "partitioned":
			h.co.maxBroadcast = 1
		case "rows":
			h.co.partialRows = partialsByRow
		case "auto":
			m = ModeAuto
		}
		got, gotCols, plan, ex, err := h.run(ctx, query, m)
		if IsUnsupported(err) {
			a.why[mode] = err.Error()
			continue
		}
		if err != nil {
			t.Errorf("%s, %s: %v", name, mode, err)
			a.differs = append(a.differs, mode)
			continue
		}
		a.ran[mode] = true
		msg := compareRows(slices.Clone(want), got, hasOrderBy(query))
		if msg == "" && !slices.Equal(wantCols, gotCols) {
			msg = fmt.Sprintf("columns %v, want %v", gotCols, wantCols)
		}
		if msg != "" {
			t.Errorf("%s, %s: %s\n%s%s", name, mode, msg, plan.describe(), ex.exchanges)
			a.differs = append(a.differs, mode)
		}
	}
	return a
}

// report logs how many queries ran on workers in each mode and agreed with one
// machine, and why each of the others stayed on the coordinator.
func report(t *testing.T, suite string, all []agreement) (agreed, distributed int) {
	t.Helper()
	var b strings.Builder
	for _, a := range all {
		if len(a.differs) == 0 {
			agreed++
		}
		if a.ran["on"] {
			distributed++
		} else {
			fmt.Fprintf(&b, "  %s: %s\n", a.name, a.why["on"])
		}
	}
	counts := make([]string, len(agreementModes))
	for i, mode := range agreementModes {
		n := 0
		for _, a := range all {
			if a.ran[mode] {
				n++
			}
		}
		counts[i] = fmt.Sprintf("%s %d", mode, n)
	}
	t.Logf("%s: %d of %d queries agree with one machine in every mode; ran on workers: %s; on the coordinator when forced:\n%s",
		suite, agreed, len(all), strings.Join(counts, ", "), b.String())
	return agreed, distributed
}

// TestAgreement runs TPC-H's 22 queries and TPC-DS's 99 on one machine and on
// workers, forced and in auto mode, and fails on any result that differs from one
// machine's. With workers on, TPC-DS's q44 returned no rows where one machine
// returned 10, without an error: a stage cut its groups to a ranking's first before
// HAVING kept them. TPC-DS runs at scale factor 1, where all but one of its queries
// return rows (IGLOO_TEST_TPCDS_SF sets another).
func TestAgreement(t *testing.T) {
	if testing.Short() {
		t.Skip("runs 121 queries four ways")
	}
	t.Run("tpch", func(t *testing.T) {
		t.Parallel()
		h := newHarness(t, 0.1, 7, 3)
		var all []agreement
		for q := 1; q <= 22; q++ {
			b, err := os.ReadFile(filepath.Join("..", "..", "bench", "queries", "tpch", fmt.Sprintf("q%02d.sql", q)))
			if err != nil {
				t.Fatal(err)
			}
			all = append(all, agree(t, h, fmt.Sprintf("q%02d", q), string(b)))
		}
		if _, n := report(t, "TPC-H", all); n < 12 {
			t.Errorf("only %d of 22 TPC-H queries ran on workers when forced", n)
		}
	})
	t.Run("tpcds", func(t *testing.T) {
		t.Parallel()
		sf := 1.0
		if v := os.Getenv("IGLOO_TEST_TPCDS_SF"); v != "" {
			sf, _ = strconv.ParseFloat(v, 64)
		}
		h := newTPCDSHarness(t, sf, 4, 3)
		// A coordinator of a fleet's size computes larger scalar subqueries of
		// HAVING itself, as q44's: one of 2 cores leaves all of q44 to itself.
		h.co.opts.Cores = 16
		rows, _, err := h.query(context.Background(), "SELECT query_nr, query FROM tpcds_queries() ORDER BY query_nr")
		if err != nil {
			t.Fatal(err)
		}
		var all []agreement
		for _, r := range rows {
			start := time.Now()
			a := agree(t, h, fmt.Sprintf("q%02d", r[0]), r[1].(string))
			if testing.Verbose() {
				t.Logf("%s: %s, ran on workers %v", a.name, time.Since(start).Round(time.Millisecond), a.ran)
			}
			all = append(all, a)
		}
		if _, n := report(t, "TPC-DS", all); n < 70 {
			t.Errorf("only %d of 99 TPC-DS queries ran on workers when forced", n)
		}
	})
}

// newTPCDSHarness is a harness of TPC-DS's tables at scale factor sf, each in files
// files, and n workers.
func newTPCDSHarness(t *testing.T, sf float64, files, n int) *harness {
	t.Helper()
	if os.Getenv("IGLOO_TEST_EXTENSION_DIR") == "" {
		t.Skip("IGLOO_TEST_EXTENSION_DIR not set: TPC-DS needs the tpcds extension")
	}
	h := newHarnessWith(t, 0, files, n, "", "tpcds")
	h.exec(fmt.Sprintf("CALL dsdgen(sf = %g)", sf))
	rows, _, err := h.query(context.Background(), "SELECT table_name FROM duckdb_tables() WHERE database_name = current_database() AND schema_name = 'main' ORDER BY 1")
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		name := r[0].(string)
		h.addTable(name, name, files)
	}
	return h
}
