package distributed

import (
	"context"
	"testing"
)

// TestAnyExists runs TPC-DS q10 and q35, which ask whether a customer bought in
// stores and either on the web or from the catalog, on workers, with the sales
// tables too large for every worker to read and the customer's details small:
// the EXISTS of store sales and the OR of the other two, one EXISTS of their
// union (see anyExists), are joins by customer, and the rows are one machine's.
func TestAnyExists(t *testing.T) {
	h := newTPCDSHarness(t, 0.01, 4, 3)
	for _, name := range []string{"store_sales", "web_sales", "catalog_sales"} {
		h.tables[name].Bytes = 1 << 36
	}
	h.tables["customer"].Bytes = 1 << 33
	for _, nr := range []string{"10", "35"} {
		rows, _, err := h.query(context.Background(), "SELECT query FROM tpcds_queries() WHERE query_nr = "+nr)
		if err != nil || len(rows) != 1 {
			t.Fatalf("q%s: %v", nr, err)
		}
		q := rows[0][0].(string)
		want, _, err := h.query(t.Context(), q)
		if err != nil {
			t.Fatalf("q%s on one machine: %v", nr, err)
		}
		got, _, plan, _, err := h.run(t.Context(), q, ModeOn)
		if err != nil {
			t.Fatalf("q%s: %v", nr, err)
		}
		if msg := compareRows(want, got, hasOrderBy(q)); msg != "" {
			t.Errorf("q%s: %s\n%s", nr, msg, plan.describe())
		}
	}
}
