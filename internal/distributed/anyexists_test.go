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

// TestNotExistsOverJoins runs TPC-DS q69, which keeps the customers, joined to
// their addresses and demographics, who bought in stores but neither on the web
// nor from the catalog, on workers, with the sales tables too large for every
// worker to read: its NOT EXISTS are anti joins by the customer's key, the
// customers read by partition (see innerItem), and the rows are one machine's.
func TestNotExistsOverJoins(t *testing.T) {
	h := newTPCDSHarness(t, 0.01, 4, 3)
	for _, name := range []string{"store_sales", "web_sales", "catalog_sales"} {
		h.tables[name].Bytes = 1 << 36
	}
	h.tables["customer"].Bytes = 1 << 33
	rows, _, err := h.query(context.Background(), "SELECT query FROM tpcds_queries() WHERE query_nr = 69")
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	q := rows[0][0].(string)
	want, _, err := h.query(t.Context(), q)
	if err != nil {
		t.Fatal(err)
	}
	got, _, plan, _, err := h.run(t.Context(), q, ModeOn)
	if err != nil {
		t.Fatal(err)
	}
	if msg := compareRows(want, got, hasOrderBy(q)); msg != "" {
		t.Errorf("%s\n%s", msg, plan.describe())
	}
}

// TestResidualCorrelations runs TPC-DS q94 and q16, which keep orders shipped
// from more than one warehouse and never returned, on workers, with the sales
// and returns too large for every worker to read: their EXISTS of the same
// order from another warehouse is a semi join by the order's key that checks
// the warehouses beside it (see correlation.residual), and the rows are one
// machine's.
func TestResidualCorrelations(t *testing.T) {
	h := newTPCDSHarness(t, 0.01, 4, 3)
	for _, name := range []string{"web_sales", "catalog_sales", "web_returns", "catalog_returns"} {
		h.tables[name].Bytes = 1 << 36
	}
	for _, nr := range []string{"94", "16"} {
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

// TestScalarJoinOverJoins runs TPC-DS q32 and q92, which sum the discounts of
// an item's sales above 1.3 times the item's average, its sales joined to the
// items and dates, on workers, with the sales too large for every worker to
// read: the average is a LEFT JOIN to the items' averages by the item's key,
// read by partition with the sales of the items (see keyItem), and the rows
// are one machine's.
func TestScalarJoinOverJoins(t *testing.T) {
	h := newTPCDSHarness(t, 0.01, 4, 3)
	for _, name := range []string{"web_sales", "catalog_sales"} {
		h.tables[name].Bytes = 1 << 36
	}
	for _, nr := range []string{"32", "92"} {
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
