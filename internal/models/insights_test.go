package models

import (
	"math"
	"strings"
	"testing"
)

// driverTable is spend by country, device and age, in a control and a test
// period: US mobile spend tripled, everything else stayed as it was.
func driverTable() *Table {
	t := &Table{}
	country := &TableCol{Name: "country", Type: TypeString, TypeName: "VARCHAR"}
	device := &TableCol{Name: "device", Type: TypeString, TypeName: "VARCHAR"}
	age := &TableCol{Name: "age", Type: TypeNumber, TypeName: "BIGINT"}
	test := &TableCol{Name: "is_test", Type: TypeBool, TypeName: "BOOLEAN"}
	spend := &TableCol{Name: "spend", Type: TypeNumber, TypeName: "DOUBLE"}
	for _, period := range []string{"false", "true"} {
		for i := range 600 {
			c := []string{"us", "uk", "de"}[i%3]
			d := []string{"mobile", "desktop"}[(i/3)%2]
			a := float64(18 + i%50)
			v := 10.0 + float64(i%7)
			if period == "true" && c == "us" && d == "mobile" {
				v *= 3
			}
			country.Str, country.Null = append(country.Str, c), append(country.Null, false)
			device.Str, device.Null = append(device.Str, d), append(device.Null, false)
			age.Num, age.Null = append(age.Num, a), append(age.Null, false)
			test.Str, test.Null = append(test.Str, period), append(test.Null, false)
			spend.Num, spend.Null = append(spend.Num, v), append(spend.Null, false)
			t.N++
		}
	}
	t.Cols = []*TableCol{country, device, age, test, spend}
	return t
}

func TestGetDrivers(t *testing.T) {
	res, err := GetDrivers(driverTable(), "IS_TEST", "spend")
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Rows[0][0]; got != `["Overall"]` {
		t.Fatalf("the first row is %v", got)
	}
	overall := res.Rows[0][3].(float64)
	if overall <= 0 || res.Rows[0][4].(float64) != 1 {
		t.Errorf("overall %v", res.Rows[0])
	}
	// The planted driver: every bit of the change, in the segment of US
	// mobile, reached through COUNTRY = us.
	found := false
	for _, row := range res.Rows[1:] {
		c := row[0].(string)
		contribution := row[3].(float64)
		if strings.Contains(c, `"COUNTRY = us"`) && strings.Contains(c, `"DEVICE = mobile"`) && !strings.Contains(c, "not ") {
			found = true
			if math.Abs(contribution-overall) > 1e-9*overall || math.Abs(row[5].(float64)-2) > 1e-9 {
				t.Errorf("US mobile: %v, want all of %v at a growth of 2", row, overall)
			}
		}
		if strings.Contains(c, `"not COUNTRY = us"`) && math.Abs(contribution) > 1e-9 {
			t.Errorf("outside the US: %v", row)
		}
	}
	if !found {
		t.Errorf("no US mobile segment: %v", res.Rows)
	}
	// Ordered by contribution after Overall.
	for i := 2; i < len(res.Rows); i++ {
		if res.Rows[i][3].(float64) > res.Rows[i-1][3].(float64)+1e-9 {
			t.Errorf("rows %d and %d out of order", i-1, i)
		}
	}
	for _, bad := range [][2]string{{"spend", "spend"}, {"is_test", "country"}, {"nope", "spend"}} {
		if _, err := GetDrivers(driverTable(), bad[0], bad[1]); err == nil {
			t.Errorf("%v: no error", bad)
		}
	}
}

func TestRegressor(t *testing.T) {
	tb := testTable(600)
	r, err := TrainRegressor(tb, "spend", 0, nil)
	if err == nil {
		// The target is one of the table's columns; the rest are features.
		if r.Target != "spend" || len(r.Features) != 2 {
			t.Errorf("trained %+v", r)
		}
	} else {
		t.Fatal(err)
	}
	if _, err := TrainRegressor(tb, "region", 0, nil); err == nil || !strings.Contains(err.Error(), "predicts numbers") {
		t.Errorf("a string target: %v", err)
	}
}
