package prices

import (
	"math"
	"testing"
)

// TestHourly checks prices of the machines the OpenTofu modules and the
// benchmarks name: by family and vCPUs, in other regions, on Spot.
func TestHourly(t *testing.T) {
	for _, c := range []struct {
		typ, region string
		spot        bool
		want        float64
	}{
		// AWS's list prices in us-east-1, as bench/pricing.toml has some.
		{"m7gd.2xlarge", "", false, 0.4271},
		{"m7gd.8xlarge", "us-east-1", false, 1.7085},
		{"r7gd.16xlarge", "", false, 4.3546},
		{"c7g.2xlarge", "", false, 0.29},
		{"r7g.4xlarge", "", false, 0.8568},
		{"m7gd.4xlarge", "", true, 0.8542 * 0.35},
		{"m7gd.4xlarge", "eu-west-1", false, 0.8542 * 1.11},
		{"m7g.medium", "", false, 0.0408},
		{"Standard_D16pds_v6", "eastus", false, 16 * 0.0429},
		{"Standard_E64pds_v6", "westeurope", true, 64 * 0.0567 * 1.15 * 0.30},
		{"n4-highmem-64", "us-central1", false, 64 * 0.0639},
		{"n4-standard-16", "", true, 16 * 0.0474 * 0.35},
		// A region the table does not know counts as the base region.
		{"m7gd.2xlarge", "mars-north-1", false, 0.4271},
	} {
		got, ok := Hourly(c.typ, c.region, c.spot)
		if !ok || math.Abs(got-c.want) > 0.002 {
			t.Errorf("%s in %q, spot %v: %.4f %v, want %.4f", c.typ, c.region, c.spot, got, ok, c.want)
		}
	}
	for _, typ := range []string{"", "z9.large", "m7gd.huge", "Standard_X4_v9", "n9-weird-4", "local"} {
		if p, ok := Hourly(typ, "", false); ok {
			t.Errorf("%q: %v", typ, p)
		}
	}
	// A large machine of a family against small ones: as their vCPUs.
	large, _ := Hourly("r7gd.16xlarge", "", false)
	small, _ := Hourly("r7gd.2xlarge", "", false)
	if math.Abs(large/small-8) > 1e-9 {
		t.Errorf("r7gd.16xlarge is %.2f times r7gd.2xlarge", large/small)
	}
}
