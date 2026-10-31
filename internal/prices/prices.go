// Package prices knows what common cloud machines cost an hour, so that
// distributed execution can weigh its workers by cost without being told
// their prices (docs/distributed-execution.md#prices).
//
// The numbers are list prices, not anyone's bill: on-demand, Linux, by the
// vCPU of each instance family as the providers list them for their base
// regions (AWS us-east-1, Azure eastus, Google Cloud us-central1) in
// October 2026, rounded. AWS's match bench/pricing.toml; Azure's and Google
// Cloud's are approximate. Within a family a machine costs about as its
// vCPUs, which is all auto mode needs to weigh a large machine against
// small ones; other regions are the base region's price times a rough
// regional factor, and Spot is a typical discount, which in fact varies
// by type, zone and hour. Set IGLOO_WORKER_PRICE_PER_HOUR and
// IGLOO_LARGE_WORKER_PRICE_PER_HOUR for exact or negotiated rates.
package prices

import (
	"regexp"
	"strconv"
	"strings"
)

// Cloud is a provider whose machines the table knows.
type Cloud string

const (
	AWS   Cloud = "aws"
	Azure Cloud = "azure"
	GCP   Cloud = "gcp"
)

// perVCPU is what a vCPU of each family costs an hour on demand in the
// cloud's base region, in dollars.
var perVCPU = map[Cloud]map[string]float64{
	AWS: {
		// Graviton, with and without local NVMe (d).
		"m6g": 0.0385, "m6gd": 0.0452, "r6g": 0.0504, "r6gd": 0.0576, "c6g": 0.034, "c6gd": 0.0384,
		"m7g": 0.0408, "m7gd": 0.05339, "r7g": 0.05355, "r7gd": 0.06804, "c7g": 0.03625, "c7gd": 0.04536,
		"m8g": 0.04488, "r8g": 0.0589, "c8g": 0.0399,
		// Intel and AMD.
		"m7i": 0.0504, "r7i": 0.06615, "c7i": 0.04463, "m7a": 0.05796, "r7a": 0.07608, "c7a": 0.05131,
		"m6i": 0.048, "r6i": 0.063, "c6i": 0.0425, "i4i": 0.0858, "i4g": 0.0772,
	},
	Azure: {
		// Cobalt (p) and AMD (a), with a local disk (d).
		"dpds_v6": 0.0429, "epds_v6": 0.0567, "dps_v6": 0.0378, "eps_v6": 0.0504,
		"dpds_v5": 0.0452, "epds_v5": 0.0609, "dads_v5": 0.0515, "eads_v5": 0.068,
		"ds_v5": 0.048, "es_v5": 0.063, "dds_v5": 0.0565, "eds_v5": 0.072,
	},
	GCP: {
		"n4-standard": 0.0474, "n4-highmem": 0.0639, "n4-highcpu": 0.0352,
		"n2-standard": 0.04856, "n2-highmem": 0.06551, "n2-highcpu": 0.03585,
		"n2d-standard": 0.04225, "n2d-highmem": 0.0570, "c4-standard": 0.0546, "c4-highmem": 0.0727,
		"c3-standard": 0.0522, "c3-highmem": 0.0703, "c3d-standard": 0.0454, "c3d-highmem": 0.0612,
	},
}

// spotShare is what a Spot machine typically costs of its on-demand price.
var spotShare = map[Cloud]float64{AWS: 0.35, Azure: 0.30, GCP: 0.35}

// regionFactor is about what a region's machines cost against the base
// region's; a region the table does not know counts as the base region.
var regionFactor = map[Cloud]map[string]float64{
	AWS: {
		"us-east-1": 1, "us-east-2": 1, "us-west-2": 1, "us-west-1": 1.17, "ca-central-1": 1.1,
		"eu-west-1": 1.11, "eu-west-2": 1.16, "eu-west-3": 1.16, "eu-central-1": 1.18, "eu-north-1": 1.06,
		"ap-south-1": 1.05, "ap-southeast-1": 1.25, "ap-southeast-2": 1.25, "ap-northeast-1": 1.28, "sa-east-1": 1.55,
	},
	Azure: {
		"eastus": 1, "eastus2": 1, "centralus": 1, "westus2": 1, "westus3": 1, "northeurope": 1.1, "westeurope": 1.15,
		"uksouth": 1.15, "germanywestcentral": 1.15, "japaneast": 1.25, "australiaeast": 1.25, "southeastasia": 1.2,
	},
	GCP: {
		"us-central1": 1, "us-east1": 1, "us-west1": 1, "us-east4": 1.12, "europe-west1": 1.1, "europe-west4": 1.1,
		"europe-west2": 1.2, "europe-west3": 1.2, "asia-northeast1": 1.28, "asia-southeast1": 1.23, "australia-southeast1": 1.3,
	},
}

var (
	// awsType is family.size: m7gd.2xlarge, r7g.medium.
	awsType = regexp.MustCompile(`^([a-z0-9-]+)\.(medium|large|xlarge|(\d+)xlarge)$`)
	// azureType is Standard_<series><vCPUs><features>_v<n>: Standard_D16pds_v6.
	azureType = regexp.MustCompile(`^standard_([a-z]+)(\d+)([a-z]*)_(v\d+)$`)
	// gcpType is family-shape-vCPUs: n4-highmem-64.
	gcpType = regexp.MustCompile(`^([a-z0-9]+-[a-z]+)-(\d+)$`)
)

// Parse names an instance type's cloud, family and vCPUs: an AWS type
// such as m7gd.2xlarge (8), an Azure size such as Standard_E64pds_v6 (64),
// or a Google Cloud machine type such as n4-highmem-64 (64). ok is false
// for a name of none of those forms.
func Parse(typ string) (cloud Cloud, family string, vcpus int, ok bool) {
	t := strings.ToLower(strings.TrimSpace(typ))
	if m := awsType.FindStringSubmatch(t); m != nil {
		switch m[2] {
		case "medium":
			vcpus = 1
		case "large":
			vcpus = 2
		case "xlarge":
			vcpus = 4
		default:
			n, _ := strconv.Atoi(m[3])
			vcpus = 4 * n
		}
		return AWS, m[1], vcpus, true
	}
	if m := azureType.FindStringSubmatch(t); m != nil {
		n, _ := strconv.Atoi(m[2])
		return Azure, m[1] + m[3] + "_" + m[4], n, true
	}
	if m := gcpType.FindStringSubmatch(t); m != nil {
		n, _ := strconv.Atoi(m[2])
		return GCP, m[1], n, true
	}
	return "", "", 0, false
}

// Hourly returns the list price an hour of a machine of instance type typ
// in region (empty for the cloud's base region), on Spot if spot; ok is
// false for a type the table does not know.
func Hourly(typ, region string, spot bool) (float64, bool) {
	cloud, family, vcpus, ok := Parse(typ)
	if !ok {
		return 0, false
	}
	rate, ok := perVCPU[cloud][family]
	if !ok || vcpus <= 0 {
		return 0, false
	}
	price := rate * float64(vcpus)
	if f, ok := regionFactor[cloud][strings.ToLower(strings.TrimSpace(region))]; ok {
		price *= f
	}
	if spot {
		price *= spotShare[cloud]
	}
	return price, true
}
