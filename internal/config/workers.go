package config

import (
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"
)

// Workers configures distributed execution (docs/distributed-execution.md):
// on the server, where its workers are and how to start them; on a worker,
// where it listens. Both sides share Secret.
type Workers struct {
	// Addresses are the workers' host:port. Empty turns distributed
	// execution off.
	Addresses []string
	// Secret authenticates coordinators and workers to each other.
	Secret string
	// Compute starts and stops workers: kubernetes, command, ec2, or empty
	// when they always run.
	Compute string
	// Max is the most workers Compute starts.
	Max int
	// IdleAfter is how long workers run without tasks before Compute stops
	// them; with IdleAuto (IGLOO_WORKER_IDLE_AFTER=auto, the default), the
	// most they do, three times their start from two minutes on.
	IdleAfter time.Duration
	IdleAuto  bool
	// StartTimeout bounds how long a statement waits for workers to start.
	StartTimeout time.Duration
	// Warm is how many idle workers past those busy stay up while some
	// work, where Compute can stop workers alone (IGLOO_WORKER_WARM, 1);
	// -1 (off) stops them only all together.
	Warm int
	// Threshold is how long a statement must take on one machine before
	// auto mode runs it on workers, unless it would run short of memory.
	Threshold time.Duration
	// Exchanges is where workers write the rows they exchange: object (the
	// warehouse's object storage), local (their own disks), or auto (their
	// own disks when every worker of a statement has room there and runs
	// outside Kubernetes, object storage otherwise). ExchangeMax,
	// on a worker, caps the bytes of exchanges on its disk; zero leaves
	// them to the disk's free space.
	Exchanges   string
	ExchangeMax int64

	K8sNamespace   string
	K8sDeployment  string
	StartCommand   string
	StopCommand    string
	EC2InstanceIDs []string

	// Price is what a worker costs an hour, for auto mode's weighing of how
	// many a statement runs on (IGLOO_WORKER_PRICE_PER_HOUR); zero for
	// unknown.
	Price float64
	// Large is the compute pool's large worker, which a statement may run
	// on alone (IGLOO_LARGE_WORKER*); no address for none.
	Large LargeWorker
	// InstanceType and Spot say what machines the workers are
	// (IGLOO_WORKER_INSTANCE_TYPE, IGLOO_WORKER_SPOT), and Region where
	// (IGLOO_CLOUD_REGION), for their list price where Price is not set.
	InstanceType string
	Spot         bool
	Region       string

	// ListenAddr is where a worker accepts tasks.
	ListenAddr string
	// SpotNotice has a worker on EC2 watch for AWS's notice that it is
	// taking the instance back, and then finish its tasks and take no more
	// (IGLOO_WORKER_SPOT_NOTICE, on by default).
	SpotNotice bool
	// IsolatedPurgeAfter is how long a worker goes without hearing from any
	// coordinator before it empties its caches by itself, for erasure
	// requests that cannot reach it (IGLOO_WORKER_ISOLATED_PURGE_AFTER, a
	// minute by default).
	IsolatedPurgeAfter time.Duration
}

// LargeWorker configures a compute pool's large worker: a worker of many
// times a small one's memory, which auto mode weighs against the small
// workers and the server for running a statement whole
// (docs/distributed-execution.md#a-large-worker).
type LargeWorker struct {
	// Address is its host:port.
	Address string
	// Compute starts and stops it, as Workers.Compute does the others, with
	// its own Deployment, commands or instances; the Deployment's namespace
	// is the workers'.
	Compute        string
	K8sDeployment  string
	StartCommand   string
	StopCommand    string
	EC2InstanceIDs []string
	// Cores and Memory are its cores and DuckDB memory limit, for estimates
	// before it has answered; Price what it costs an hour, zero for
	// unknown; InstanceType and Spot what machine it is, for its list
	// price where Price is not set.
	Cores        int
	Memory       int64
	Price        float64
	InstanceType string
	Spot         bool
}

// WorkerPort is the port workers listen on unless told otherwise.
const WorkerPort = "7433"

// ShuffleDir is the directory under the warehouse root where workers
// exchange the rows of tables partitioned for a join, while a statement
// runs.
const ShuffleDir = "_igloo_shuffle"

// minWorkerSecret matches distributed.MinSecretLength.
const minWorkerSecret = 32

func loadWorkers(lookup func(string) (string, bool), errs *[]error) Workers {
	get := func(key, fallback string) string {
		if v, _ := lookup(key); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return fallback
	}
	addErr := func(format string, args ...any) { *errs = append(*errs, fmt.Errorf(format, args...)) }
	w := Workers{
		Secret:        get("IGLOO_WORKER_SECRET", ""),
		Compute:       get("IGLOO_WORKER_COMPUTE", ""),
		K8sNamespace:  get("IGLOO_WORKER_K8S_NAMESPACE", ""),
		K8sDeployment: get("IGLOO_WORKER_K8S_DEPLOYMENT", ""),
		StartCommand:  get("IGLOO_WORKER_START_COMMAND", ""),
		StopCommand:   get("IGLOO_WORKER_STOP_COMMAND", ""),
		ListenAddr:    get("IGLOO_WORKER_LISTEN_ADDR", ":"+WorkerPort),
		Exchanges:     get("IGLOO_EXCHANGES", "object"),
	}
	if w.Exchanges != "object" && w.Exchanges != "local" && w.Exchanges != "auto" {
		addErr("IGLOO_EXCHANGES must be object, local or auto, got %q", w.Exchanges)
	}
	if v := get("IGLOO_EXCHANGE_MAX_SIZE", ""); v != "" {
		n, err := ParseSize(v)
		if err != nil {
			addErr("IGLOO_EXCHANGE_MAX_SIZE: %v", err)
		}
		w.ExchangeMax = n
	}
	for _, a := range strings.Split(get("IGLOO_WORKERS", ""), ",") {
		if a = strings.TrimSpace(a); a == "" {
			continue
		}
		if _, _, err := net.SplitHostPort(a); err != nil {
			a = net.JoinHostPort(a, WorkerPort)
		}
		if h, p, err := net.SplitHostPort(a); err != nil || h == "" || p == "" {
			addErr("IGLOO_WORKERS entry %q must be host or host:port", a)
			continue
		}
		w.Addresses = append(w.Addresses, a)
	}
	ids := func(key string) []string {
		var out []string
		for _, id := range strings.Split(get(key, ""), ",") {
			if id = strings.TrimSpace(id); id != "" {
				out = append(out, id)
			}
		}
		return out
	}
	w.EC2InstanceIDs = ids("IGLOO_WORKER_EC2_INSTANCE_IDS")
	price := func(key string) float64 {
		v := get(key, "0")
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || math.IsInf(f, 0) || math.IsNaN(f) {
			addErr("%s must be a price such as 0.58, or 0 for unknown, got %q", key, v)
			return 0
		}
		return f
	}
	w.Price = price("IGLOO_WORKER_PRICE_PER_HOUR")
	flag := func(key string) bool {
		switch v := strings.ToLower(get(key, "false")); v {
		case "true", "on", "1":
			return true
		case "false", "off", "0":
		default:
			addErr("%s must be true or false, got %q", key, v)
		}
		return false
	}
	w.InstanceType, w.Spot, w.Region = get("IGLOO_WORKER_INSTANCE_TYPE", ""), flag("IGLOO_WORKER_SPOT"), get("IGLOO_CLOUD_REGION", "")
	w.Large = LargeWorker{
		Address:        get("IGLOO_LARGE_WORKER", ""),
		Compute:        get("IGLOO_LARGE_WORKER_COMPUTE", ""),
		K8sDeployment:  get("IGLOO_LARGE_WORKER_K8S_DEPLOYMENT", ""),
		StartCommand:   get("IGLOO_LARGE_WORKER_START_COMMAND", ""),
		StopCommand:    get("IGLOO_LARGE_WORKER_STOP_COMMAND", ""),
		EC2InstanceIDs: ids("IGLOO_LARGE_WORKER_EC2_INSTANCE_IDS"),
		Price:          price("IGLOO_LARGE_WORKER_PRICE_PER_HOUR"),
		InstanceType:   get("IGLOO_LARGE_WORKER_INSTANCE_TYPE", ""),
		Spot:           flag("IGLOO_LARGE_WORKER_SPOT"),
	}
	if a := w.Large.Address; a != "" {
		if _, _, err := net.SplitHostPort(a); err != nil {
			a = net.JoinHostPort(a, WorkerPort)
		}
		if h, p, err := net.SplitHostPort(a); err != nil || h == "" || p == "" || strings.Contains(a, ",") {
			addErr("IGLOO_LARGE_WORKER %q must be one host or host:port", w.Large.Address)
		}
		w.Large.Address = a
	}
	if v := get("IGLOO_LARGE_WORKER_CORES", ""); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			addErr("IGLOO_LARGE_WORKER_CORES must be a positive integer, got %q", v)
		}
		w.Large.Cores = n
	}
	if v := get("IGLOO_LARGE_WORKER_MEMORY", ""); v != "" {
		n, err := ParseSize(v)
		if err != nil || n <= 0 {
			addErr("IGLOO_LARGE_WORKER_MEMORY must be a size such as 400GiB, got %q", v)
		}
		w.Large.Memory = n
	}

	v := get("IGLOO_WORKER_MAX", "4")
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		addErr("IGLOO_WORKER_MAX must be a positive integer, got %q", v)
	}
	w.Max = n
	duration := func(key, fallback string) time.Duration {
		v := get(key, fallback)
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			addErr("%s must be a positive duration such as 10m, got %q", key, v)
		}
		return d
	}
	switch v := strings.ToLower(get("IGLOO_WORKER_SPOT_NOTICE", "on")); v {
	case "on", "true":
		w.SpotNotice = true
	case "off", "false":
	default:
		addErr("IGLOO_WORKER_SPOT_NOTICE must be on or off, got %q", v)
	}
	if strings.EqualFold(get("IGLOO_WORKER_IDLE_AFTER", "auto"), "auto") {
		w.IdleAfter, w.IdleAuto = 10*time.Minute, true
	} else {
		w.IdleAfter = duration("IGLOO_WORKER_IDLE_AFTER", "10m")
	}
	w.StartTimeout = duration("IGLOO_WORKER_START_TIMEOUT", "3m")
	switch v := strings.ToLower(get("IGLOO_WORKER_WARM", "1")); v {
	case "off":
		w.Warm = -1
	default:
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			addErr("IGLOO_WORKER_WARM must be a number of workers, or off, got %q", v)
		}
		w.Warm = n
	}
	w.IsolatedPurgeAfter = duration("IGLOO_WORKER_ISOLATED_PURGE_AFTER", "1m")
	if w.IsolatedPurgeAfter > 0 && w.IsolatedPurgeAfter < 10*time.Second {
		addErr("IGLOO_WORKER_ISOLATED_PURGE_AFTER must be at least 10s: coordinators check workers every few seconds, got %s",
			w.IsolatedPurgeAfter)
	}
	w.Threshold = duration("IGLOO_WORKER_THRESHOLD", "1m")

	if len(w.Addresses) == 0 {
		if w.Large.Address != "" {
			addErr("IGLOO_LARGE_WORKER needs IGLOO_WORKERS: the large worker belongs to a pool's workers")
		}
		return w
	}
	if len(w.Secret) < minWorkerSecret {
		addErr("IGLOO_WORKER_SECRET must be set to at least %d characters when IGLOO_WORKERS is", minWorkerSecret)
	}
	switch w.Compute {
	case "":
	case "kubernetes":
		if w.K8sDeployment == "" {
			addErr("IGLOO_WORKER_K8S_DEPLOYMENT must name the workers' Deployment")
		}
	case "command":
		if w.StartCommand == "" || w.StopCommand == "" {
			addErr("IGLOO_WORKER_START_COMMAND and IGLOO_WORKER_STOP_COMMAND must both be set")
		}
	case "ec2":
		if len(w.EC2InstanceIDs) == 0 {
			addErr("IGLOO_WORKER_EC2_INSTANCE_IDS must list the workers' instances")
		}
	default:
		addErr("IGLOO_WORKER_COMPUTE must be kubernetes, command, ec2 or empty, got %q", w.Compute)
	}
	if l := w.Large; l.Address != "" {
		switch l.Compute {
		case "":
		case "kubernetes":
			if l.K8sDeployment == "" {
				addErr("IGLOO_LARGE_WORKER_K8S_DEPLOYMENT must name the large worker's Deployment")
			}
		case "command":
			if l.StartCommand == "" || l.StopCommand == "" {
				addErr("IGLOO_LARGE_WORKER_START_COMMAND and IGLOO_LARGE_WORKER_STOP_COMMAND must both be set")
			}
		case "ec2":
			if len(l.EC2InstanceIDs) == 0 {
				addErr("IGLOO_LARGE_WORKER_EC2_INSTANCE_IDS must list the large worker's instance")
			}
		default:
			addErr("IGLOO_LARGE_WORKER_COMPUTE must be kubernetes, command, ec2 or empty, got %q", l.Compute)
		}
		if l.Compute != "" && (l.Cores == 0 || l.Memory == 0) {
			// Stopped, it is weighed by its size before it ever answers.
			addErr("IGLOO_LARGE_WORKER_CORES and IGLOO_LARGE_WORKER_MEMORY must be set when IGLOO_LARGE_WORKER_COMPUTE starts it")
		}
	}
	return w
}

// LoadWorker reads a worker's configuration (igloo worker): where table
// data lives and how to reach it, DuckDB's settings, the read cache, logs,
// and the worker's own settings. Workers have no catalog and no users.
func LoadWorker(lookup func(string) (string, bool)) (Config, error) {
	cfg, err := Load(func(key string) (string, bool) {
		switch key {
		case "IGLOO_ADMIN_PASSWORD":
			// Workers have no users; Load insists on an administrator.
			return "workers-have-no-users", true
		case "IGLOO_CATALOG_URI", "IGLOO_WORKERS":
			return "", false
		}
		return lookup(key)
	})
	if err != nil {
		return cfg, err
	}
	var errs []error
	if cfg.Storage.Location == "" {
		errs = append(errs, errors.New("IGLOO_WAREHOUSE_LOCATION must be set"))
	} else if err := cfg.Storage.parseLocation(); err != nil {
		errs = append(errs, fmt.Errorf("IGLOO_WAREHOUSE_LOCATION: %w", err))
	}
	if len(cfg.Workers.Secret) < minWorkerSecret {
		errs = append(errs, fmt.Errorf("IGLOO_WORKER_SECRET must be set to at least %d characters", minWorkerSecret))
	}
	if cfg.DuckDB.TempDir == "" {
		errs = append(errs, errors.New("IGLOO_DUCKDB_TEMP_DIR must be set"))
	}
	return cfg, errors.Join(errs...)
}
