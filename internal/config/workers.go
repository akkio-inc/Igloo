package config

import (
	"errors"
	"fmt"
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
	// them.
	IdleAfter time.Duration
	// StartTimeout bounds how long a statement waits for workers to start.
	StartTimeout time.Duration

	K8sNamespace   string
	K8sDeployment  string
	StartCommand   string
	StopCommand    string
	EC2InstanceIDs []string

	// ListenAddr is where a worker accepts tasks.
	ListenAddr string
}

// WorkerPort is the port workers listen on unless told otherwise.
const WorkerPort = "7433"

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
	for _, id := range strings.Split(get("IGLOO_WORKER_EC2_INSTANCE_IDS", ""), ",") {
		if id = strings.TrimSpace(id); id != "" {
			w.EC2InstanceIDs = append(w.EC2InstanceIDs, id)
		}
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
	w.IdleAfter = duration("IGLOO_WORKER_IDLE_AFTER", "10m")
	w.StartTimeout = duration("IGLOO_WORKER_START_TIMEOUT", "3m")

	if len(w.Addresses) == 0 {
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
