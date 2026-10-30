package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Config is the gateway's settings, from IGLOO_GATEWAY_* variables; see
// docs/scale-to-zero.md.
type Config struct {
	ListenAddr    string
	HTTPAddr      string
	Backend       string
	ActivityURL   string
	SuspendAfter  time.Duration
	ResumeTimeout time.Duration
	// FirstByteTimeout and MaxWaiting hold connections to limits until
	// they are forwarded (see Options).
	FirstByteTimeout time.Duration
	MaxWaiting       int

	// ConsoleListenAddr and ConsoleBackend forward the web console too,
	// when ConsoleBackend is set.
	ConsoleListenAddr string
	ConsoleBackend    string
	// FlightListenAddr and FlightBackend forward Arrow Flight SQL too, when
	// FlightBackend is set.
	FlightListenAddr string
	FlightBackend    string
	// IcebergListenAddr and IcebergBackend forward the Iceberg REST catalog
	// too, when IcebergBackend is set.
	IcebergListenAddr string
	IcebergBackend    string

	// Compute is ec2, kubernetes or command.
	Compute        string
	EC2InstanceIDs []string
	K8sNamespace   string
	K8sDeployment  string
	K8sReplicas    int
	// K8sWorkerDeployment, if set, is a Deployment of distributed-execution
	// workers to stop along with Igloo.
	K8sWorkerDeployment string
	StartCommand        string
	StopCommand         string
}

// LoadConfig reads the configuration using lookup, normally os.LookupEnv.
func LoadConfig(lookup func(string) (string, bool)) (Config, error) {
	get := func(key, fallback string) string {
		if v, _ := lookup(key); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
		return fallback
	}
	cfg := Config{
		ListenAddr:          get("IGLOO_GATEWAY_LISTEN_ADDR", ":5432"),
		HTTPAddr:            get("IGLOO_GATEWAY_HTTP_ADDR", ":8080"),
		Backend:             get("IGLOO_GATEWAY_BACKEND", ""),
		Compute:             get("IGLOO_GATEWAY_COMPUTE", ""),
		K8sNamespace:        get("IGLOO_GATEWAY_K8S_NAMESPACE", ""),
		K8sDeployment:       get("IGLOO_GATEWAY_K8S_DEPLOYMENT", ""),
		K8sWorkerDeployment: get("IGLOO_GATEWAY_K8S_WORKER_DEPLOYMENT", ""),
		StartCommand:        get("IGLOO_GATEWAY_START_COMMAND", ""),
		StopCommand:         get("IGLOO_GATEWAY_STOP_COMMAND", ""),
	}

	var errs []error
	addErr := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	host, port, err := net.SplitHostPort(cfg.Backend)
	if err != nil || host == "" || port == "" {
		addErr("IGLOO_GATEWAY_BACKEND must be Igloo's SQL address as host:port, got %q", cfg.Backend)
	}

	cfg.ConsoleBackend = get("IGLOO_GATEWAY_CONSOLE_BACKEND", "")
	cfg.ConsoleListenAddr = get("IGLOO_GATEWAY_CONSOLE_LISTEN_ADDR", ":8443")
	if cfg.ConsoleBackend != "" {
		if h, p, err := net.SplitHostPort(cfg.ConsoleBackend); err != nil || h == "" || p == "" {
			addErr("IGLOO_GATEWAY_CONSOLE_BACKEND must be Igloo's console address as host:port, got %q", cfg.ConsoleBackend)
		}
	}
	cfg.FlightBackend = get("IGLOO_GATEWAY_FLIGHT_BACKEND", "")
	cfg.FlightListenAddr = get("IGLOO_GATEWAY_FLIGHT_LISTEN_ADDR", ":31337")
	if cfg.FlightBackend != "" {
		if h, p, err := net.SplitHostPort(cfg.FlightBackend); err != nil || h == "" || p == "" {
			addErr("IGLOO_GATEWAY_FLIGHT_BACKEND must be Igloo's Flight SQL address as host:port, got %q", cfg.FlightBackend)
		}
	}
	cfg.IcebergBackend = get("IGLOO_GATEWAY_ICEBERG_BACKEND", "")
	cfg.IcebergListenAddr = get("IGLOO_GATEWAY_ICEBERG_LISTEN_ADDR", ":8181")
	if cfg.IcebergBackend != "" {
		if h, p, err := net.SplitHostPort(cfg.IcebergBackend); err != nil || h == "" || p == "" {
			addErr("IGLOO_GATEWAY_ICEBERG_BACKEND must be Igloo's Iceberg REST address as host:port, got %q", cfg.IcebergBackend)
		}
	}

	// The activity endpoint defaults to Igloo's HTTP port on the same host.
	// Setting the variable to an empty string turns the check off.
	if v, ok := lookup("IGLOO_GATEWAY_ACTIVITY_URL"); ok {
		cfg.ActivityURL = strings.TrimSpace(v)
	} else if host != "" {
		cfg.ActivityURL = "http://" + net.JoinHostPort(host, "8080") + "/activity"
	}
	if cfg.ActivityURL != "" {
		if u, err := url.Parse(cfg.ActivityURL); err != nil || u.Host == "" {
			addErr("IGLOO_GATEWAY_ACTIVITY_URL must be a URL such as http://igloo:8080/activity, got %q", cfg.ActivityURL)
		}
	}

	duration := func(key, fallback string) time.Duration {
		v := get(key, fallback)
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			addErr("%s must be a positive duration such as 10m, got %q", key, v)
		}
		return d
	}
	cfg.SuspendAfter = duration("IGLOO_GATEWAY_SUSPEND_AFTER", "10m")
	cfg.ResumeTimeout = duration("IGLOO_GATEWAY_RESUME_TIMEOUT", "5m")
	cfg.FirstByteTimeout = duration("IGLOO_GATEWAY_FIRST_BYTE_TIMEOUT", defaultFirstByteTimeout.String())
	maxWaiting := get("IGLOO_GATEWAY_MAX_WAITING", strconv.Itoa(defaultMaxWaiting))
	if n, err := strconv.Atoi(maxWaiting); err != nil || n < 1 {
		addErr("IGLOO_GATEWAY_MAX_WAITING must be a positive integer, got %q", maxWaiting)
	} else {
		cfg.MaxWaiting = n
	}

	switch cfg.Compute {
	case "ec2":
		for _, id := range strings.Split(get("IGLOO_GATEWAY_EC2_INSTANCE_IDS", ""), ",") {
			if id = strings.TrimSpace(id); id != "" {
				cfg.EC2InstanceIDs = append(cfg.EC2InstanceIDs, id)
			}
		}
		if len(cfg.EC2InstanceIDs) == 0 {
			addErr("IGLOO_GATEWAY_EC2_INSTANCE_IDS must list the instances Igloo runs on")
		}
	case "kubernetes":
		if cfg.K8sDeployment == "" {
			addErr("IGLOO_GATEWAY_K8S_DEPLOYMENT must name Igloo's Deployment")
		}
		v := get("IGLOO_GATEWAY_K8S_REPLICAS", "1")
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			addErr("IGLOO_GATEWAY_K8S_REPLICAS must be a positive integer, got %q", v)
		}
		cfg.K8sReplicas = n
	case "command":
		if cfg.StartCommand == "" || cfg.StopCommand == "" {
			addErr("IGLOO_GATEWAY_START_COMMAND and IGLOO_GATEWAY_STOP_COMMAND must both be set")
		}
	default:
		addErr("IGLOO_GATEWAY_COMPUTE must be ec2, kubernetes or command, got %q", cfg.Compute)
	}
	return cfg, errors.Join(errs...)
}

// NewCompute returns the driver cfg.Compute names.
func NewCompute(ctx context.Context, cfg Config) (Compute, error) {
	switch cfg.Compute {
	case "ec2":
		return NewEC2(ctx, cfg.EC2InstanceIDs)
	case "kubernetes":
		k, err := NewKubernetes(cfg.K8sNamespace, cfg.K8sDeployment, cfg.K8sReplicas)
		if err != nil || cfg.K8sWorkerDeployment == "" {
			return k, err
		}
		workers, err := NewKubernetes(cfg.K8sNamespace, cfg.K8sWorkerDeployment, 0)
		if err != nil {
			return nil, err
		}
		return AlsoStops{Compute: k, Others: []Compute{workers}}, nil
	case "command":
		return Command{StartCommand: cfg.StartCommand, StopCommand: cfg.StopCommand}, nil
	}
	return nil, fmt.Errorf("unknown compute %q", cfg.Compute)
}
