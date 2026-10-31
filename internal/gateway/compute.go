package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// Command runs shell commands to start and stop the compute, for platforms
// without a built-in driver: for example `gcloud compute instances start`
// or `az vm start`.
type Command struct {
	StartCommand, StopCommand string
}

// Start runs StartCommand.
func (c Command) Start(ctx context.Context) error { return run(ctx, c.StartCommand) }

// Stop runs StopCommand.
func (c Command) Stop(ctx context.Context) error { return run(ctx, c.StopCommand) }

func run(ctx context.Context, command string) error {
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%s: %w: %s", command, err, strings.TrimSpace(tail(out.String(), 500)))
	}
	return nil
}

func tail(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[len(s)-n:]
}

// EC2 starts and stops EC2 instances. Credentials come from the usual AWS
// sources, normally the gateway machine's instance profile, which needs
// ec2:DescribeInstances, ec2:StartInstances and ec2:StopInstances.
type EC2 struct {
	client *ec2.Client
	ids    []string
}

// NewEC2 returns a driver for the instances with ids.
func NewEC2(ctx context.Context, ids []string) (*EC2, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	return &EC2{client: ec2.NewFromConfig(cfg), ids: ids}, nil
}

// Start starts the instances that are stopped. An instance that is still
// stopping cannot be started, so it waits for that to finish.
func (e *EC2) Start(ctx context.Context) error {
	for {
		states, err := e.states(ctx)
		if err != nil {
			return err
		}
		var stopped []string
		waiting := false
		for id, st := range states {
			switch st {
			case types.InstanceStateNameStopped:
				stopped = append(stopped, id)
			case types.InstanceStateNameStopping:
				waiting = true
			case types.InstanceStateNameShuttingDown, types.InstanceStateNameTerminated:
				return fmt.Errorf("instance %s is %s", id, st)
			}
		}
		if len(stopped) > 0 {
			slices.Sort(stopped)
			_, err := e.client.StartInstances(ctx, &ec2.StartInstancesInput{InstanceIds: stopped})
			if err != nil && !strings.Contains(err.Error(), "IncorrectInstanceState") {
				return fmt.Errorf("start instances: %w", err)
			}
			// On IncorrectInstanceState, look again.
			if err == nil && !waiting {
				return nil
			}
		} else if !waiting {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Stop stops the instances. Their disks, and so a local catalog database
// and storage, are kept.
func (e *EC2) Stop(ctx context.Context) error {
	_, err := e.client.StopInstances(ctx, &ec2.StopInstancesInput{InstanceIds: e.ids})
	if err != nil {
		return fmt.Errorf("stop instances: %w", err)
	}
	return nil
}

func (e *EC2) states(ctx context.Context) (map[string]types.InstanceStateName, error) {
	out, err := e.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{InstanceIds: e.ids})
	if err != nil {
		return nil, fmt.Errorf("describe instances: %w", err)
	}
	states := make(map[string]types.InstanceStateName, len(e.ids))
	for _, r := range out.Reservations {
		for _, inst := range r.Instances {
			if inst.State != nil {
				states[aws.ToString(inst.InstanceId)] = inst.State.Name
			}
		}
	}
	for _, id := range e.ids {
		if _, ok := states[id]; !ok {
			return nil, fmt.Errorf("instance %s not found", id)
		}
	}
	return states, nil
}

// serviceAccountDir is where Kubernetes mounts a pod's API credentials.
const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// Kubernetes scales a Deployment between zero and Replicas through the
// Kubernetes API, using the gateway pod's service account. The account
// needs get and patch on the Deployment's scale subresource. Nodes are
// released by the cluster autoscaler once the pods are gone.
type Kubernetes struct {
	Namespace, Deployment string
	Replicas              int
	client                *http.Client
	base                  string
	tokenFile             string
}

// NewKubernetes returns a driver for the Deployment name in namespace; an
// empty namespace means the gateway's own.
func NewKubernetes(namespace, name string, replicas int) (*Kubernetes, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, errors.New("not running in Kubernetes: KUBERNETES_SERVICE_HOST is not set")
	}
	if namespace == "" {
		b, err := os.ReadFile(serviceAccountDir + "/namespace")
		if err != nil {
			return nil, fmt.Errorf("read namespace: %w", err)
		}
		namespace = strings.TrimSpace(string(b))
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("cluster CA certificate is not valid PEM")
	}
	return &Kubernetes{
		Namespace:  namespace,
		Deployment: name,
		Replicas:   replicas,
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
		base:      "https://" + net.JoinHostPort(host, port),
		tokenFile: serviceAccountDir + "/token",
	}, nil
}

// Start scales the Deployment to Replicas.
func (k *Kubernetes) Start(ctx context.Context) error { return k.scale(ctx, k.Replicas, "") }

// Stop scales the Deployment to zero.
func (k *Kubernetes) Stop(ctx context.Context) error { return k.scale(ctx, 0, "") }

// ScaleAtLeast makes the Deployment run at least n replicas, or none for
// n = 0. It never lowers a count otherwise, and changes it only if no one
// else did since it read it, so callers that each want some replicas, such
// as Igloo replicas starting distributed-execution workers, end up with the
// most any of them asked for.
func (k *Kubernetes) ScaleAtLeast(ctx context.Context, n int) error {
	if n == 0 {
		return k.Stop(ctx)
	}
	for attempt := 0; ; attempt++ {
		current, version, err := k.replicas(ctx)
		if err != nil {
			return err
		}
		if current >= n {
			return nil
		}
		err = k.scale(ctx, n, version)
		if !errors.Is(err, errConflict) || attempt == 4 {
			return err
		}
	}
}

// errConflict means the Deployment's scale changed since it was read.
var errConflict = errors.New("conflict")

// replicas reads the Deployment's replica count and the version of its
// scale.
func (k *Kubernetes) replicas(ctx context.Context) (int, string, error) {
	req, err := k.request(ctx, http.MethodGet, "")
	if err != nil {
		return 0, "", err
	}
	resp, err := k.client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("read deployment %s/%s: %w", k.Namespace, k.Deployment, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return 0, "", fmt.Errorf("read deployment %s/%s: %s: %s", k.Namespace, k.Deployment, resp.Status, strings.TrimSpace(string(msg)))
	}
	var scale struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
		} `json:"metadata"`
		Spec struct {
			Replicas int `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&scale); err != nil {
		return 0, "", fmt.Errorf("read deployment %s/%s: %w", k.Namespace, k.Deployment, err)
	}
	return scale.Spec.Replicas, scale.Metadata.ResourceVersion, nil
}

// scale sets the Deployment's replicas; with version, only if its scale is
// still at that version.
func (k *Kubernetes) scale(ctx context.Context, replicas int, version string) error {
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	if version != "" {
		v, _ := json.Marshal(version)
		body = fmt.Sprintf(`{"metadata":{"resourceVersion":%s},"spec":{"replicas":%d}}`, v, replicas)
	}
	req, err := k.request(ctx, http.MethodPatch, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/merge-patch+json")
	resp, err := k.client.Do(req)
	if err != nil {
		return fmt.Errorf("scale deployment %s/%s: %w", k.Namespace, k.Deployment, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	if resp.StatusCode == http.StatusConflict && version != "" {
		return errConflict
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return fmt.Errorf("scale deployment %s/%s to %d: %s: %s", k.Namespace, k.Deployment, replicas, resp.Status, strings.TrimSpace(string(msg)))
	}
	return nil
}

// request builds a request for the Deployment's scale subresource.
func (k *Kubernetes) request(ctx context.Context, method, body string) (*http.Request, error) {
	// Projected service account tokens rotate; read it each time.
	token, err := os.ReadFile(k.tokenFile)
	if err != nil {
		return nil, fmt.Errorf("read service account token: %w", err)
	}
	u := fmt.Sprintf("%s/apis/apps/v1/namespaces/%s/deployments/%s/scale", k.base, k.Namespace, k.Deployment)
	req, err := http.NewRequestWithContext(ctx, method, u, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	return req, nil
}

// AlsoStops makes a driver stop more compute when it stops: the
// distributed-execution workers' Deployments, small and large, so they do
// not outlive the Igloo pods that start them.
type AlsoStops struct {
	Compute
	Others []Compute
}

// Stop stops the compute and then the others.
func (a AlsoStops) Stop(ctx context.Context) error {
	errs := []error{a.Compute.Stop(ctx)}
	for _, o := range a.Others {
		errs = append(errs, o.Stop(ctx))
	}
	return errors.Join(errs...)
}
