// Package spot learns that the cloud is about to take back the machine
// Igloo runs on. AWS tells a Spot Instance two minutes before it stops or
// terminates it, through the instance metadata service; a worker that
// hears it can finish its tasks and take no more, rather than lose them.
package spot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// EC2 is the address of EC2's instance metadata service.
const EC2 = "http://169.254.169.254"

// ErrNotEC2 says the instance metadata service did not answer, as off EC2,
// or in a container its hop limit keeps from the service.
var ErrNotEC2 = errors.New("no EC2 instance metadata service")

// Notice is an interruption the cloud has scheduled: what it will do to the
// machine (stop, terminate or hibernate) and when.
type Notice struct {
	Action string
	Time   time.Time
}

// Grace is how long a machine that heard n may take to finish its work:
// until margin before the time the cloud gave, and at least floor.
func (n Notice) Grace(margin, floor time.Duration) time.Duration {
	return max(time.Until(n.Time)-margin, floor)
}

// tokenTTL is how long the metadata tokens asked for last; one is asked for
// again an hour before it expires.
const tokenTTL = 6 * time.Hour

// WatchEC2 asks the instance metadata service at base every interval
// whether AWS has scheduled the instance's interruption, and returns the
// notice once it has, or ctx's error once ctx ends. It returns ErrNotEC2
// at once if the service gives no token (IMDSv2) within a few seconds.
func WatchEC2(ctx context.Context, base string, interval time.Duration) (Notice, error) {
	w := &watcher{base: strings.TrimRight(base, "/"), client: &http.Client{Timeout: 3 * time.Second}}
	if err := w.refresh(ctx); err != nil {
		if ctx.Err() != nil {
			return Notice{}, ctx.Err()
		}
		return Notice{}, fmt.Errorf("%w: %v", ErrNotEC2, err)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		if n, ok := w.check(ctx); ok {
			return n, nil
		}
		select {
		case <-ctx.Done():
			return Notice{}, ctx.Err()
		case <-t.C:
		}
	}
}

type watcher struct {
	base    string
	client  *http.Client
	token   string
	expires time.Time
}

// refresh asks for a metadata token.
func (w *watcher) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, w.base+"/latest/api/token", nil)
	if err != nil {
		return err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", fmt.Sprint(int(tokenTTL.Seconds())))
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK || len(body) == 0 {
		return fmt.Errorf("token request: %s", resp.Status)
	}
	w.token, w.expires = string(body), time.Now().Add(tokenTTL-time.Hour)
	return nil
}

// check reports the interruption scheduled, if any. A request that fails
// says nothing; the next check asks again.
func (w *watcher) check(ctx context.Context) (Notice, bool) {
	if time.Now().After(w.expires) {
		if err := w.refresh(ctx); err != nil {
			return Notice{}, false
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.base+"/latest/meta-data/spot/instance-action", nil)
	if err != nil {
		return Notice{}, false
	}
	req.Header.Set("X-aws-ec2-metadata-token", w.token)
	resp, err := w.client.Do(req)
	if err != nil {
		return Notice{}, false
	}
	defer resp.Body.Close() //nolint:errcheck
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized:
		// The token expired early, as after the instance was stopped and
		// started again.
		w.expires = time.Time{}
		return Notice{}, false
	default:
		// 404: nothing scheduled.
		return Notice{}, false
	}
	var action struct {
		Action string `json:"action"`
		Time   string `json:"time"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 4096)).Decode(&action); err != nil || action.Action == "" {
		return Notice{}, false
	}
	at, err := time.Parse(time.RFC3339, action.Time)
	if err != nil {
		// Taken at its word all the same: two minutes from now.
		at = time.Now().Add(2 * time.Minute)
	}
	return Notice{Action: action.Action, Time: at}, true
}

// Instance is what the instance metadata service says of the machine: its
// ID, instance type and region, and whether it is a Spot Instance.
type Instance struct {
	ID, Type, Region string
	Spot             bool
}

// DescribeEC2 asks the instance metadata service at base (IMDSv2) what the
// machine is, for the list price of its kind (package prices). It returns
// ErrNotEC2 if the service gives no token within a few seconds.
func DescribeEC2(ctx context.Context, base string) (Instance, error) {
	w := &watcher{base: strings.TrimRight(base, "/"), client: &http.Client{Timeout: 3 * time.Second}}
	if err := w.refresh(ctx); err != nil {
		return Instance{}, fmt.Errorf("%w: %v", ErrNotEC2, err)
	}
	var in Instance
	var err error
	if in.Type, err = w.get(ctx, "instance-type"); err != nil {
		return Instance{}, err
	}
	in.ID, _ = w.get(ctx, "instance-id")
	in.Region, _ = w.get(ctx, "placement/region")
	lifecycle, _ := w.get(ctx, "instance-life-cycle")
	in.Spot = lifecycle == "spot"
	return in, nil
}

// get reads a value of the instance's metadata.
func (w *watcher) get(ctx context.Context, path string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.base+"/latest/meta-data/"+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token", w.token)
	resp, err := w.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s: %s", path, resp.Status)
	}
	return strings.TrimSpace(string(body)), nil
}
