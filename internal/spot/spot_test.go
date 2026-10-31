package spot

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestWatchEC2 checks that a watch asks for a token and polls the instance
// action with it, says nothing while none is scheduled, and returns the one
// AWS schedules.
func TestWatchEC2(t *testing.T) {
	var polls atomic.Int32
	scheduled := atomic.Bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/latest/api/token":
			if r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") == "" {
				http.Error(rw, "no ttl", http.StatusBadRequest)
				return
			}
			_, _ = rw.Write([]byte("tok"))
		case r.Method == http.MethodGet && r.URL.Path == "/latest/meta-data/spot/instance-action":
			if r.Header.Get("X-aws-ec2-metadata-token") != "tok" {
				http.Error(rw, "", http.StatusUnauthorized)
				return
			}
			if polls.Add(1) < 3 || !scheduled.Load() {
				if polls.Load() >= 2 {
					scheduled.Store(true)
				}
				http.NotFound(rw, r)
				return
			}
			_, _ = rw.Write([]byte(`{"action": "terminate", "time": "2026-10-27T00:15:00Z"}`))
		default:
			http.NotFound(rw, r)
		}
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	n, err := WatchEC2(ctx, srv.URL, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if n.Action != "terminate" || !n.Time.Equal(time.Date(2026, 10, 27, 0, 15, 0, 0, time.UTC)) {
		t.Errorf("notice %+v", n)
	}
	if polls.Load() < 3 {
		t.Errorf("returned after %d polls, before anything was scheduled", polls.Load())
	}
}

// TestWatchEC2Off checks that a watch where no metadata service answers
// ends at once.
func TestWatchEC2Off(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	if _, err := WatchEC2(context.Background(), srv.URL, time.Second); !errors.Is(err, ErrNotEC2) {
		t.Errorf("got %v, want ErrNotEC2", err)
	}
}

// A machine that heard a notice finishes its work until the margin before
// the time given, and at least the floor.
func TestGrace(t *testing.T) {
	n := Notice{Action: "terminate", Time: time.Now().Add(2 * time.Minute)}
	if g := n.Grace(10*time.Second, 30*time.Second); g < 109*time.Second || g > 110*time.Second {
		t.Errorf("two minutes ahead: grace %s", g)
	}
	n.Time = time.Now().Add(20 * time.Second)
	if g := n.Grace(10*time.Second, 30*time.Second); g != 30*time.Second {
		t.Errorf("twenty seconds ahead: grace %s, want the floor", g)
	}
}

// TestDescribeEC2 checks what the metadata service says of the machine.
func TestDescribeEC2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == "/latest/api/token" {
			_, _ = rw.Write([]byte("tok"))
			return
		}
		if r.Header.Get("X-aws-ec2-metadata-token") != "tok" {
			http.Error(rw, "", http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/latest/meta-data/instance-type":
			_, _ = rw.Write([]byte("m7gd.2xlarge"))
		case "/latest/meta-data/placement/region":
			_, _ = rw.Write([]byte("us-east-2"))
		case "/latest/meta-data/instance-life-cycle":
			_, _ = rw.Write([]byte("spot"))
		default:
			http.NotFound(rw, r)
		}
	}))
	defer srv.Close()
	in, err := DescribeEC2(context.Background(), srv.URL)
	if err != nil || in != (Instance{Type: "m7gd.2xlarge", Region: "us-east-2", Spot: true}) {
		t.Errorf("instance %+v, %v", in, err)
	}
	if _, err := DescribeEC2(context.Background(), "http://127.0.0.1:1"); !errors.Is(err, ErrNotEC2) {
		t.Errorf("no metadata service: %v", err)
	}
}
