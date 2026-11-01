package cleanroom

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

type noRecipients struct{}

func (noRecipients) Recipient(*http.Request) (string, error) {
	return "", errors.New("no such recipient")
}

// Without TLS, the API answers only requests to localhost from this
// machine or a private network: one from across the internet that names
// localhost in its Host header used to be let through to authentication.
func TestAPIWithoutTLSOnlyFromNearby(t *testing.T) {
	api := NewAPI(nil, noRecipients{}, nil, 0, nil)
	for peer, want := range map[string]int{
		"127.0.0.1:50123":     http.StatusUnauthorized,
		"172.18.0.1:50123":    http.StatusUnauthorized,
		"198.51.100.23:50123": http.StatusForbidden,
	} {
		r := httptest.NewRequest("GET", "http://localhost:8444"+APIPrefix+"/", nil)
		r.RemoteAddr = peer
		w := httptest.NewRecorder()
		api.ServeHTTP(w, r)
		if w.Code != want {
			t.Errorf("from %s: %d %s", peer, w.Code, w.Body)
		}
	}
}
