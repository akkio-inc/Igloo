package sharing

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// A client of a provider's Delta Sharing server, for external tables. It
// asks for the Parquet response format, which every server supports.

// parseProfile reads a profile file: the provider's endpoint and the bearer
// token to send it.
func parseProfile(text string) (Provider, error) {
	var raw struct {
		Version        *int    `json:"shareCredentialsVersion"`
		Type           string  `json:"type"`
		Endpoint       string  `json:"endpoint"`
		BearerToken    string  `json:"bearerToken"`
		ExpirationTime *string `json:"expirationTime"`
	}
	dec := json.NewDecoder(strings.NewReader(text))
	if err := dec.Decode(&raw); err != nil {
		return Provider{}, fmt.Errorf("the profile is not a Delta Sharing profile file (JSON): %w", err)
	}
	switch {
	case raw.Version == nil:
		return Provider{}, errors.New("the profile has no shareCredentialsVersion")
	case *raw.Version != 1 && *raw.Version != 2:
		return Provider{}, fmt.Errorf("profile version %d is not supported", *raw.Version)
	case raw.Type != "" && raw.Type != "bearer_token":
		return Provider{}, fmt.Errorf("profiles of type %q are not supported; ask the provider for a bearer token profile", raw.Type)
	case raw.BearerToken == "":
		return Provider{}, errors.New("the profile has no bearerToken")
	}
	endpoint, err := checkEndpoint(raw.Endpoint)
	if err != nil {
		return Provider{}, err
	}
	p := Provider{Endpoint: endpoint, Token: raw.BearerToken}
	if raw.ExpirationTime != nil && *raw.ExpirationTime != "" {
		t, err := time.Parse(time.RFC3339Nano, *raw.ExpirationTime)
		if err != nil {
			return Provider{}, fmt.Errorf("the profile's expirationTime %q is not a time", *raw.ExpirationTime)
		}
		p.TokenExpires = &t
	}
	return p, nil
}

// checkEndpoint validates a provider's endpoint URL. It must use HTTPS, so
// the bearer token is not sent in the clear, unless it is on this machine
// or a private network: a loopback or private address, or a host name
// without dots, such as a Docker or Kubernetes service.
func checkEndpoint(raw string) (string, error) {
	u, err := url.Parse(strings.TrimSuffix(strings.TrimSpace(raw), "/"))
	if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("the profile's endpoint %q must be an https:// URL", raw)
	}
	if u.Scheme == "http" && !privateHost(u.Hostname()) {
		return "", fmt.Errorf("the profile's endpoint %q must use https:// to keep the bearer token private", raw)
	}
	return u.String(), nil
}

func privateHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback() || ip.IsPrivate()
	}
	return host == "localhost" || !strings.Contains(host, ".")
}

// client calls one provider.
type client struct {
	endpoint string
	token    string
	http     *http.Client
}

// remoteTable is a table in a provider's share.
type remoteTable struct {
	Share, Schema, Name string
}

// remoteSnapshot is a provider's answer to a query.
type remoteSnapshot struct {
	version          int64
	tableID          string
	schema           string
	partitionColumns []string
	files            []remoteFile
}

// remoteFile is a data file of a shared table.
type remoteFile struct {
	ID              string
	URL             string
	Size            int64
	PartitionValues map[string]*string
	Expires         time.Time
}

// defaultURLLifetime is assumed for file URLs whose expiry the provider
// does not say.
const defaultURLLifetime = 15 * time.Minute

// providerError is an error response from a provider.
type providerError struct {
	status  int
	message string
}

func (e *providerError) Error() string {
	return fmt.Sprintf("the provider answered HTTP %d: %s", e.status, e.message)
}

func (c *client) do(ctx context.Context, method, path string, body any) (*http.Response, error) {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("User-Agent", "Igloo")
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// The URL holds nothing secret, but keep messages short.
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("could not reach the provider: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close() //nolint:errcheck
		raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		var e struct {
			ErrorCode string `json:"errorCode"`
			Message   string `json:"message"`
		}
		msg := strings.TrimSpace(string(raw))
		if json.Unmarshal(raw, &e) == nil && e.Message != "" {
			msg = e.Message
		}
		return nil, &providerError{status: resp.StatusCode, message: truncate(msg, 500)}
	}
	return resp, nil
}

func pathOf(parts ...string) string {
	var b strings.Builder
	for _, p := range parts {
		b.WriteString("/")
		b.WriteString(url.PathEscape(p))
	}
	return b.String()
}

// list follows a listing's pages.
func (c *client) list(ctx context.Context, path string, item func(json.RawMessage) error) error {
	token := ""
	for page := 0; page < 10_000; page++ {
		p := path + "?maxResults=500"
		if token != "" {
			p += "&pageToken=" + url.QueryEscape(token)
		}
		resp, err := c.do(ctx, http.MethodGet, p, nil)
		if err != nil {
			return err
		}
		var out struct {
			Items         []json.RawMessage `json:"items"`
			NextPageToken string            `json:"nextPageToken"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 64<<20)).Decode(&out)
		resp.Body.Close() //nolint:errcheck
		if err != nil {
			return fmt.Errorf("read the provider's answer: %w", err)
		}
		for _, it := range out.Items {
			if err := item(it); err != nil {
				return err
			}
		}
		if out.NextPageToken == "" || len(out.Items) == 0 {
			return nil
		}
		token = out.NextPageToken
	}
	return errors.New("the provider's listing does not end")
}

// shares lists the provider's shares.
func (c *client) shares(ctx context.Context) ([]string, error) {
	var out []string
	err := c.list(ctx, "/shares", func(raw json.RawMessage) error {
		var s struct {
			Name string `json:"name"`
		}
		if err := json.Unmarshal(raw, &s); err != nil {
			return err
		}
		out = append(out, s.Name)
		return nil
	})
	return out, err
}

// tables lists the tables of one of the provider's shares.
func (c *client) tables(ctx context.Context, share string) ([]remoteTable, error) {
	var out []remoteTable
	err := c.list(ctx, pathOf("shares", share, "all-tables"), func(raw json.RawMessage) error {
		var t struct {
			Name, Schema, Share string
		}
		if err := json.Unmarshal(raw, &t); err != nil {
			return err
		}
		out = append(out, remoteTable{Share: t.Share, Schema: t.Schema, Name: t.Name})
		return nil
	})
	return out, err
}

func tablePath(share, schema, name string, suffix ...string) string {
	return pathOf(append([]string{"shares", share, "schemas", schema, "tables", name}, suffix...)...)
}

// version asks for the table's current version.
func (c *client) version(ctx context.Context, share, schema, name string) (int64, error) {
	resp, err := c.do(ctx, http.MethodGet, tablePath(share, schema, name, "version"), nil)
	if err != nil {
		return 0, err
	}
	resp.Body.Close() //nolint:errcheck
	return headerVersion(resp)
}

func headerVersion(resp *http.Response) (int64, error) {
	v, err := strconv.ParseInt(resp.Header.Get("Delta-Table-Version"), 10, 64)
	if err != nil {
		return 0, errors.New("the provider's answer has no Delta-Table-Version")
	}
	return v, nil
}

// query lists the files of the table's current version.
func (c *client) query(ctx context.Context, share, schema, name string, now time.Time) (*remoteSnapshot, error) {
	resp, err := c.do(ctx, http.MethodPost, tablePath(share, schema, name, "query"), map[string]any{})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	if strings.Contains(strings.ToLower(resp.Header.Get("delta-sharing-capabilities")), "responseformat=delta") {
		return nil, errors.New("the provider answered in the delta response format, which Igloo does not read")
	}
	snap := &remoteSnapshot{}
	if snap.version, err = headerVersion(resp); err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64<<10), 64<<20)
	sawProtocol, sawMetadata := false, false
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var action struct {
			Protocol *struct {
				MinReaderVersion int `json:"minReaderVersion"`
			} `json:"protocol"`
			Metadata *struct {
				ID               string            `json:"id"`
				SchemaString     string            `json:"schemaString"`
				PartitionColumns []string          `json:"partitionColumns"`
				Configuration    map[string]string `json:"configuration"`
				Format           struct {
					Provider string `json:"provider"`
				} `json:"format"`
			} `json:"metaData"`
			File *struct {
				URL                 string             `json:"url"`
				ID                  string             `json:"id"`
				PartitionValues     map[string]*string `json:"partitionValues"`
				Size                int64              `json:"size"`
				ExpirationTimestamp *int64             `json:"expirationTimestamp"`
			} `json:"file"`
			EndStream *struct {
				ErrorMessage string `json:"errorMessage"`
			} `json:"endStreamAction"`
		}
		if err := json.Unmarshal(line, &action); err != nil {
			return nil, fmt.Errorf("read the provider's answer: %w", err)
		}
		switch {
		case action.Protocol != nil:
			if action.Protocol.MinReaderVersion > 1 {
				return nil, fmt.Errorf("the shared table needs a reader of protocol version %d; Igloo reads version 1", action.Protocol.MinReaderVersion)
			}
			sawProtocol = true
		case action.Metadata != nil:
			m := action.Metadata
			if m.Format.Provider != "" && !strings.EqualFold(m.Format.Provider, "parquet") {
				return nil, fmt.Errorf("the shared table's files are %s, not Parquet", m.Format.Provider)
			}
			if mode := m.Configuration["delta.columnMapping.mode"]; mode != "" && mode != "none" {
				return nil, errors.New("the shared table uses Delta column mapping, which needs the delta response format; ask the provider to share it without")
			}
			snap.tableID, snap.schema, snap.partitionColumns = m.ID, m.SchemaString, m.PartitionColumns
			sawMetadata = true
		case action.File != nil:
			f := action.File
			if f.URL == "" || f.ID == "" {
				return nil, errors.New("the provider listed a file without a URL or ID")
			}
			expires := now.Add(defaultURLLifetime)
			if f.ExpirationTimestamp != nil {
				expires = time.UnixMilli(*f.ExpirationTimestamp)
			}
			snap.files = append(snap.files, remoteFile{ID: f.ID, URL: f.URL, Size: f.Size, PartitionValues: f.PartitionValues, Expires: expires})
		case action.EndStream != nil && action.EndStream.ErrorMessage != "":
			return nil, fmt.Errorf("the provider failed while listing files: %s", truncate(action.EndStream.ErrorMessage, 500))
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read the provider's answer: %w", err)
	}
	if !sawProtocol || !sawMetadata {
		return nil, errors.New("the provider's answer has no protocol or metadata")
	}
	return snap, nil
}
