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
	"slices"
	"strconv"
	"strings"
	"time"
)

// A client of a provider's Delta Sharing server, for external tables.
//
// The protocol has two response formats. In the parquet format, which every
// server speaks, a table's files are plain Parquet whose columns are the
// table's. The delta format wraps Delta log actions and can carry what the
// parquet format cannot: deletion vectors, which mark deleted rows that the
// files still hold, and column mapping, under which the files' columns have
// other names than the table's. Providers answer in the delta format only
// for tables that need it (Databricks for every table with deletion
// vectors enabled, even with nothing deleted), and only to clients that say
// they read it and which of its features. So, as the official clients do,
// Igloo asks for the table's metadata in either format and queries the
// files in the one the provider picked, saying which reader features it
// applies (readerFeatures); a table that needs any other is refused.

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
	version int64
	tableID string
	// format is the response format the provider answered in.
	format string
	schema string
	// partitionColumns name the table's partition columns as its users
	// see them.
	partitionColumns []string
	// columnMapping is Delta's column mapping mode: "" (none), "name" or
	// "id".
	columnMapping string
	files         []remoteFile
}

// remoteFile is a data file of a shared table.
type remoteFile struct {
	ID   string
	URL  string
	Size int64
	// PartitionValues are keyed by the partition columns' names in the
	// files: their physical names under column mapping.
	PartitionValues map[string]*string
	Expires         time.Time
	// DV is the file's deletion vector, if it has one.
	DV *deletionVector
}

const (
	formatParquet = "parquet"
	formatDelta   = "delta"
	// capabilitiesHeader says which response formats and Delta reader
	// features a client reads, and in a response which format it is in.
	capabilitiesHeader = "delta-sharing-capabilities"
)

// readerFeatures are the Delta table features Igloo reads in the delta
// response format. It applies deletion vectors and column mapping;
// timestampNtz needs only DuckDB's TIMESTAMP; v2Checkpoint and
// vacuumProtocolCheck concern the table's log and its maintenance, which
// the provider reads, not the files a reader gets.
var readerFeatures = []string{"deletionVectors", "columnMapping", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck"}

// Capabilities headers: offering either format, for the metadata, and
// asking for one, for a query. Databricks refuses a query that names two
// formats, or reader features with the parquet format.
var (
	offerFormats = "responseformat=parquet,delta;readerfeatures=" + strings.Join(readerFeatures, ",")
	askParquet   = "responseformat=parquet"
	askDelta     = "responseformat=delta;readerfeatures=" + strings.Join(readerFeatures, ",")
)

const (
	// defaultURLLifetime is assumed for file URLs whose expiry the
	// provider does not say.
	defaultURLLifetime = 15 * time.Minute
	// maxRemoteFiles bounds what a provider can make Igloo hold in memory
	// and put in an external table's view.
	maxRemoteFiles = 200_000
)

// providerError is an error response from a provider.
type providerError struct {
	status  int
	message string
}

func (e *providerError) Error() string {
	return fmt.Sprintf("the provider answered HTTP %d: %s", e.status, e.message)
}

// do sends a request, with capabilities as its capabilities header unless
// that is empty.
func (c *client) do(ctx context.Context, method, path string, body any, capabilities string) (*http.Response, error) {
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
	if capabilities != "" {
		req.Header.Set(capabilitiesHeader, capabilities)
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
		resp, err := c.do(ctx, http.MethodGet, p, nil, "")
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
	resp, err := c.do(ctx, http.MethodGet, tablePath(share, schema, name, "version"), nil, "")
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

// format asks the provider which response format it answers the table's
// queries in, offering both in a request for the table's metadata. Servers
// that do not know the delta format answer in the parquet one.
func (c *client) format(ctx context.Context, share, schema, name string) (string, error) {
	resp, err := c.do(ctx, http.MethodGet, tablePath(share, schema, name, "metadata"), nil, offerFormats)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	if f := respondedFormat(resp.Header); f != "" {
		return f, nil
	}
	// Without the header, the protocol action shows the format.
	var first struct {
		Protocol map[string]json.RawMessage `json:"protocol"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&first) == nil && first.Protocol["deltaProtocol"] != nil {
		return formatDelta, nil
	}
	return formatParquet, nil
}

// respondedFormat is the response format a capabilities header names, or
// "" if it names none.
func respondedFormat(h http.Header) string {
	for part := range strings.SplitSeq(h.Get(capabilitiesHeader), ";") {
		k, v, _ := strings.Cut(part, "=")
		if !strings.EqualFold(strings.TrimSpace(k), "responseformat") {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(v)) {
		case formatDelta:
			return formatDelta
		case formatParquet:
			return formatParquet
		}
	}
	return ""
}

// queryLine is a line of a query's answer, in either response format.
type queryLine struct {
	Protocol *struct {
		// The parquet format's.
		MinReaderVersion *int `json:"minReaderVersion"`
		// The delta format's: Delta's protocol action.
		DeltaProtocol *struct {
			MinReaderVersion int      `json:"minReaderVersion"`
			ReaderFeatures   []string `json:"readerFeatures"`
		} `json:"deltaProtocol"`
	} `json:"protocol"`
	Metadata *struct {
		tableMetadata
		// The delta format's: Delta's metadata action.
		DeltaMetadata *tableMetadata `json:"deltaMetadata"`
	} `json:"metaData"`
	File *struct {
		// The parquet format's.
		URL             string             `json:"url"`
		PartitionValues map[string]*string `json:"partitionValues"`
		Size            int64              `json:"size"`
		// Both formats'.
		ID                  string `json:"id"`
		ExpirationTimestamp *int64 `json:"expirationTimestamp"`
		// The delta format's: a Delta log action, which for a table's
		// files is an add action.
		DeletionVectorFileID string                     `json:"deletionVectorFileId"`
		DeltaSingleAction    map[string]json.RawMessage `json:"deltaSingleAction"`
	} `json:"file"`
	EndStream *struct {
		ErrorMessage string `json:"errorMessage"`
	} `json:"endStreamAction"`
}

// tableMetadata is a table's metadata, in both formats.
type tableMetadata struct {
	ID               string            `json:"id"`
	SchemaString     string            `json:"schemaString"`
	PartitionColumns []string          `json:"partitionColumns"`
	Configuration    map[string]string `json:"configuration"`
	Format           struct {
		Provider string `json:"provider"`
	} `json:"format"`
}

// addAction is what a reader needs of Delta's add action.
type addAction struct {
	Path            string             `json:"path"`
	PartitionValues map[string]*string `json:"partitionValues"`
	Size            int64              `json:"size"`
	DeletionVector  *deletionVector    `json:"deletionVector"`
}

// query lists the files of the table's current version, in the response
// format the provider picks for the table.
func (c *client) query(ctx context.Context, share, schema, name string, now time.Time) (*remoteSnapshot, error) {
	format, err := c.format(ctx, share, schema, name)
	if err != nil {
		return nil, err
	}
	ask := askParquet
	if format == formatDelta {
		ask = askDelta
	}
	resp, err := c.do(ctx, http.MethodPost, tablePath(share, schema, name, "query"), map[string]any{}, ask)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	snap := &remoteSnapshot{format: formatParquet}
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
		var action queryLine
		if err := json.Unmarshal(line, &action); err != nil {
			return nil, fmt.Errorf("read the provider's answer: %w", err)
		}
		switch {
		case action.Protocol != nil:
			// The protocol comes first, and its form says the format.
			if p := action.Protocol.DeltaProtocol; p != nil {
				snap.format = formatDelta
				if err := checkDeltaProtocol(p.MinReaderVersion, p.ReaderFeatures); err != nil {
					return nil, err
				}
			} else if v := action.Protocol.MinReaderVersion; v != nil && *v > 1 {
				return nil, fmt.Errorf("the shared table needs a reader of protocol version %d; Igloo reads version 1", *v)
			}
			sawProtocol = true
		case action.Metadata != nil:
			m := &action.Metadata.tableMetadata
			if action.Metadata.DeltaMetadata != nil {
				m = action.Metadata.DeltaMetadata
				snap.format = formatDelta
			}
			if m.Format.Provider != "" && !strings.EqualFold(m.Format.Provider, "parquet") {
				return nil, fmt.Errorf("the shared table's files are %s, not Parquet", m.Format.Provider)
			}
			switch mode := m.Configuration["delta.columnMapping.mode"]; {
			case mode == "" || mode == "none":
			case snap.format != formatDelta:
				return nil, errors.New("the shared table uses Delta column mapping, which needs the delta response format, " +
					"and the provider answered in the parquet format")
			case mode == "name" || mode == "id":
				snap.columnMapping = mode
			default:
				return nil, fmt.Errorf("the shared table uses Delta column mapping mode %q, which Igloo does not read", mode)
			}
			snap.tableID, snap.schema, snap.partitionColumns = m.ID, m.SchemaString, m.PartitionColumns
			sawMetadata = true
		case action.File != nil:
			a := action.File
			f, err := remoteFileOf(a.ID, a.URL, a.Size, a.PartitionValues, a.DeletionVectorFileID, a.DeltaSingleAction)
			if err != nil {
				return nil, err
			}
			f.Expires = now.Add(defaultURLLifetime)
			if a.ExpirationTimestamp != nil {
				f.Expires = time.UnixMilli(*a.ExpirationTimestamp)
			}
			if len(snap.files) == maxRemoteFiles {
				return nil, fmt.Errorf("the shared table has more than %d files, more than Igloo reads through an external table", maxRemoteFiles)
			}
			snap.files = append(snap.files, f)
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
	for _, f := range snap.files {
		if f.DV != nil && snap.format != formatDelta {
			return nil, errors.New("the provider listed a deletion vector in the parquet response format")
		}
	}
	return snap, nil
}

// checkDeltaProtocol refuses a table whose Delta protocol needs a reader
// Igloo is not. Reader version 2 adds column mapping, which the table's
// metadata says whether it uses; version 3 lists the features a reader
// needs.
func checkDeltaProtocol(minReaderVersion int, features []string) error {
	switch {
	case minReaderVersion <= 2:
		return nil
	case minReaderVersion > 3:
		return fmt.Errorf("the shared table needs a Delta reader of protocol version %d; Igloo reads up to version 3", minReaderVersion)
	}
	var missing []string
	for _, f := range features {
		if !slices.Contains(readerFeatures, f) {
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("the shared table uses Delta features Igloo does not read: %s", strings.Join(missing, ", "))
	}
	return nil
}

// remoteFileOf reads a file action: the parquet format's fields, or the
// delta format's add action.
func remoteFileOf(id, fileURL string, size int64, partitions map[string]*string, vectorFileID string, delta map[string]json.RawMessage) (remoteFile, error) {
	f := remoteFile{ID: id, URL: fileURL, Size: size, PartitionValues: partitions}
	if delta != nil {
		raw, ok := delta["add"]
		if !ok || len(delta) != 1 {
			return f, errors.New("the provider listed a change other than an added file among a table's files")
		}
		var add addAction
		if err := json.Unmarshal(raw, &add); err != nil {
			return f, fmt.Errorf("read the provider's answer: %w", err)
		}
		f.URL, f.Size, f.PartitionValues, f.DV = add.Path, add.Size, add.PartitionValues, add.DeletionVector
		if f.DV != nil {
			f.DV.fileID = vectorFileID
			if err := f.DV.check(); err != nil {
				return f, err
			}
		}
	}
	if f.URL == "" || f.ID == "" {
		return f, errors.New("the provider listed a file without a URL or ID")
	}
	if u, err := url.Parse(f.URL); err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return f, errors.New("the provider listed a file whose URL is not an http(s) URL")
	}
	return f, nil
}
