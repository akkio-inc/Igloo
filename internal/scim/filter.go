package scim

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
)

// maxPage bounds how many resources one list returns.
const maxPage = 200

// filter is the one kind of SCIM filter identity providers send to find a
// resource: attribute eq "value".
type filter struct {
	// attr is the attribute's name, lowercase, without its schema.
	attr  string
	value string
}

// parseFilter reads a filter, or returns nil for none. Only eq on one
// attribute is supported.
func parseFilter(s string) (*filter, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	i := strings.Index(strings.ToLower(s), " eq ")
	if i <= 0 {
		return nil, errors.New("only filters of the form attribute eq \"value\" are supported")
	}
	attr := strings.ToLower(strings.TrimSpace(s[:i]))
	for _, urn := range []string{schemaUser, schemaGroup} {
		attr = strings.TrimPrefix(attr, strings.ToLower(urn)+":")
	}
	if attr == "" || strings.ContainsAny(attr, " []()\"") {
		return nil, errors.New("only filters of the form attribute eq \"value\" are supported")
	}
	raw := strings.TrimSpace(s[i+4:])
	var value any
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		return nil, errors.New("the filter's value must be a JSON string, number or boolean")
	}
	f := &filter{attr: attr}
	switch v := value.(type) {
	case string:
		f.value = v
	case bool, float64:
		f.value = raw
	default:
		return nil, errors.New("the filter's value must be a JSON string, number or boolean")
	}
	return f, nil
}

// page reads startIndex and count: where a list starts (from 1) and how
// many resources it returns at most.
func page(r *http.Request) (start, count int) {
	start, count = 1, 100
	if n, err := strconv.Atoi(r.URL.Query().Get("startIndex")); err == nil && n > 1 {
		start = n
	}
	if n, err := strconv.Atoi(r.URL.Query().Get("count")); err == nil {
		count = max(0, min(n, maxPage))
	}
	return start, count
}

// slice returns the page of items that starts at start (from 1).
func slice[T any](items []T, start, count int) []T {
	if start > len(items) {
		return nil
	}
	end := min(len(items), start-1+count)
	return items[start-1 : end]
}

// excluded reports whether the request's excludedAttributes names attr.
func excluded(r *http.Request, attr string) bool {
	for _, a := range strings.Split(r.URL.Query().Get("excludedAttributes"), ",") {
		if strings.EqualFold(strings.TrimSpace(a), attr) {
			return true
		}
	}
	return false
}
