// Package workload decides what a statement runs under: the query tag that
// attributes it to a client or campaign, and the limits an admin set for
// its user's roles. Settings come from ALTER ROLE ... SET (stored by
// auth.SettingStore); Controller enforces the limits on each replica.
package workload

import (
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/config"
)

// The settings ALTER ROLE and ALTER USER can set.
const (
	// QueryTag tags the statements of sessions that set no tag of their own.
	QueryTag = "query_tag"
	// StatementTimeout is the longest a statement may run, wherever the
	// session's own statement_timeout is longer or unset.
	StatementTimeout = "statement_timeout"
	// MaxConcurrentStatements is how many statements may run at once on a
	// replica: per user when set on a user, for all members together when
	// set on a role.
	MaxConcurrentStatements = "max_concurrent_statements"
	// MemoryLimit is the memory each statement reserves from the replica's
	// query memory before it starts.
	MemoryLimit = "memory_limit"
	// SQLDialect is the SQL dialect of sessions that choose none of their
	// own: duckdb or snowflake.
	SQLDialect = "sql_dialect"
)

// Names lists the settings.
var Names = []string{QueryTag, StatementTimeout, MaxConcurrentStatements, MemoryLimit, SQLDialect}

// MaxQueryTag bounds a query tag's length in bytes, as Snowflake's does.
const MaxQueryTag = 2000

// maxConcurrent bounds max_concurrent_statements.
const maxConcurrent = 10_000

// Normalize checks value for the setting name and returns it as stored.
func Normalize(name, value string) (string, error) {
	switch name {
	case QueryTag:
		return value, CheckQueryTag(value)
	case StatementTimeout:
		d, err := ParseTimeout(value)
		if err != nil {
			return "", err
		}
		return FormatTimeout(d), nil
	case MaxConcurrentStatements:
		n, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil || n < 1 || n > maxConcurrent {
			return "", fmt.Errorf("max_concurrent_statements must be a whole number from 1 to %d, got %q", maxConcurrent, value)
		}
		return strconv.Itoa(n), nil
	case MemoryLimit:
		n, err := config.ParseSize(value)
		if err != nil || n <= 0 {
			return "", fmt.Errorf("memory_limit must be a size such as 4GB or 512MiB, got %q", value)
		}
		return strings.TrimSpace(value), nil
	case SQLDialect:
		switch d := strings.ToLower(strings.TrimSpace(value)); d {
		case "duckdb", "snowflake":
			return d, nil
		}
		return "", fmt.Errorf("sql_dialect must be duckdb or snowflake, got %q", value)
	}
	return "", fmt.Errorf("unknown setting %q; use %s", name, strings.Join(Names, ", "))
}

// CheckQueryTag reports whether tag can be a query tag.
func CheckQueryTag(tag string) error {
	if len(tag) > MaxQueryTag {
		return fmt.Errorf("query_tag is at most %d bytes", MaxQueryTag)
	}
	if strings.ContainsRune(tag, 0) {
		return errors.New("query_tag cannot contain NUL characters")
	}
	return nil
}

var timeoutValue = regexp.MustCompile(`(?i)^(\d+)\s*(us|ms|s|min|h|d)?$`)

// ParseTimeout parses a PostgreSQL statement_timeout value: a number of
// milliseconds, or a number with a unit ('30s', '5min'). Zero means none.
func ParseTimeout(value string) (time.Duration, error) {
	m := timeoutValue.FindStringSubmatch(strings.TrimSpace(strings.Trim(strings.TrimSpace(value), `'"`)))
	if m == nil {
		return 0, errors.New(`invalid value for statement_timeout; use milliseconds or a unit, e.g. '30s' or '5min'`)
	}
	n, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return 0, err
	}
	unit := map[string]time.Duration{
		"": time.Millisecond, "us": time.Microsecond, "ms": time.Millisecond, "s": time.Second,
		"min": time.Minute, "h": time.Hour, "d": 24 * time.Hour,
	}[strings.ToLower(m[2])]
	return time.Duration(n) * unit, nil
}

// FormatTimeout renders a timeout the way PostgreSQL's SHOW does.
func FormatTimeout(d time.Duration) string {
	switch {
	case d == 0:
		return "0"
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dmin", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	}
	return fmt.Sprintf("%dms", d/time.Millisecond)
}

// Limits are the settings that apply to one user's statements.
type Limits struct {
	// QueryTag tags their statements when the session sets none.
	QueryTag string
	// StatementTimeout caps every statement; zero means no cap.
	StatementTimeout time.Duration
	// Slots is the concurrency limit their statements count against.
	Slots Slots
	// Memory is what each statement reserves, in bytes; zero means none.
	Memory int64
	// SQLDialect is the dialect of sessions that choose none; "" means
	// DuckDB's.
	SQLDialect string
}

// Slots is a limit on statements running at once, shared by everyone
// whose limit comes from the same user or role.
type Slots struct {
	// Key names where the limit was set, such as "role analysts".
	Key string
	// Max is the limit; zero means none.
	Max int
}

// Limited reports whether anything holds the user's statements back
// before they start.
func (l Limits) Limited() bool { return l.Slots.Max > 0 || l.Memory > 0 }

// Resolve works out the limits of user, who has the custom roles roles,
// from the settings that bear on them. For each setting, the most specific
// target that sets it wins: the user, then their custom roles, then their
// built-in role. Where several custom roles set one, the most generous
// applies (the longest timeout, the most statements at once, the smallest
// reservation), and for the query tag and SQL dialect the first role by
// name. Values that
// do not parse are ignored; they were checked when set.
func Resolve(user auth.User, roles []string, settings []auth.Setting) Limits {
	var l Limits
	for _, name := range Names {
		var level []auth.Setting
		for _, targetType := range []string{auth.GranteeUser, auth.GranteeRole, auth.TargetBuiltin} {
			level = level[:0]
			for _, s := range settings {
				if s.Name == name && applies(s, targetType, user, roles) {
					level = append(level, s)
				}
			}
			if len(level) > 0 {
				break
			}
		}
		if len(level) == 0 {
			continue
		}
		slices.SortFunc(level, func(a, b auth.Setting) int { return strings.Compare(a.Target, b.Target) })
		switch name {
		case QueryTag:
			l.QueryTag = level[0].Value
		case SQLDialect:
			l.SQLDialect = level[0].Value
		case StatementTimeout:
			// Zero, no limit, is the most generous of all.
			for _, s := range level {
				d, err := ParseTimeout(s.Value)
				if err != nil {
					continue
				}
				if d == 0 {
					l.StatementTimeout = 0
					break
				}
				l.StatementTimeout = max(l.StatementTimeout, d)
			}
		case MaxConcurrentStatements:
			for _, s := range level {
				if n, err := strconv.Atoi(s.Value); err == nil && n > l.Slots.Max {
					l.Slots = Slots{Key: targetName(s), Max: n}
				}
			}
		case MemoryLimit:
			for _, s := range level {
				if n, err := config.ParseSize(s.Value); err == nil && n > 0 && (l.Memory == 0 || n < l.Memory) {
					l.Memory = n
				}
			}
		}
	}
	return l
}

func applies(s auth.Setting, targetType string, user auth.User, roles []string) bool {
	if s.TargetType != targetType {
		return false
	}
	switch targetType {
	case auth.GranteeUser:
		return s.Target == user.Name
	case auth.GranteeRole:
		return slices.Contains(roles, s.Target)
	}
	return s.Target == string(user.Role)
}

// targetName names a setting's target for messages: "user ana", "role
// analysts" or "built-in role reader".
func targetName(s auth.Setting) string {
	switch s.TargetType {
	case auth.GranteeUser:
		return "user " + s.Target
	case auth.GranteeRole:
		return "role " + s.Target
	}
	return "built-in role " + s.Target
}
