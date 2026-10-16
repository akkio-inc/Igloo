package auth

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// Setting is a workload setting, such as a default query tag or a
// concurrency limit, that an admin gave a user, a custom role or a built-in
// role with ALTER ROLE ... SET. Package workload says what each one means.
type Setting struct {
	// TargetType is GranteeUser, GranteeRole (a custom role) or
	// TargetBuiltin (a built-in role, for every user who has it).
	TargetType string
	Target     string
	Name       string
	Value      string
	SetBy      string
	SetAt      time.Time
}

// TargetBuiltin marks a setting on a built-in role.
const TargetBuiltin = "builtin"

// SettingStore keeps workload settings. A user's or custom role's settings
// go when it is dropped. Implementations must be safe for concurrent use.
type SettingStore interface {
	// SetSetting adds or replaces a setting. It fails with ErrNotFound or
	// ErrRoleNotFound when its user or custom role does not exist.
	SetSetting(ctx context.Context, s Setting) error
	// ResetSettings removes the target's settings of the given names, or
	// all of them when names is nil.
	ResetSettings(ctx context.Context, targetType, target string, names []string) error
	// Settings lists every setting, by target and name.
	Settings(ctx context.Context) ([]Setting, error)
	// UserSettings returns the settings that bear on user: their own,
	// their custom roles' and their built-in role's.
	UserSettings(ctx context.Context, user string, role Role) ([]Setting, error)
}

// settingsMigrations create the settings table. Each target is a user, a
// custom role or a built-in role, and a user's or custom role's settings
// are deleted with it.
var settingsMigrations = []string{
	`CREATE TABLE IF NOT EXISTS igloo_auth.role_settings (
		user_name    text REFERENCES igloo_auth.users (name) ON DELETE CASCADE,
		role_name    text REFERENCES igloo_auth.roles (name) ON DELETE CASCADE,
		builtin_role text CHECK (builtin_role IN ('admin', 'writer', 'reader', 'none')),
		setting      text NOT NULL,
		value        text NOT NULL,
		set_by       text NOT NULL DEFAULT '',
		set_at       timestamptz NOT NULL DEFAULT now(),
		CHECK (num_nonnulls(user_name, role_name, builtin_role) = 1)
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS role_settings_target ON igloo_auth.role_settings
		((coalesce(user_name, '')), (coalesce(role_name, '')), (coalesce(builtin_role, '')), setting)`,
}

// targetColumns maps a target type to the column that names it.
var targetColumns = map[string]string{GranteeUser: "user_name", GranteeRole: "role_name", TargetBuiltin: "builtin_role"}

func (s *PostgresStore) SetSetting(ctx context.Context, set Setting) error {
	column, ok := targetColumns[set.TargetType]
	if !ok {
		return errors.New("unknown setting target")
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The names lock keeps the user or role from being dropped and
		// recreated meanwhile, as for grants.
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		if set.TargetType != TargetBuiltin {
			if err := checkGrantee(ctx, tx, set.TargetType, set.Target); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.role_settings (`+column+`, setting, value, set_by)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT ((coalesce(user_name, '')), (coalesce(role_name, '')), (coalesce(builtin_role, '')), setting)
			DO UPDATE SET value = EXCLUDED.value, set_by = EXCLUDED.set_by, set_at = now()`,
			set.Target, set.Name, set.Value, set.SetBy)
		return err
	})
}

func (s *PostgresStore) ResetSettings(ctx context.Context, targetType, target string, names []string) error {
	column, ok := targetColumns[targetType]
	if !ok {
		return errors.New("unknown setting target")
	}
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_auth.role_settings
		WHERE `+column+` = $1 AND ($2::text[] IS NULL OR setting = ANY ($2))`, target, names)
	return err
}

const settingColumns = `CASE WHEN user_name IS NOT NULL THEN 'user' WHEN role_name IS NOT NULL THEN 'role' ELSE 'builtin' END,
	coalesce(user_name, role_name, builtin_role), setting, value, set_by, set_at`

func scanSetting(row pgx.CollectableRow) (Setting, error) {
	var x Setting
	err := row.Scan(&x.TargetType, &x.Target, &x.Name, &x.Value, &x.SetBy, &x.SetAt)
	return x, err
}

func (s *PostgresStore) Settings(ctx context.Context) ([]Setting, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+settingColumns+` FROM igloo_auth.role_settings ORDER BY 1, 2, 3`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanSetting)
}

func (s *PostgresStore) UserSettings(ctx context.Context, user string, role Role) ([]Setting, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+settingColumns+` FROM igloo_auth.role_settings
		WHERE user_name = $1
		   OR role_name IN (SELECT role FROM igloo_auth.role_members WHERE member = $1)
		   OR builtin_role = $2
		ORDER BY 1, 2, 3`, user, string(role))
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanSetting)
}

func (m *MemoryGrants) SetSetting(ctx context.Context, set Setting) error {
	switch set.TargetType {
	case GranteeUser:
		if _, err := m.users.Get(ctx, set.Target); err != nil {
			return err
		}
	case GranteeRole, TargetBuiltin:
	default:
		return errors.New("unknown setting target")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if set.TargetType == GranteeRole && m.roles[set.Target] == nil {
		return ErrRoleNotFound
	}
	if set.SetAt.IsZero() {
		set.SetAt = time.Now()
	}
	m.settings = slices.DeleteFunc(m.settings, func(x Setting) bool { return sameSetting(x, set) })
	m.settings = append(m.settings, set)
	return nil
}

func (m *MemoryGrants) ResetSettings(_ context.Context, targetType, target string, names []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.settings = slices.DeleteFunc(m.settings, func(x Setting) bool {
		return x.TargetType == targetType && x.Target == target && (names == nil || slices.Contains(names, x.Name))
	})
	return nil
}

func (m *MemoryGrants) Settings(context.Context) ([]Setting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := slices.Clone(m.settings)
	sortSettings(out)
	return out, nil
}

func (m *MemoryGrants) UserSettings(_ context.Context, user string, role Role) ([]Setting, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Setting
	for _, x := range m.settings {
		switch {
		case x.TargetType == GranteeUser && x.Target == user,
			x.TargetType == TargetBuiltin && x.Target == string(role),
			x.TargetType == GranteeRole && m.roles[x.Target] != nil && slices.Contains(m.roles[x.Target].Members, user):
			out = append(out, x)
		}
	}
	sortSettings(out)
	return out, nil
}

// forgetSettings removes the settings of a dropped user or custom role.
// The caller holds m.mu.
func (m *MemoryGrants) forgetSettings(targetType, target string) {
	m.settings = slices.DeleteFunc(m.settings, func(x Setting) bool { return x.TargetType == targetType && x.Target == target })
}

func sameSetting(a, b Setting) bool {
	return a.TargetType == b.TargetType && a.Target == b.Target && a.Name == b.Name
}

func sortSettings(s []Setting) {
	sort.Slice(s, func(i, j int) bool {
		a, b := s[i], s[j]
		if a.TargetType != b.TargetType {
			return a.TargetType < b.TargetType
		}
		if a.Target != b.Target {
			return a.Target < b.Target
		}
		return a.Name < b.Name
	})
}

var (
	_ SettingStore = (*PostgresStore)(nil)
	_ SettingStore = (*MemoryGrants)(nil)
)
