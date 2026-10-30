package auth

import (
	"context"
	"errors"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PostgresStore keeps SCIM provisioning in igloo_auth.scim_users,
// igloo_auth.scim_groups, igloo_auth.scim_members and
// igloo_auth.scim_changes.

const scimUserColumns = `user_name, external_id, scim_user_name, display_name, given_name, family_name, created_at, updated_at`

func scanSCIMUser(row pgx.Row) (SCIMUser, error) {
	var u SCIMUser
	err := row.Scan(&u.User, &u.ExternalID, &u.UserName, &u.DisplayName, &u.GivenName, &u.FamilyName, &u.Created, &u.Modified)
	if errors.Is(err, pgx.ErrNoRows) {
		return SCIMUser{}, ErrSCIMNotFound
	}
	return u, err
}

func (s *PostgresStore) SCIMUsers(ctx context.Context) ([]SCIMUser, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+scimUserColumns+` FROM igloo_auth.scim_users ORDER BY user_name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (SCIMUser, error) { return scanSCIMUser(row) })
}

func (s *PostgresStore) SCIMUser(ctx context.Context, user string) (SCIMUser, error) {
	return scanSCIMUser(s.pool.QueryRow(ctx, `SELECT `+scimUserColumns+` FROM igloo_auth.scim_users WHERE user_name = $1`, user))
}

func (s *PostgresStore) PutSCIMUser(ctx context.Context, u SCIMUser) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_auth.scim_users
			(user_name, external_id, scim_user_name, display_name, given_name, family_name)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (user_name) DO UPDATE SET external_id = EXCLUDED.external_id,
			scim_user_name = EXCLUDED.scim_user_name, display_name = EXCLUDED.display_name,
			given_name = EXCLUDED.given_name, family_name = EXCLUDED.family_name, updated_at = now()`,
		u.User, u.ExternalID, u.UserName, u.DisplayName, u.GivenName, u.FamilyName)
	return foreignKeyError(err)
}

func (s *PostgresStore) DeleteSCIMUser(ctx context.Context, user string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.scim_members WHERE user_name = $1`, user); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.scim_users WHERE user_name = $1`, user); err != nil {
			return err
		}
		return markSCIMChanges(ctx, tx, []string{user})
	})
}

// foreignKeyError maps a foreign key violation, a user that does not
// exist, to ErrNotFound.
func foreignKeyError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return ErrNotFound
	}
	return err
}

const scimGroupQuery = `SELECT g.id, g.display_name, g.external_id, g.created_at, g.updated_at,
		coalesce(array_agg(m.user_name ORDER BY m.user_name) FILTER (WHERE m.user_name IS NOT NULL), '{}')
	FROM igloo_auth.scim_groups g LEFT JOIN igloo_auth.scim_members m ON m.group_id = g.id`

func scanSCIMGroup(row pgx.Row) (SCIMGroup, error) {
	var g SCIMGroup
	err := row.Scan(&g.ID, &g.DisplayName, &g.ExternalID, &g.Created, &g.Modified, &g.Members)
	if errors.Is(err, pgx.ErrNoRows) {
		return SCIMGroup{}, ErrSCIMNotFound
	}
	return g, err
}

func (s *PostgresStore) SCIMGroups(ctx context.Context) ([]SCIMGroup, error) {
	rows, err := s.pool.Query(ctx, scimGroupQuery+` GROUP BY g.id ORDER BY g.display_name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (SCIMGroup, error) { return scanSCIMGroup(row) })
}

func (s *PostgresStore) SCIMGroup(ctx context.Context, id string) (SCIMGroup, error) {
	return scanSCIMGroup(s.pool.QueryRow(ctx, scimGroupQuery+` WHERE g.id = $1 GROUP BY g.id`, id))
}

func (s *PostgresStore) CreateSCIMGroup(ctx context.Context, g SCIMGroup) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO igloo_auth.scim_groups (id, display_name, external_id) VALUES ($1, $2, $3)`,
			g.ID, g.DisplayName, g.ExternalID); err != nil {
			return err
		}
		if err := setSCIMMembers(ctx, tx, g.ID, g.Members); err != nil {
			return err
		}
		return markSCIMChanges(ctx, tx, g.Members)
	})
	return scimGroupError(err)
}

func (s *PostgresStore) ModifySCIMGroup(ctx context.Context, id string, change func(*SCIMGroup) error) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The row lock orders concurrent changes of one group.
		var one int
		err := tx.QueryRow(ctx, `SELECT 1 FROM igloo_auth.scim_groups WHERE id = $1 FOR UPDATE`, id).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSCIMNotFound
		}
		if err != nil {
			return err
		}
		g, err := scanSCIMGroup(tx.QueryRow(ctx, scimGroupQuery+` WHERE g.id = $1 GROUP BY g.id`, id))
		if err != nil {
			return err
		}
		before := slices.Clone(g.Members)
		if err := change(&g); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE igloo_auth.scim_groups SET display_name = $2, external_id = $3, updated_at = now()
			WHERE id = $1`, id, g.DisplayName, g.ExternalID); err != nil {
			return err
		}
		if err := setSCIMMembers(ctx, tx, id, g.Members); err != nil {
			return err
		}
		return markSCIMChanges(ctx, tx, append(before, g.Members...))
	})
	return scimGroupError(err)
}

// setSCIMMembers makes members the members of group id.
func setSCIMMembers(ctx context.Context, tx pgx.Tx, id string, members []string) error {
	if members == nil {
		members = []string{}
	}
	if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.scim_members WHERE group_id = $1 AND NOT (user_name = ANY ($2))`, id, members); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.scim_members (group_id, user_name)
		SELECT $1, unnest($2::text[]) ON CONFLICT DO NOTHING`, id, members)
	return err
}

// scimGroupError gives group errors this package's meanings.
func scimGroupError(err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return ErrSCIMGroupExists
	}
	return foreignKeyError(err)
}

func (s *PostgresStore) DeleteSCIMGroup(ctx context.Context, id string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The row lock keeps members from joining before it goes.
		var one int
		err := tx.QueryRow(ctx, `SELECT 1 FROM igloo_auth.scim_groups WHERE id = $1 FOR UPDATE`, id).Scan(&one)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrSCIMNotFound
		}
		if err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `SELECT user_name FROM igloo_auth.scim_members WHERE group_id = $1`, id)
		if err != nil {
			return err
		}
		members, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.scim_groups WHERE id = $1`, id); err != nil {
			return err
		}
		return markSCIMChanges(ctx, tx, members)
	})
}

// markSCIMChanges records, in tx, that the groups of users changed. The
// mark is the transaction's ID, which no other change has. Changes mark
// users after changing members, and in name order, so that concurrent
// ones take their locks in one order.
func markSCIMChanges(ctx context.Context, tx pgx.Tx, users []string) error {
	if users == nil {
		users = []string{}
	}
	_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.scim_changes (user_name, mark)
		SELECT name, txid_current() FROM igloo_auth.users WHERE name = ANY ($1) ORDER BY name
		ON CONFLICT (user_name) DO UPDATE SET mark = EXCLUDED.mark`, users)
	return err
}

func (s *PostgresStore) SCIMChanges(ctx context.Context) ([]SCIMChange, error) {
	rows, err := s.pool.Query(ctx, `SELECT user_name, mark FROM igloo_auth.scim_changes ORDER BY user_name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (SCIMChange, error) {
		var c SCIMChange
		err := row.Scan(&c.User, &c.Mark)
		return c, err
	})
}

func (s *PostgresStore) SCIMChangeApplied(ctx context.Context, c SCIMChange) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_auth.scim_changes WHERE user_name = $1 AND mark = $2`, c.User, c.Mark)
	return err
}

func (s *PostgresStore) UserSCIMGroups(ctx context.Context, user string) ([]string, error) {
	rows, err := s.db(ctx).Query(ctx, `SELECT g.display_name FROM igloo_auth.scim_members m
		JOIN igloo_auth.scim_groups g ON g.id = m.group_id WHERE m.user_name = $1 ORDER BY 1`, user)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

func (s *PostgresStore) SCIMGroupNames(ctx context.Context, names []string) ([]string, error) {
	if names == nil {
		names = []string{}
	}
	rows, err := s.db(ctx).Query(ctx, `SELECT display_name FROM igloo_auth.scim_groups WHERE display_name = ANY ($1) ORDER BY 1`, names)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

var _ SCIMStore = (*PostgresStore)(nil)
