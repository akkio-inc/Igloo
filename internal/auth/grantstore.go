package auth

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// PostgresStore keeps custom roles and grants next to the users, in
// igloo_auth.roles, igloo_auth.role_members and igloo_auth.grants.

// lockNames takes the transaction-scoped lock that orders changes to the
// names of users and roles and the grants that name them.
func lockNames(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", namesLock)
	return err
}

func (s *PostgresStore) CreateRole(ctx context.Context, name string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		var isUser bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM igloo_auth.users WHERE name = $1)`, name).Scan(&isUser); err != nil {
			return err
		}
		if isUser {
			return ErrNameTaken
		}
		tag, err := tx.Exec(ctx, `INSERT INTO igloo_auth.roles (name) VALUES ($1) ON CONFLICT (name) DO NOTHING`, name)
		if err == nil && tag.RowsAffected() == 0 {
			return ErrRoleExists
		}
		return err
	})
}

func (s *PostgresStore) DropRole(ctx context.Context, name string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `DELETE FROM igloo_auth.roles WHERE name = $1`, name)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return ErrRoleNotFound
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.sso_group_roles WHERE role = $1`, name); err != nil {
			return err
		}
		if err := forgetOwned(ctx, tx, GranteeRole, name); err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `DELETE FROM igloo_auth.grants WHERE grantee_type = 'role' AND grantee = $1`, name); err != nil {
			return err
		}
		return forgetGrantee(ctx, tx, GranteeRole, name)
	})
}

func (s *PostgresStore) Roles(ctx context.Context) ([]CustomRole, error) {
	rows, err := s.pool.Query(ctx, `SELECT r.name, r.created_at,
			coalesce(array_agg(m.member ORDER BY m.member) FILTER (WHERE m.member IS NOT NULL), '{}')
		FROM igloo_auth.roles r LEFT JOIN igloo_auth.role_members m ON m.role = r.name
		GROUP BY r.name, r.created_at ORDER BY r.name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (CustomRole, error) {
		var r CustomRole
		err := row.Scan(&r.Name, &r.CreatedAt, &r.Members)
		return r, err
	})
}

func (s *PostgresStore) AddMember(ctx context.Context, role, user, by string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		if err := checkGrantee(ctx, tx, GranteeRole, role); err != nil {
			return err
		}
		if err := checkGrantee(ctx, tx, GranteeUser, user); err != nil {
			return err
		}
		// A membership single sign-on manages becomes the admin's, so the
		// next sign-in does not remove it.
		_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.role_members (role, member, granted_by) VALUES ($1, $2, $3)
			ON CONFLICT (role, member) DO UPDATE SET granted_by = EXCLUDED.granted_by, granted_at = now()
			WHERE igloo_auth.role_members.granted_by = $4`, role, user, by, SSOGrantor)
		return err
	})
}

func (s *PostgresStore) RemoveMember(ctx context.Context, role, user string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := checkGrantee(ctx, tx, GranteeRole, role); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM igloo_auth.role_members WHERE role = $1 AND member = $2`, role, user)
		return err
	})
}

// checkGrantee returns ErrNotFound or ErrRoleNotFound unless the user or
// role exists.
func checkGrantee(ctx context.Context, tx pgx.Tx, kind, name string) error {
	table, missing := "igloo_auth.users", ErrNotFound
	if kind == GranteeRole {
		table, missing = "igloo_auth.roles", ErrRoleNotFound
	}
	var ok bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM `+table+` WHERE name = $1)`, name).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return missing
	}
	return nil
}

func (s *PostgresStore) Grant(ctx context.Context, grants []Grant) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		for _, g := range grants {
			if err := checkGrantee(ctx, tx, g.GranteeType, g.Grantee); err != nil {
				return err
			}
			if g.Object.Catalog != "" {
				if err := grantConnection(ctx, tx, g); err != nil {
					return err
				}
				continue
			}
			_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.grants
				(grantee, grantee_type, privilege, schema_name, table_name, granted_by)
				SELECT $1, $2, $3, $4, $5, $6
				WHERE NOT EXISTS (SELECT 1 FROM igloo_auth.grants WHERE grantee = $1 AND grantee_type = $2
					AND privilege = $3 AND lower(schema_name) = lower($4) AND lower(table_name) = lower($5))`,
				g.Grantee, g.GranteeType, string(g.Privilege), g.Object.Schema, g.Object.Table, g.GrantedBy)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) Revoke(ctx context.Context, grants []Grant) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, g := range grants {
			if g.Object.Catalog != "" {
				if err := revokeConnection(ctx, tx, g); err != nil {
					return err
				}
				continue
			}
			_, err := tx.Exec(ctx, `DELETE FROM igloo_auth.grants WHERE grantee = $1 AND grantee_type = $2
				AND privilege = $3 AND lower(schema_name) = lower($4) AND lower(table_name) = lower($5)`,
				g.Grantee, g.GranteeType, string(g.Privilege), g.Object.Schema, g.Object.Table)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

const grantColumns = `grantee, grantee_type, privilege, schema_name, table_name, granted_by, granted_at`

func scanGrant(row pgx.CollectableRow) (Grant, error) {
	var (
		g    Grant
		priv string
	)
	err := row.Scan(&g.Grantee, &g.GranteeType, &priv, &g.Object.Schema, &g.Object.Table, &g.GrantedBy, &g.GrantedAt)
	g.Privilege = Privilege(priv)
	return g, err
}

func (s *PostgresStore) Grants(ctx context.Context) ([]Grant, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+grantColumns+` FROM igloo_auth.grants
		ORDER BY grantee, schema_name, table_name, privilege`)
	if err != nil {
		return nil, err
	}
	grants, err := pgx.CollectRows(rows, scanGrant)
	if err != nil {
		return nil, err
	}
	rows, err = s.pool.Query(ctx, `SELECT `+connectionGrantColumns+` FROM igloo_auth.connection_grants
		ORDER BY grantee, connection_name, schema_name, table_name, privilege`)
	if err != nil {
		return nil, err
	}
	more, err := pgx.CollectRows(rows, scanConnectionGrant)
	return append(grants, more...), err
}

func (s *PostgresStore) UserPrivileges(ctx context.Context, user string) (Privileges, error) {
	var p Privileges
	err := pgx.BeginTxFunc(ctx, s.pool, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT role FROM igloo_auth.role_members WHERE member = $1 ORDER BY role`, user)
		if err != nil {
			return err
		}
		if p.Roles, err = pgx.CollectRows(rows, pgx.RowTo[string]); err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT `+grantColumns+` FROM igloo_auth.grants
			WHERE (grantee_type = 'user' AND grantee = $1)
			   OR (grantee_type = 'role' AND grantee = ANY ($2))`, user, p.Roles)
		if err != nil {
			return err
		}
		if p.Grants, err = pgx.CollectRows(rows, scanGrant); err != nil {
			return err
		}
		rows, err = tx.Query(ctx, `SELECT `+connectionGrantColumns+` FROM igloo_auth.connection_grants
			WHERE (grantee_type = 'user' AND grantee = $1)
			   OR (grantee_type = 'role' AND grantee = ANY ($2))`, user, p.Roles)
		if err != nil {
			return err
		}
		more, err := pgx.CollectRows(rows, scanConnectionGrant)
		p.Grants = append(p.Grants, more...)
		return err
	})
	return p, err
}

func (s *PostgresStore) ForgetUser(ctx context.Context, user string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		// A user of that name may exist again already; leave theirs alone.
		var exists bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM igloo_auth.users WHERE name = $1)`, user).Scan(&exists); err != nil || exists {
			return err
		}
		if err := forgetOwned(ctx, tx, GranteeUser, user); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.grants WHERE grantee_type = 'user' AND grantee = $1`, user); err != nil {
			return err
		}
		return forgetGrantee(ctx, tx, GranteeUser, user)
	})
}

func (s *PostgresStore) ForgetObject(ctx context.Context, o Object) error {
	if o.Catalog != "" {
		return s.forgetConnectionObject(ctx, o)
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, table := range []string{"igloo_auth.grants", "igloo_auth.owners"} {
			var err error
			if o.IsSchema() {
				_, err = tx.Exec(ctx, `DELETE FROM `+table+` WHERE lower(schema_name) = lower($1)`, o.Schema)
			} else {
				_, err = tx.Exec(ctx, `DELETE FROM `+table+` WHERE table_name <> ''
					AND lower(schema_name) = lower($1) AND lower(table_name) = lower($2)`, o.Schema, o.Table)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *PostgresStore) RenameObject(ctx context.Context, from, to Object) error {
	if from.Catalog != "" || to.Catalog != "" {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for _, table := range []string{"igloo_auth.grants", "igloo_auth.owners"} {
			// Grants and owners left on the new name belong to a table
			// dropped outside Igloo, not to this one, unless the new name
			// is the old one, or differs from it in case only.
			if _, err := tx.Exec(ctx, `DELETE FROM `+table+` WHERE table_name <> ''
				AND lower(schema_name) = lower($1) AND lower(table_name) = lower($2)
				AND NOT (lower(schema_name) = lower($3) AND lower(table_name) = lower($4))`,
				to.Schema, to.Table, from.Schema, from.Table); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE `+table+` SET schema_name = $3, table_name = $4
				WHERE table_name <> '' AND lower(schema_name) = lower($1) AND lower(table_name) = lower($2)`,
				from.Schema, from.Table, to.Schema, to.Table); err != nil {
				return err
			}
		}
		return nil
	})
}

var _ GrantStore = (*PostgresStore)(nil)
