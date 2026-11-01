package auth

import (
	"context"
	"errors"
	"hash/fnv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PostgresStore keeps what account.go describes in igloo_auth.users,
// igloo_auth.identities and igloo_auth.sso_group_roles.

func (s *PostgresStore) SetLogin(ctx context.Context, name string, enabled bool) error {
	if enabled {
		return s.updateUser(ctx, `UPDATE igloo_auth.users SET disabled = false, updated_at = now() WHERE name = $1`, name)
	}
	return s.changeAdmins(ctx, name, func(tx pgx.Tx) (pgconn.CommandTag, error) {
		return tx.Exec(ctx, `UPDATE igloo_auth.users SET disabled = true, epoch = epoch + 1, updated_at = now()
			WHERE name = $1`, name)
	})
}

func (s *PostgresStore) SetOwner(ctx context.Context, name, owner string) error {
	return s.updateUser(ctx, `UPDATE igloo_auth.users SET owner = $2, updated_at = now() WHERE name = $1`, name, owner)
}

func (s *PostgresStore) SetEmail(ctx context.Context, name, email string) error {
	err := s.updateUser(ctx, `UPDATE igloo_auth.users SET email = $2, updated_at = now() WHERE name = $1`, name, email)
	return uniqueError(err, ErrEmailTaken)
}

func (s *PostgresStore) SetPassword(ctx context.Context, name, verifier string, tokenSalt []byte) error {
	return s.updateUser(ctx, `UPDATE igloo_auth.users SET verifier = $2, token_salt = coalesce($3, token_salt),
		updated_at = now() WHERE name = $1`, name, verifier, tokenSalt)
}

func (s *PostgresStore) EnsureTokenSalt(ctx context.Context, name string, salt []byte) ([]byte, error) {
	var stored []byte
	err := s.pool.QueryRow(ctx, `UPDATE igloo_auth.users SET token_salt = coalesce(token_salt, $2)
		WHERE name = $1 RETURNING token_salt`, name, salt).Scan(&stored)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return stored, err
}

func (s *PostgresStore) UserByEmail(ctx context.Context, email string) (User, error) {
	u, err := scanUser(s.pool.QueryRow(ctx, `SELECT `+userColumns+` FROM igloo_auth.users
		WHERE lower(email) = lower($1) AND email <> ''`, NormalizeEmail(email)))
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrNotFound
	}
	return u, err
}

// updateUser runs an UPDATE of one user, returning ErrNotFound if there is
// no such user.
func (s *PostgresStore) updateUser(ctx context.Context, sql, name string, args ...any) error {
	tag, err := s.pool.Exec(ctx, sql, append([]any{name}, args...)...)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return err
}

const identityColumns = `issuer, subject, user_name, email, groups, account_id, created_at, coalesce(last_sign_in, created_at)`

func scanIdentity(row pgx.Row) (Identity, error) {
	var id Identity
	err := row.Scan(&id.Issuer, &id.Subject, &id.User, &id.Email, &id.Groups, &id.AccountID, &id.CreatedAt, &id.LastSignIn)
	if errors.Is(err, pgx.ErrNoRows) {
		return Identity{}, ErrIdentityNotFound
	}
	return id, err
}

func (s *PostgresStore) IdentityBySubject(ctx context.Context, issuer, subject string) (Identity, error) {
	return scanIdentity(s.pool.QueryRow(ctx, `SELECT `+identityColumns+` FROM igloo_auth.identities
		WHERE issuer = $1 AND subject = $2`, issuer, subject))
}

func (s *PostgresStore) IdentityOf(ctx context.Context, user string) (Identity, error) {
	return scanIdentity(s.db(ctx).QueryRow(ctx, `SELECT `+identityColumns+` FROM igloo_auth.identities
		WHERE user_name = $1`, user))
}

func (s *PostgresStore) Identities(ctx context.Context) ([]Identity, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+identityColumns+` FROM igloo_auth.identities ORDER BY user_name`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Identity, error) { return scanIdentity(row) })
}

func (s *PostgresStore) LinkIdentity(ctx context.Context, id Identity) error {
	if id.Groups == nil {
		id.Groups = []string{}
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO igloo_auth.identities (issuer, subject, user_name, email, groups, account_id, last_sign_in)
		VALUES ($1, $2, $3, $4, $5, $6, now())`, id.Issuer, id.Subject, id.User, id.Email, id.Groups, id.AccountID)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return ErrIdentityTaken
		case "23503":
			return ErrNotFound
		}
	}
	return err
}

func (s *PostgresStore) UpdateIdentity(ctx context.Context, id Identity) error {
	if id.Groups == nil {
		id.Groups = []string{}
	}
	tag, err := s.pool.Exec(ctx, `UPDATE igloo_auth.identities SET email = $3, groups = $4, account_id = $5, last_sign_in = now()
		WHERE issuer = $1 AND subject = $2`, id.Issuer, id.Subject, id.Email, id.Groups, id.AccountID)
	if err == nil && tag.RowsAffected() == 0 {
		return ErrIdentityNotFound
	}
	return err
}

func (s *PostgresStore) UnlinkIdentity(ctx context.Context, user string) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `DELETE FROM igloo_auth.identities WHERE user_name = $1`, user)
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		// Console sessions single sign-on opened end with the link.
		_, err = tx.Exec(ctx, `UPDATE igloo_auth.users SET epoch = epoch + 1, updated_at = now() WHERE name = $1`, user)
		return err
	})
}

func (s *PostgresStore) GroupRoles(ctx context.Context) ([]GroupRole, error) {
	rows, err := s.db(ctx).Query(ctx, `SELECT group_name, role, granted_by, granted_at FROM igloo_auth.sso_group_roles
		ORDER BY group_name, role`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (GroupRole, error) {
		var g GroupRole
		err := row.Scan(&g.Group, &g.Role, &g.GrantedBy, &g.GrantedAt)
		return g, err
	})
}

func (s *PostgresStore) GrantGroupRole(ctx context.Context, g GroupRole) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		// The names lock orders this with DROP ROLE, which removes the
		// role's mappings.
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		if !IsBuiltinRole(g.Role) {
			if err := checkGrantee(ctx, tx, GranteeRole, g.Role); err != nil {
				return err
			}
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.sso_group_roles (group_name, role, granted_by) VALUES ($1, $2, $3)
			ON CONFLICT (group_name, role) DO NOTHING`, g.Group, g.Role, g.GrantedBy)
		return err
	})
}

func (s *PostgresStore) RevokeGroupRole(ctx context.Context, group, role string) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_auth.sso_group_roles WHERE group_name = $1 AND role = $2`, group, role)
	return err
}

func (s *PostgresStore) SyncRoles(ctx context.Context, user string, roles []string) error {
	if roles == nil {
		roles = []string{}
	}
	return pgx.BeginFunc(ctx, s.db(ctx), func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM igloo_auth.role_members
			WHERE member = $1 AND granted_by = $2 AND NOT (role = ANY ($3))`, user, SSOGrantor, roles); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.role_members (role, member, granted_by)
			SELECT r.name, u.name, $2 FROM igloo_auth.roles r, igloo_auth.users u
			WHERE r.name = ANY ($3) AND u.name = $1
			ON CONFLICT (role, member) DO NOTHING`, user, SSOGrantor, roles)
		return err
	})
}

// LockRoles implements SSOStore with a transaction advisory lock keyed on
// user.
func (s *PostgresStore) LockRoles(ctx context.Context, user string, fn func(context.Context) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		h := fnv.New64a()
		_, _ = h.Write([]byte("igloo roles " + user))
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(h.Sum64())); err != nil {
			return err
		}
		return fn(context.WithValue(ctx, rolesTx{s}, tx))
	})
}

// rolesTx is the key of the transaction LockRoles runs fn in, in fn's
// context.
type rolesTx struct{ s *PostgresStore }

// querier is what the store queries: its pool, or a transaction.
type querier interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// db is the transaction of LockRoles that ctx runs in, if any, or else the
// pool. The calls that work out and write a user's roles use it: Get,
// IdentityOf, UserSCIMGroups, SCIMGroupNames, GroupRoles, SyncRoles and
// SetRole.
func (s *PostgresStore) db(ctx context.Context) querier {
	if tx, ok := ctx.Value(rolesTx{s}).(pgx.Tx); ok {
		return tx
	}
	return s.pool
}

var (
	_ AccountStore = (*PostgresStore)(nil)
	_ SSOStore     = (*PostgresStore)(nil)
)
