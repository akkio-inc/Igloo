package auth

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Grants on connections to other databases (see package federation), their
// schemas and their tables live apart from the Iceberg catalog's, in
// igloo_auth.connection_grants. A replica of a version from before
// connections reads only igloo_auth.grants, so during an upgrade it cannot
// mistake a grant on crm.public.customers for one on the Iceberg table
// public.customers. Only SELECT is granted: connections are read-only.
//
// An empty schema_name grants on the whole connection, an empty table_name
// on the whole schema.
const connectionGrantsTable = `CREATE TABLE IF NOT EXISTS igloo_auth.connection_grants (
	grantee         text NOT NULL,
	grantee_type    text NOT NULL CHECK (grantee_type IN ('user', 'role')),
	privilege       text NOT NULL CHECK (privilege IN ('SELECT')),
	connection_name text NOT NULL,
	schema_name     text NOT NULL DEFAULT '',
	table_name      text NOT NULL DEFAULT '',
	granted_by      text NOT NULL DEFAULT '',
	granted_at      timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (grantee_type, grantee, privilege, connection_name, schema_name, table_name)
)`

const connectionGrantColumns = `grantee, grantee_type, privilege, connection_name, schema_name, table_name, granted_by, granted_at`

func scanConnectionGrant(row pgx.CollectableRow) (Grant, error) {
	var (
		g    Grant
		priv string
	)
	err := row.Scan(&g.Grantee, &g.GranteeType, &priv, &g.Object.Catalog, &g.Object.Schema, &g.Object.Table, &g.GrantedBy, &g.GrantedAt)
	g.Privilege = Privilege(priv)
	return g, err
}

// grantConnection adds one grant on a connection's object, unless an equal
// one exists.
func grantConnection(ctx context.Context, tx pgx.Tx, g Grant) error {
	_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.connection_grants
		(grantee, grantee_type, privilege, connection_name, schema_name, table_name, granted_by)
		SELECT $1, $2, $3, $4, $5, $6, $7
		WHERE NOT EXISTS (SELECT 1 FROM igloo_auth.connection_grants WHERE grantee = $1 AND grantee_type = $2
			AND privilege = $3 AND lower(connection_name) = lower($4) AND lower(schema_name) = lower($5)
			AND lower(table_name) = lower($6))`,
		g.Grantee, g.GranteeType, string(g.Privilege), g.Object.Catalog, g.Object.Schema, g.Object.Table, g.GrantedBy)
	return err
}

// revokeConnection removes one grant on a connection's object.
func revokeConnection(ctx context.Context, tx pgx.Tx, g Grant) error {
	_, err := tx.Exec(ctx, `DELETE FROM igloo_auth.connection_grants WHERE grantee = $1 AND grantee_type = $2
		AND privilege = $3 AND lower(connection_name) = lower($4) AND lower(schema_name) = lower($5)
		AND lower(table_name) = lower($6)`,
		g.Grantee, g.GranteeType, string(g.Privilege), g.Object.Catalog, g.Object.Schema, g.Object.Table)
	return err
}

// forgetGrantee removes every grant on connections to a user or role.
func forgetGrantee(ctx context.Context, tx pgx.Tx, granteeType, name string) error {
	_, err := tx.Exec(ctx, `DELETE FROM igloo_auth.connection_grants WHERE grantee_type = $1 AND grantee = $2`, granteeType, name)
	return err
}

// forgetConnectionObject removes the grants on a connection's object and
// on everything in it.
func (s *PostgresStore) forgetConnectionObject(ctx context.Context, o Object) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM igloo_auth.connection_grants WHERE lower(connection_name) = lower($1)
		AND ($2 = '' OR lower(schema_name) = lower($2)) AND ($3 = '' OR lower(table_name) = lower($3))`,
		o.Catalog, o.Schema, o.Table)
	return err
}
