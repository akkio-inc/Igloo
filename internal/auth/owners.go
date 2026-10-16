package auth

import (
	"context"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
)

// Ownership names the user or custom role that owns a schema, table or
// view of the Iceberg catalog: by default whoever created it. An owner is
// who answers for the object. Owning one lets the owner set its comments
// and hand it to another owner, and nothing else: reading, writing,
// changing and dropping it take roles and grants, as for everyone.
type Ownership struct {
	Object Object
	Owner  string
	// OwnerType is GranteeUser or GranteeRole.
	OwnerType string
	SetBy     string
	SetAt     time.Time
}

// OwnerStore keeps the owners of the catalog's objects. They follow the
// objects as grants do: the GrantStore's ForgetObject and RenameObject
// drop and move them too, and dropping a user or a custom role drops what
// it owned, which then has no owner.
type OwnerStore interface {
	// SetObjectOwner records o. With keep, an owner recorded already stays. It
	// fails with ErrNotFound or ErrRoleNotFound if the owner does not
	// exist.
	SetObjectOwner(ctx context.Context, o Ownership, keep bool) error
	// ObjectOwners lists every owner recorded.
	ObjectOwners(ctx context.Context) ([]Ownership, error)
}

// OwnerOf returns the owner of o among owners: the table's own, for a table
// or view.
func OwnerOf(owners []Ownership, o Object) (Ownership, bool) {
	i := slices.IndexFunc(owners, func(w Ownership) bool { return w.Object.Equal(o) && w.Object.IsSchema() == o.IsSchema() })
	if i < 0 {
		return Ownership{}, false
	}
	return owners[i], true
}

// Owns reports whether the user with custom roles roles owns o: directly,
// or through one of the roles.
func (w Ownership) Owns(user string, roles []string) bool {
	if w.OwnerType == GranteeRole {
		return slices.Contains(roles, w.Owner)
	}
	return w.Owner == user
}

func (m *MemoryGrants) SetObjectOwner(ctx context.Context, o Ownership, keep bool) error {
	if o.OwnerType == GranteeUser {
		if _, err := m.users.Get(ctx, o.Owner); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if o.OwnerType == GranteeRole && m.roles[o.Owner] == nil {
		return ErrRoleNotFound
	}
	if o.SetAt.IsZero() {
		o.SetAt = time.Now()
	}
	for i, w := range m.owners {
		if w.Object.Equal(o.Object) && w.Object.IsSchema() == o.Object.IsSchema() {
			if !keep {
				m.owners[i] = o
			}
			return nil
		}
	}
	m.owners = append(m.owners, o)
	return nil
}

func (m *MemoryGrants) ObjectOwners(context.Context) ([]Ownership, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.owners), nil
}

// dropOwners removes the owners that drop matches. m.mu must be held.
func (m *MemoryGrants) dropOwners(drop func(Ownership) bool) {
	m.owners = slices.DeleteFunc(m.owners, drop)
}

func (s *PostgresStore) SetObjectOwner(ctx context.Context, o Ownership, keep bool) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := lockNames(ctx, tx); err != nil {
			return err
		}
		if err := checkGrantee(ctx, tx, o.OwnerType, o.Owner); err != nil {
			return err
		}
		conflict := `DO UPDATE SET schema_name = EXCLUDED.schema_name, table_name = EXCLUDED.table_name,
			owner = EXCLUDED.owner, owner_type = EXCLUDED.owner_type, set_by = EXCLUDED.set_by, set_at = now()`
		if keep {
			conflict = "DO NOTHING"
		}
		_, err := tx.Exec(ctx, `INSERT INTO igloo_auth.owners (schema_name, table_name, owner, owner_type, set_by)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (lower(schema_name), lower(table_name)) `+conflict,
			o.Object.Schema, o.Object.Table, o.Owner, o.OwnerType, o.SetBy)
		return err
	})
}

func (s *PostgresStore) ObjectOwners(ctx context.Context) ([]Ownership, error) {
	rows, err := s.pool.Query(ctx, `SELECT schema_name, table_name, owner, owner_type, set_by, set_at
		FROM igloo_auth.owners ORDER BY lower(schema_name), lower(table_name)`)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (Ownership, error) {
		var o Ownership
		err := row.Scan(&o.Object.Schema, &o.Object.Table, &o.Owner, &o.OwnerType, &o.SetBy, &o.SetAt)
		return o, err
	})
}

// forgetOwned drops the ownership of what a dropped user or role owned.
func forgetOwned(ctx context.Context, tx pgx.Tx, ownerType, name string) error {
	_, err := tx.Exec(ctx, `DELETE FROM igloo_auth.owners WHERE owner_type = $1 AND owner = $2`, ownerType, name)
	return err
}

var (
	_ OwnerStore = (*MemoryGrants)(nil)
	_ OwnerStore = (*PostgresStore)(nil)
)
