package clone

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/apache/iceberg-go/table"
)

// Branches and tags are Iceberg snapshot references. Igloo makes, drops
// and reads them, and moves a branch ahead with FastForward; DuckDB
// commits every write to main, so other branches change only through other
// engines.

// Ref is a branch or tag of a table.
type Ref struct {
	Name       string
	Kind       table.RefType
	SnapshotID int64
	// Time is when the snapshot was committed.
	Time time.Time
}

// keepForever is the max-ref-age of the branches and tags Igloo makes:
// they stay until they are dropped, rather than expiring with the
// snapshots they name.
const keepForever = math.MaxInt64

// Refs lists the table's branches and tags, main first.
func (m *Manager) Refs(ctx context.Context, ident table.Identifier) ([]Ref, error) {
	tbl, err := m.cat.LoadTable(ctx, ident)
	if err != nil {
		return nil, err
	}
	var out []Ref
	for name, r := range tbl.Metadata().Refs() {
		ref := Ref{Name: name, Kind: r.SnapshotRefType, SnapshotID: r.SnapshotID}
		if s := tbl.Metadata().SnapshotByID(ref.SnapshotID); s != nil {
			ref.Time = time.UnixMilli(s.TimestampMs).UTC()
		}
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].Name == table.MainBranch) != (out[j].Name == table.MainBranch) {
			return out[i].Name == table.MainBranch
		}
		if out[i].Kind != out[j].Kind {
			return out[i].Kind == table.BranchRef
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// Resolve returns the ID of the snapshot of the table at a.
func (m *Manager) Resolve(ctx context.Context, ident table.Identifier, a At) (int64, error) {
	tbl, err := m.cat.LoadTable(ctx, ident)
	if err != nil {
		return 0, err
	}
	snap, err := a.Pick(tbl)
	if err != nil {
		return 0, err
	}
	if snap == nil {
		return 0, fmt.Errorf("%w: %s has no snapshot", ErrNotFound, strings.Join(ident, "."))
	}
	return snap.SnapshotID, nil
}

// CreateRef makes a branch or tag of the table at the snapshot a picks.
// One of that name exists already: with replace, a branch or tag (as
// kind) is moved; with ifNotExists, nothing happens.
func (m *Manager) CreateRef(ctx context.Context, ident table.Identifier, name string, kind table.RefType, a At, replace, ifNotExists bool) error {
	if name == table.MainBranch {
		return fmt.Errorf("%w: main is the table itself", ErrInvalid)
	}
	tbl, err := m.cat.LoadTable(ctx, ident)
	if err != nil {
		return err
	}
	var existing *int64
	if ref, ok := findRef(tbl, name); ok {
		switch {
		case ifNotExists:
			return nil
		case !replace:
			return fmt.Errorf("%w: %s has a %s %q", ErrExists, strings.Join(ident, "."), ref.SnapshotRefType, name)
		case ref.SnapshotRefType != kind:
			return fmt.Errorf("%w: %q is a %s of %s, not a %s", ErrExists, name, ref.SnapshotRefType, strings.Join(ident, "."), kind)
		}
		existing = &ref.SnapshotID
	}
	snap, err := a.Pick(tbl)
	if err != nil {
		return err
	}
	if snap == nil {
		return fmt.Errorf("%w: %s has no snapshot to name yet", ErrInvalid, strings.Join(ident, "."))
	}
	_, _, err = m.cat.CommitTable(ctx, ident,
		[]table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID()), table.AssertRefSnapshotID(name, existing)},
		[]table.Update{table.NewSetSnapshotRefUpdate(name, snap.SnapshotID, kind, keepForever, 0, 0)})
	return err
}

// DropRef drops a branch or tag. One that does not exist is an error
// unless ifExists.
func (m *Manager) DropRef(ctx context.Context, ident table.Identifier, name string, kind table.RefType, ifExists bool) error {
	if name == table.MainBranch {
		return fmt.Errorf("%w: main cannot be dropped", ErrInvalid)
	}
	tbl, err := m.cat.LoadTable(ctx, ident)
	if err != nil {
		return err
	}
	r, ok := findRef(tbl, name)
	if !ok || r.SnapshotRefType != kind {
		if ifExists {
			return nil
		}
		return fmt.Errorf("%w: %s has no %s %q", ErrNotFound, strings.Join(ident, "."), kind, name)
	}
	_, _, err = m.cat.CommitTable(ctx, ident,
		[]table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID()), table.AssertRefSnapshotID(name, &r.SnapshotID)},
		[]table.Update{table.NewRemoveSnapshotRefUpdate(name)})
	return err
}

// FastForward moves branch ahead to the head of the branch to, which must
// descend from it. Moving main changes what the table reads.
func (m *Manager) FastForward(ctx context.Context, ident table.Identifier, branch, to string) error {
	tbl, err := m.cat.LoadTable(ctx, ident)
	if err != nil {
		return err
	}
	name := strings.Join(ident, ".")
	target, ok := findRef(tbl, to)
	if !ok || target.SnapshotRefType != table.BranchRef {
		return fmt.Errorf("%w: %s has no branch %q", ErrNotFound, name, to)
	}
	from, ok := findRef(tbl, branch)
	if ok && from.SnapshotRefType != table.BranchRef {
		return fmt.Errorf("%w: %q is a tag of %s; only branches move", ErrInvalid, branch, name)
	}
	if !ok && branch != table.MainBranch {
		return fmt.Errorf("%w: %s has no branch %q", ErrNotFound, name, branch)
	}
	var current *int64
	if ok {
		if from.SnapshotID == target.SnapshotID {
			return nil
		}
		if !descends(tbl.Metadata(), target.SnapshotID, from.SnapshotID) {
			return fmt.Errorf("%w: %s has commits that %s does not; fast-forward only moves a branch ahead along its own history",
				ErrInvalid, branch, to)
		}
		current = &from.SnapshotID
	}
	upd := table.NewSetSnapshotRefUpdate(branch, target.SnapshotID, table.BranchRef, 0, 0, 0)
	if ok {
		upd = table.NewSetSnapshotRefUpdate(branch, target.SnapshotID, table.BranchRef,
			deref64(from.MaxRefAgeMs), deref64(from.MaxSnapshotAgeMs), derefInt(from.MinSnapshotsToKeep))
	}
	_, _, err = m.cat.CommitTable(ctx, ident,
		[]table.Requirement{table.AssertTableUUID(tbl.Metadata().TableUUID()), table.AssertRefSnapshotID(branch, current)},
		[]table.Update{upd})
	return err
}

// descends reports whether snapshot id is ancestor or a descendant of it.
func descends(meta table.Metadata, id, ancestor int64) bool {
	for seen := 0; seen <= len(meta.Snapshots()); seen++ {
		if id == ancestor {
			return true
		}
		s := meta.SnapshotByID(id)
		if s == nil || s.ParentSnapshotID == nil {
			return false
		}
		id = *s.ParentSnapshotID
	}
	return false
}

func findRef(tbl *table.Table, name string) (table.SnapshotRef, bool) {
	for n, r := range tbl.Metadata().Refs() {
		if n == name {
			return r, true
		}
	}
	return table.SnapshotRef{}, false
}

func deref64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
