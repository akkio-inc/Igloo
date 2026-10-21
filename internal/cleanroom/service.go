package cleanroom

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Who may do what:
//
//   - Admins create and drop rooms and add and remove collaborators, and
//     add datasets for partners, whose data comes in as external tables.
//   - A local collaborator's members (its user, or the holders of its
//     role) add, replace and remove its datasets, as admins may; they must
//     be able to read the source themselves.
//   - Members of any collaborator, and admins, add and remove templates.
//   - Only a collaborator's members approve on its behalf: admins cannot
//     approve for a collaborator they are not a member of, and only a
//     partner's own token approves for it.
//   - Members of a collaborator a template names in RUN BY run it, and
//     only they receive its result.
//   - Members of any collaborator, and admins, see the room, its templates
//     and its run history; a run's result only its requester's members
//     see.

// Engine is what runs need of the SQL server; *pgwire.Server implements
// it.
type Engine interface {
	// Execute runs plan as room's identity, which reads plan.Reads and
	// nothing else, and writes the result to result's table when result
	// is set.
	Execute(ctx context.Context, room string, plan *Plan, result *ResultTarget) (*Outcome, error)
	// DropResult drops a result table, if it exists.
	DropResult(ctx context.Context, schema, table string) error
	// Confined applies a change of confinements on this replica at once.
	Confined(ctx context.Context) error
}

// ResultTarget is the table a local collaborator's result goes to, which
// its user or role reads.
type ResultTarget struct {
	Schema, Table string
	// Grantee is the user or role, by GranteeType (auth.GranteeUser or
	// auth.GranteeRole).
	Grantee, GranteeType string
}

// Outcome is what a run returned.
type Outcome struct {
	Columns   []Column
	Rows      [][]*string
	Truncated bool
}

// Options configure clean rooms.
type Options struct {
	Store *Store
	// Policies keep the confinements, with the policies.
	Policies policy.Store
	// Parser parses templates.
	Parser *Parser
	// Databases are where datasets are in SQL.
	Databases Databases
	// Retention is how long runs are kept in the history.
	Retention time.Duration
	Log       *slog.Logger
}

// Service carries out clean room statements and runs.
type Service struct {
	store     *Store
	policies  policy.Store
	parser    *Parser
	dbs       Databases
	retention time.Duration
	log       *slog.Logger
	engine    atomic.Pointer[Engine]
}

// New sets up clean rooms. Runs wait for SetEngine.
func New(opts Options) (*Service, error) {
	if opts.Store == nil || opts.Policies == nil || opts.Parser == nil || opts.Databases.Catalog == "" || opts.Databases.Local == "" {
		return nil, ErrUnavailable
	}
	if opts.Log == nil {
		opts.Log = slog.New(slog.DiscardHandler)
	}
	return &Service{store: opts.Store, policies: opts.Policies, parser: opts.Parser, dbs: opts.Databases,
		retention: opts.Retention, log: opts.Log.With("component", "cleanroom")}, nil
}

// SetEngine gives the service the SQL server that runs templates.
func (s *Service) SetEngine(e Engine) { s.engine.Store(&e) }

func (s *Service) runner() Engine {
	if e := s.engine.Load(); e != nil {
		return *e
	}
	return nil
}

// Close releases the store.
func (s *Service) Close() { s.store.Close() }

// --- confinements --------------------------------------------------------------

// confinement keeps a table to c's members, and room's runs.
func confinement(room string, c Collaborator, schema, table string) policy.Confinement {
	e := policy.Confinement{Schema: schema, Table: table, Room: room}
	switch c.Kind {
	case KindUser:
		e.Users = []string{c.Principal}
	case KindRole:
		e.Roles = []string{c.Principal}
	}
	return e
}

// confine writes a room's confinements: every dataset's source, and the
// result table of every template for every local collaborator it names in
// RUN BY, from the moment the template exists, so a result is never
// readable by others, on any replica. extra are more, for a dataset about
// to be added.
func (s *Service) confine(ctx context.Context, room string, extra ...policy.Confinement) error {
	cols, err := s.store.Collaborators(ctx, room)
	if err != nil {
		return err
	}
	byName := map[string]Collaborator{}
	for _, c := range cols {
		byName[c.Name] = c
	}
	datasets, err := s.store.Datasets(ctx, room)
	if err != nil {
		return err
	}
	templates, err := s.store.Templates(ctx, room)
	if err != nil {
		return err
	}
	entries := extra
	for _, d := range datasets {
		entries = append(entries, confinement(room, byName[d.Collaborator], d.Schema, d.Table))
	}
	for _, t := range templates {
		for _, name := range t.RunBy {
			if c, ok := byName[name]; ok && c.Kind != KindPartner {
				entries = append(entries, confinement(room, c, ResultSchema, ResultTable(room, t.Name, c.Name)))
			}
		}
	}
	return s.setConfinements(ctx, room, entries)
}

func (s *Service) setConfinements(ctx context.Context, room string, entries []policy.Confinement) error {
	// Every replica writes every room's when it starts; most often they
	// are as they were.
	if snap, err := s.policies.Load(ctx); err == nil && sameConfinements(snap.Catalog.Confinements, room, entries) {
		return nil
	}
	if err := s.policies.Update(ctx, func(c *policy.Catalog) error { return c.SetConfinements(room, entries) }); err != nil {
		return fmt.Errorf("protect the datasets of clean room %s: %w", room, err)
	}
	if e := s.runner(); e != nil {
		return e.Confined(ctx)
	}
	return nil
}

// sameConfinements reports whether room's confinements in current are
// entries, in any order.
func sameConfinements(current []policy.Confinement, room string, entries []policy.Confinement) bool {
	key := func(c policy.Confinement) string {
		return strings.ToLower(c.Schema+"."+c.Table) + "\x00" + strings.Join(c.Users, ",") + "\x00" + strings.Join(c.Roles, ",")
	}
	var have, want []string
	for _, c := range current {
		if c.Room == room {
			have = append(have, key(c))
		}
	}
	for _, c := range entries {
		want = append(want, key(c))
	}
	slices.Sort(have)
	slices.Sort(want)
	return slices.Equal(slices.Compact(have), slices.Compact(want))
}

// Reconcile writes every room's confinements again, and drops those of
// rooms that no longer exist, in case a change stopped halfway.
func (s *Service) Reconcile(ctx context.Context) error {
	rooms, err := s.store.Rooms(ctx)
	if err != nil {
		return err
	}
	snap, err := s.policies.Load(ctx)
	if err != nil {
		return err
	}
	var stale []string
	for _, c := range snap.Catalog.Confinements {
		if !slices.ContainsFunc(rooms, func(r Room) bool { return r.Name == c.Room }) && !slices.Contains(stale, c.Room) {
			stale = append(stale, c.Room)
		}
	}
	var errs []error
	for _, room := range stale {
		errs = append(errs, s.setConfinements(ctx, room, nil))
	}
	for _, r := range rooms {
		errs = append(errs, s.confine(ctx, r.Name))
	}
	return errors.Join(errs...)
}

// --- membership ------------------------------------------------------------------

// room loads a room the actor may see, and its collaborators. Rooms the
// actor is not in look like rooms that do not exist.
func (s *Service) room(ctx context.Context, a Actor, name string) (Room, []Collaborator, error) {
	r, err := s.store.Room(ctx, name)
	if err != nil {
		return r, nil, err
	}
	cols, err := s.store.Collaborators(ctx, name)
	if err != nil {
		return r, nil, err
	}
	if !a.Admin && !slices.ContainsFunc(cols, a.member) {
		return r, nil, ErrNotFound
	}
	return r, cols, nil
}

// acting returns the collaborator a acts for: as, or the only one a is a
// member of.
func acting(a Actor, cols []Collaborator, as string) (Collaborator, error) {
	var mine []Collaborator
	for _, c := range cols {
		if a.member(c) {
			mine = append(mine, c)
		}
	}
	if as != "" {
		i := slices.IndexFunc(mine, func(c Collaborator) bool { return c.Name == as })
		if i < 0 {
			return Collaborator{}, denied("%s does not act for collaborator %s", a.Name(), as)
		}
		return mine[i], nil
	}
	switch len(mine) {
	case 0:
		return Collaborator{}, denied("%s is not a member of any collaborator of this room", a.Name())
	case 1:
		return mine[0], nil
	}
	names := make([]string, len(mine))
	for i, c := range mine {
		names[i] = c.Name
	}
	return Collaborator{}, denied("%s acts for several collaborators (%s); say which with AS collaborator", a.Name(), strings.Join(names, ", "))
}

func find(cols []Collaborator, name string) (Collaborator, bool) {
	i := slices.IndexFunc(cols, func(c Collaborator) bool { return c.Name == name })
	if i < 0 {
		return Collaborator{}, false
	}
	return cols[i], true
}

// --- rooms and collaborators ---------------------------------------------------------

// CreateRoom creates an empty room.
func (s *Service) CreateRoom(ctx context.Context, a Actor, name, comment string) error {
	if !a.Admin {
		return denied("only admins create clean rooms")
	}
	if err := ValidName("clean room", name); err != nil {
		return err
	}
	return s.store.CreateRoom(ctx, Room{Name: name, Comment: comment, CreatedBy: a.Name()})
}

// DropRoom drops a room, its result tables and its confinements.
func (s *Service) DropRoom(ctx context.Context, a Actor, name string) error {
	if !a.Admin {
		return denied("only admins drop clean rooms")
	}
	cols, err := s.store.Collaborators(ctx, name)
	if err != nil {
		return err
	}
	templates, err := s.store.Templates(ctx, name)
	if err != nil {
		return err
	}
	if err := s.store.DropRoom(ctx, name); err != nil {
		return err
	}
	var errs []error
	for _, t := range templates {
		errs = append(errs, s.dropResults(ctx, name, t.Name, cols))
	}
	return errors.Join(append(errs, s.setConfinements(ctx, name, nil))...)
}

// dropResults drops a template's result tables of the local collaborators.
func (s *Service) dropResults(ctx context.Context, room, template string, cols []Collaborator) error {
	e := s.runner()
	if e == nil {
		return nil
	}
	var errs []error
	for _, c := range cols {
		if c.Kind != KindPartner {
			errs = append(errs, e.DropResult(ctx, ResultSchema, ResultTable(room, template, c.Name)))
		}
	}
	return errors.Join(errs...)
}

// RoomInfo is a room as SHOW CLEAN ROOMS lists it.
type RoomInfo struct {
	Room
	Collaborators []string
	// Yours are the collaborators the actor acts for.
	Yours []string
}

// Rooms lists the rooms the actor is in, or every room for admins.
func (s *Service) Rooms(ctx context.Context, a Actor) ([]RoomInfo, error) {
	rooms, err := s.store.Rooms(ctx)
	if err != nil {
		return nil, err
	}
	cols, err := s.store.Collaborators(ctx, "")
	if err != nil {
		return nil, err
	}
	var out []RoomInfo
	for _, r := range rooms {
		info := RoomInfo{Room: r}
		for _, c := range cols {
			if c.Room != r.Name {
				continue
			}
			info.Collaborators = append(info.Collaborators, c.Name)
			if a.member(c) {
				info.Yours = append(info.Yours, c.Name)
			}
		}
		if a.Admin || len(info.Yours) > 0 {
			out = append(out, info)
		}
	}
	return out, nil
}

// AddCollaborator adds a party to a room. The caller has checked that
// its principal exists.
func (s *Service) AddCollaborator(ctx context.Context, a Actor, c Collaborator) error {
	if !a.Admin {
		return denied("only admins add collaborators")
	}
	if err := ValidName("collaborator", c.Name); err != nil {
		return err
	}
	switch {
	case c.Kind != KindUser && c.Kind != KindRole && c.Kind != KindPartner:
		return invalid("a collaborator is a USER, a ROLE or a RECIPIENT")
	case c.Kind != KindPartner && c.Provider != "":
		return invalid("only partners (RECIPIENT) have a provider")
	}
	c.AddedBy = a.Name()
	return s.store.AddCollaborator(ctx, c)
}

// RemoveCollaborator removes a party that contributes no datasets and is
// named by no template, with its approvals and result tables.
func (s *Service) RemoveCollaborator(ctx context.Context, a Actor, room, name string) error {
	if !a.Admin {
		return denied("only admins remove collaborators")
	}
	cols, err := s.store.Collaborators(ctx, room)
	if err != nil {
		return err
	}
	c, ok := find(cols, name)
	if !ok {
		return ErrNotFound
	}
	datasets, err := s.store.Datasets(ctx, room)
	if err != nil {
		return err
	}
	if i := slices.IndexFunc(datasets, func(d *Dataset) bool { return d.Collaborator == name }); i >= 0 {
		return fmt.Errorf("collaborator %s %w: it contributes dataset %s; remove its datasets first", name, ErrInUse, datasets[i].Name)
	}
	templates, err := s.store.Templates(ctx, room)
	if err != nil {
		return err
	}
	if i := slices.IndexFunc(templates, func(t Template) bool { return slices.Contains(t.RunBy, name) }); i >= 0 {
		return fmt.Errorf("collaborator %s %w: template %s may be run by it; remove or replace the template first", name, ErrInUse, templates[i].Name)
	}
	if err := s.store.RemoveCollaborator(ctx, room, name); err != nil {
		return err
	}
	var errs []error
	for _, t := range templates {
		errs = append(errs, s.dropResults(ctx, room, t.Name, []Collaborator{c}))
	}
	return errors.Join(append(errs, s.confine(ctx, room))...)
}

// --- datasets --------------------------------------------------------------------------

// Source is what a dataset reads, as the SQL server found it for the user
// adding it.
type Source struct {
	Schema, Table string
	Kind          string
	Columns       []string
	// Provider is an external table's Delta Sharing provider.
	Provider string
	// Identity is the source's identity (see Dataset.Identity).
	Identity string
}

// AddDataset adds a dataset of a collaborator, or with replace changes
// one. The caller has resolved the source for the actor, who could read
// it with no masking or row access policies applying.
func (s *Service) AddDataset(ctx context.Context, a Actor, d Dataset, src Source, replace bool) error {
	if err := ValidName("dataset", d.Name); err != nil {
		return err
	}
	_, cols, err := s.room(ctx, a, d.Room)
	if err != nil {
		return err
	}
	c, ok := find(cols, d.Collaborator)
	if !ok {
		return fmt.Errorf("collaborator %s %w", d.Collaborator, ErrNotFound)
	}
	switch {
	case c.Kind == KindPartner && !a.Admin:
		return denied("only admins add a partner's datasets, which are external tables of its provider")
	case c.Kind == KindPartner && (src.Kind != SourceExternal || src.Provider != c.Provider || c.Provider == ""):
		return invalid("a partner's datasets are external tables of its Delta Sharing provider (%s); %s is not one", orNone(c.Provider), src.Schema+"."+src.Table)
	case c.Kind != KindPartner && !a.Admin && !a.member(c):
		return denied("only members of collaborator %s, or admins, add its datasets", c.Name)
	}
	if err := d.Policy.Check(src.Columns); err != nil {
		return err
	}
	if src.Kind != SourceTable && src.Kind != SourceExternal {
		return invalid("%s is not a table; contribute a table of what the room may read (CREATE TABLE ... AS)", src.Schema+"."+src.Table)
	}
	// A table some room keeps to a party may join others only for the
	// same party: anything else would let a new party's users read it.
	entry := confinement(d.Room, c, src.Schema, src.Table)
	snap, err := s.policies.Load(ctx)
	if err != nil {
		return err
	}
	if rules := snap.Rules(src.Schema, src.Table); rules != nil {
		for _, other := range rules.Confined {
			if other.Room == d.Room {
				continue
			}
			if !slices.Equal(other.Users, entry.Users) || !slices.Equal(other.Roles, entry.Roles) {
				return denied("%s is in clean room %s for another party; a table is in rooms for one party only", src.Schema+"."+src.Table, other.Room)
			}
		}
		if rules.Policies() {
			return invalid("%s has masking or row access policies; contribute a table of what the room may read instead", src.Schema+"."+src.Table)
		}
	}
	d.Schema, d.Table, d.Kind, d.Identity, d.AddedBy = src.Schema, src.Table, src.Kind, src.Identity, a.Name()
	// Confine first: the dataset is in no room until it is stored, and
	// kept to its party from then on.
	if err := s.confine(ctx, d.Room, entry); err != nil {
		return err
	}
	err = s.store.PutDataset(ctx, d, replace)
	if cerr := s.confine(ctx, d.Room); cerr != nil {
		s.log.Warn("could not update a room's confinements", "room", d.Room, "err", cerr)
	}
	return err
}

func orNone(s string) string {
	if s == "" {
		return "none: add the collaborator with PROVIDER"
	}
	return s
}

// RemoveDataset removes a dataset; its source is readable as before.
func (s *Service) RemoveDataset(ctx context.Context, a Actor, room, name string) error {
	_, cols, err := s.room(ctx, a, room)
	if err != nil {
		return err
	}
	datasets, err := s.store.Datasets(ctx, room)
	if err != nil {
		return err
	}
	i := slices.IndexFunc(datasets, func(d *Dataset) bool { return d.Name == name })
	if i < 0 {
		return ErrNotFound
	}
	if c, _ := find(cols, datasets[i].Collaborator); !a.Admin && !a.member(c) {
		return denied("only members of collaborator %s, or admins, remove its datasets", c.Name)
	}
	if err := s.store.RemoveDataset(ctx, room, name); err != nil {
		return err
	}
	return s.confine(ctx, room)
}

// --- templates ----------------------------------------------------------------

// AddTemplate checks a template against the room's datasets and stores it
// as version 1, or with replace as its next version, which needs every
// approval again. It returns the version.
func (s *Service) AddTemplate(ctx context.Context, a Actor, t Template, replace bool) (int, error) {
	if err := ValidName("template", t.Name); err != nil {
		return 0, err
	}
	_, cols, err := s.room(ctx, a, t.Room)
	if err != nil {
		return 0, err
	}
	if !a.Admin && !slices.ContainsFunc(cols, a.member) {
		return 0, denied("only the room's collaborators, and admins, add templates")
	}
	if len(t.RunBy) == 0 {
		return 0, invalid("say who may run the template, and receive its results: RUN BY (collaborator, ...)")
	}
	for i, name := range t.RunBy {
		if _, ok := find(cols, name); !ok {
			return 0, fmt.Errorf("collaborator %s %w", name, ErrNotFound)
		}
		if slices.Contains(t.RunBy[:i], name) {
			return 0, invalid("RUN BY names %s twice", name)
		}
	}
	datasets, err := s.store.Datasets(ctx, t.Room)
	if err != nil {
		return 0, err
	}
	if _, err := s.parser.Check(ctx, t, datasets); err != nil {
		return 0, err
	}
	t.CreatedBy = a.Name()
	version, err := s.store.PutTemplate(ctx, t, replace)
	if err != nil {
		return 0, err
	}
	return version, s.confine(ctx, t.Room)
}

// AddBuiltin adds a template made from a built-in.
func (s *Service) AddBuiltin(ctx context.Context, a Actor, t Template, builtin string, args map[string]string, replace bool) (int, error) {
	sql, params, describe, err := Builtin(builtin, args)
	if err != nil {
		return 0, err
	}
	t.SQL, t.Parameters, t.Builtin = sql, params, describe
	return s.AddTemplate(ctx, a, t, replace)
}

// RemoveTemplate removes a template, with its approvals and result tables.
func (s *Service) RemoveTemplate(ctx context.Context, a Actor, room, name string) error {
	_, cols, err := s.room(ctx, a, room)
	if err != nil {
		return err
	}
	if !a.Admin && !slices.ContainsFunc(cols, a.member) {
		return denied("only the room's collaborators, and admins, remove templates")
	}
	if err := s.store.RemoveTemplate(ctx, room, name); err != nil {
		return err
	}
	return errors.Join(s.dropResults(ctx, room, name, cols), s.confine(ctx, room))
}

// TemplateStatus is a template's current version, and whether it can run.
type TemplateStatus struct {
	Template
	// Needs are the collaborators that must approve the version: those
	// whose datasets it reads. Approved are those that did, of it as it
	// is, reading the datasets as they are.
	Needs, Approved []string
	// Approves is what an approval approves: the digest of the version and
	// of the datasets it reads, their sources and policies.
	Approves string
	// Problem says why the template cannot run now, or is empty.
	Problem string
}

// Runnable reports whether every approval it needs is there.
func (t TemplateStatus) Runnable() bool {
	return t.Problem == "" && len(t.Approved) == len(t.Needs)
}

// Templates lists a room's templates and their approvals.
func (s *Service) Templates(ctx context.Context, a Actor, room string) ([]TemplateStatus, error) {
	if _, _, err := s.room(ctx, a, room); err != nil {
		return nil, err
	}
	return s.templates(ctx, room)
}

func (s *Service) templates(ctx context.Context, room string) ([]TemplateStatus, error) {
	templates, err := s.store.Templates(ctx, room)
	if err != nil {
		return nil, err
	}
	datasets, err := s.store.Datasets(ctx, room)
	if err != nil {
		return nil, err
	}
	approvals, err := s.store.Approvals(ctx, room)
	if err != nil {
		return nil, err
	}
	out := make([]TemplateStatus, len(templates))
	for i, t := range templates {
		st := TemplateStatus{Template: t}
		read, err := s.parser.Check(ctx, t, datasets)
		if err != nil {
			st.Problem = err.Error()
		}
		for _, d := range read {
			if !slices.Contains(st.Needs, d.Collaborator) {
				st.Needs = append(st.Needs, d.Collaborator)
			}
		}
		st.Approves = approves(t, read)
		for _, ap := range approvals {
			if ap.Of(t, read) && slices.Contains(st.Needs, ap.Collaborator) {
				st.Approved = append(st.Approved, ap.Collaborator)
			}
		}
		out[i] = st
	}
	return out, nil
}

// Approve approves a template's version for the collaborator the actor
// acts for: as, or the only one.
func (s *Service) Approve(ctx context.Context, a Actor, room, template string, version int, as string) (string, error) {
	_, cols, err := s.room(ctx, a, room)
	if err != nil {
		return "", err
	}
	c, err := acting(a, cols, as)
	if err != nil {
		return "", err
	}
	statuses, err := s.templates(ctx, room)
	if err != nil {
		return "", err
	}
	i := slices.IndexFunc(statuses, func(t TemplateStatus) bool { return t.Name == template })
	if i < 0 {
		return "", ErrNotFound
	}
	st := statuses[i]
	if st.Problem != "" {
		return "", invalid("template %s cannot run as it is: %s", template, st.Problem)
	}
	if !slices.Contains(st.Needs, c.Name) {
		return "", invalid("template %s reads no dataset of collaborator %s, so it needs no approval of its; it needs %s",
			template, c.Name, strings.Join(st.Needs, ", "))
	}
	return c.Name, s.store.Approve(ctx, Approval{Room: room, Template: template, Version: version, Digest: st.Approves,
		Collaborator: c.Name, ApprovedBy: a.Name()})
}

// Unapprove withdraws the approvals of the collaborator the actor acts for.
func (s *Service) Unapprove(ctx context.Context, a Actor, room, template, as string) (string, error) {
	_, cols, err := s.room(ctx, a, room)
	if err != nil {
		return "", err
	}
	c, err := acting(a, cols, as)
	if err != nil {
		return "", err
	}
	return c.Name, s.store.Unapprove(ctx, room, template, c.Name)
}

// RoomView is everything about a room that its collaborators see.
type RoomView struct {
	Room          Room
	Collaborators []Collaborator
	// Yours are the collaborators the actor acts for.
	Yours     []string
	Datasets  []*Dataset
	Templates []TemplateStatus
}

// Describe returns a room as the actor sees it.
func (s *Service) Describe(ctx context.Context, a Actor, room string) (*RoomView, error) {
	r, cols, err := s.room(ctx, a, room)
	if err != nil {
		return nil, err
	}
	v := &RoomView{Room: r, Collaborators: cols}
	for _, c := range cols {
		if a.member(c) {
			v.Yours = append(v.Yours, c.Name)
		}
	}
	if v.Datasets, err = s.store.Datasets(ctx, room); err != nil {
		return nil, err
	}
	if v.Templates, err = s.templates(ctx, room); err != nil {
		return nil, err
	}
	return v, nil
}

// --- runs ------------------------------------------------------------------------

// Run runs a template for the collaborator the actor acts for, with values
// for its parameters, and returns the run with its result.
func (s *Service) Run(ctx context.Context, a Actor, room, template string, values map[string]any, as string) (*Run, error) {
	e := s.runner()
	if e == nil {
		return nil, ErrUnavailable
	}
	_, cols, err := s.room(ctx, a, room)
	if err != nil {
		return nil, err
	}
	requester, err := acting(a, cols, as)
	if err != nil {
		return nil, err
	}
	templates, err := s.store.Templates(ctx, room)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(templates, func(t Template) bool { return t.Name == template })
	if i < 0 {
		return nil, fmt.Errorf("template %s %w", template, ErrNotFound)
	}
	t := templates[i]
	if !slices.Contains(t.RunBy, requester.Name) {
		return nil, denied("template %s may be run by %s, not by %s", t.Name, strings.Join(t.RunBy, ", "), requester.Name)
	}
	datasets, err := s.store.Datasets(ctx, room)
	if err != nil {
		return nil, err
	}
	// Checked again against the policies as they are now.
	plan, err := s.parser.Plan(ctx, t, datasets, s.dbs, values)
	if err != nil {
		return nil, err
	}
	approvals, err := s.store.Approvals(ctx, room)
	if err != nil {
		return nil, err
	}
	run := &Run{Room: room, Template: t.Name, Version: t.Version, Requester: requester.Name, RequestedBy: a.Name(),
		Parameters: values, Noise: plan.Noise, Cost: plan.Cost}
	budgets := map[string]int{}
	var missing []string
	for _, d := range plan.Datasets {
		run.Datasets = append(run.Datasets, d.Name)
		budgets[d.Name] = d.Policy.RunsPerDay
		approved := slices.ContainsFunc(approvals, func(ap Approval) bool {
			return ap.Of(t, plan.Datasets) && ap.Collaborator == d.Collaborator
		})
		switch {
		case !approved && !slices.Contains(missing, d.Collaborator):
			missing = append(missing, d.Collaborator)
		case approved && !slices.Contains(run.Approvals, d.Collaborator):
			run.Approvals = append(run.Approvals, d.Collaborator)
		}
	}
	if len(missing) > 0 {
		return nil, denied("template %s version %d runs only once %s approve it, as it reads the datasets now", t.Name, t.Version, strings.Join(missing, " and "))
	}
	slices.Sort(run.Approvals)
	// Every dataset is kept to its party now, and carries no policies a
	// run would read past.
	snap, err := s.policies.Load(ctx)
	if err != nil {
		return nil, err
	}
	for _, d := range plan.Datasets {
		rules := snap.Rules(d.Schema, d.Table)
		if !slices.Contains(rules.Rooms(), room) {
			return nil, fmt.Errorf("dataset %s is not protected yet; try again in a moment", d.Name)
		}
		if rules.Policies() {
			return nil, invalid("the source of dataset %s, %s, has masking or row access policies now; contribute a table of what the room may read instead",
				d.Name, d.Source())
		}
	}
	var target *ResultTarget
	if requester.Kind != KindPartner {
		target = &ResultTarget{Schema: ResultSchema, Table: ResultTable(room, t.Name, requester.Name), Grantee: requester.Principal,
			GranteeType: auth.GranteeRole}
		if requester.Kind == KindUser {
			target.GranteeType = auth.GranteeUser
		}
		if rules := snap.Rules(target.Schema, target.Table); !slices.Contains(rules.Rooms(), room) {
			return nil, fmt.Errorf("the result table %s.%s is not protected yet; try again in a moment", target.Schema, target.Table)
		}
		run.ResultTable = target.Schema + "." + target.Table
	}
	if err := s.store.BeginRun(ctx, run, budgets, s.retention); err != nil {
		return nil, err
	}
	out, err := e.Execute(ctx, room, plan, target)
	var refused *RefusedError
	switch {
	case err == nil:
		run.Status, run.Columns, run.Result = RunSucceeded, out.Columns, out.Rows
		run.Rows, run.Truncated = int64(len(out.Rows)), out.Truncated
	case errors.As(err, &refused):
		run.Status, run.Error = RunRefused, err.Error()
	default:
		run.Status, run.Error = RunFailed, err.Error()
	}
	if ferr := s.store.FinishRun(context.WithoutCancel(ctx), run); ferr != nil {
		s.log.Warn("could not record the end of a run", "room", room, "run", run.ID, "err", ferr)
	}
	s.log.Info("clean room run", "room", room, "template", t.Name, "version", t.Version, "requester", requester.Name,
		"by", a.Name(), "status", run.Status, "rows", run.Rows)
	return run, err
}

// Runs lists a room's runs, newest first, without results.
func (s *Service) Runs(ctx context.Context, a Actor, room string, limit int) ([]Run, error) {
	if _, _, err := s.room(ctx, a, room); err != nil {
		return nil, err
	}
	return s.store.Runs(ctx, room, limit)
}

// RunResult returns a run, with its result when the actor acts for its
// requester.
func (s *Service) RunResult(ctx context.Context, a Actor, room string, id int64) (*Run, error) {
	_, cols, err := s.room(ctx, a, room)
	if err != nil {
		return nil, err
	}
	r, err := s.store.Run(ctx, room, id)
	if err != nil {
		return nil, err
	}
	if c, ok := find(cols, r.Requester); !ok || !a.member(c) {
		r.Columns, r.Result = nil, nil
	}
	return &r, nil
}
