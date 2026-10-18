package icebergrest

import (
	"context"
	"net/http"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/clone"
)

// Zero-copy clones (package clone) are tables whose metadata names files
// under their sources' locations. Three things follow for other engines:
//
//   - Dropping a table with purgeRequested makes Lakekeeper delete every
//     file under its location, so whether a dropped table is purged is
//     decided as for every way Igloo deletes a table's files
//     (clone.Guard.MayPurge): a table in the warehouse whose files no
//     clone reads is purged whatever the client asks, as in SQL, so that
//     its files do not linger; otherwise its files stay, and a drop that
//     asks for them to go is refused, naming the clones if that is why,
//     so that the client may drop it without purging or drop the clones
//     first. Erasure requests delete whatever a drop leaves (package
//     erasure).
//   - Credentials reach one table's location (see Vendor), so they would
//     not reach a clone's files under its source's. Igloo hands out none
//     for clones, rather than credentials that cover the source too, which
//     would read the source's newer rows and older snapshots, beyond what
//     the clone holds.
//   - Write credentials for a table whose files clones read delete
//     nothing, so an engine's own snapshot expiry or orphan cleanup cannot
//     delete files the clones need; Igloo's maintenance deletes them once
//     no clone does.
//   - The table properties that say which locations a clone reads
//     (igloo.clone.*) are Igloo's: commits may not set or remove them, as
//     maintenance rebuilds its record of shared files from them.
//
// Lakekeeper itself deletes no data or manifest files when it commits:
// only old metadata files of the table's own, inside its location, which no
// clone reads (a clone has its own). Removing snapshots through a commit
// changes metadata only.

// Clones is what the endpoint needs to know of clones; *clone.Guard
// implements it.
type Clones interface {
	// Sources returns the locations of other tables whose files a table
	// with these properties reads: none for a table that is no clone.
	Sources(properties map[string]string) []string
	// Shared reports whether clones may read files under location.
	Shared(ctx context.Context, location string) (bool, error)
	// MayPurge decides whether the files of the table called name, at
	// location, may go with it; inCatalog says the catalog has it still.
	// The verdict's Release, when set, must be called.
	MayPurge(ctx context.Context, what string, name []string, location string, inCatalog bool) clone.PurgeVerdict
}

// errConflict is the error type of a drop refused because clones read the
// table's files.
const errConflict = "ConflictException"

// cloneProperties is the prefix of the table properties Igloo sets on
// clones (clone.SharesProperty and the others).
const cloneProperties = "igloo.clone."

// checkCloneProperties refuses setting or removing the properties keys
// name that Igloo keeps on clones.
func checkCloneProperties(keys []string) error {
	for _, k := range keys {
		if strings.HasPrefix(strings.ToLower(k), cloneProperties) {
			return errorf(http.StatusBadRequest, errBadRequest,
				"table property %s is Igloo's: it records which tables' files a clone reads", k)
		}
	}
	return nil
}

// purgeRequested reports whether a drop asks Lakekeeper to delete the
// table's files. Lakekeeper reads the parameter as a boolean; any value
// but false counts.
func purgeRequested(c *call) bool {
	for _, v := range c.r.URL.Query()["purgeRequested"] {
		if !strings.EqualFold(strings.TrimSpace(v), "false") {
			return true
		}
	}
	return false
}

// purgeVerdict decides whether the table at path, called name, which is
// about to be dropped, has its files purged with it (Clones.MayPurge).
func (s *Server) purgeVerdict(c *call, path string, name []string) (clone.PurgeVerdict, error) {
	r, err := s.cat.call(c.ctx(), http.MethodGet, path, nil, nil)
	if err != nil {
		return clone.PurgeVerdict{}, err
	}
	t, err := parseLoadResult(r.body)
	if err != nil {
		return clone.PurgeVerdict{}, err
	}
	return s.opts.Clones.MayPurge(c.ctx(), "drop", name, t.location, true), nil
}

// purgeRefusal refuses a drop that asks for the table's files to go when
// v keeps them.
func purgeRefusal(v clone.PurgeVerdict) error {
	switch {
	case v.Unsure:
		return errorf(http.StatusServiceUnavailable, errUnavailable,
			"could not tell whether the table's files may go, so it is not purged; try again")
	case len(v.Clones) > 0:
		return errorf(http.StatusConflict, errConflict,
			"the table is not purged: clones read its files (%s); drop them first, or drop the table without purgeRequested, which keeps its files",
			strings.Join(v.Clones, ", "))
	}
	return errorf(http.StatusConflict, errConflict,
		"the table is not purged: %s; drop it without purgeRequested, which keeps its files", v.Why)
}

// cloneCredentials refuses credentials for table o, loaded as t, if it is a
// clone, whose files lie under other tables' locations.
func (s *Server) cloneCredentials(o auth.Object, t *loadResult) error {
	if s.opts.Clones == nil || len(s.opts.Clones.Sources(t.properties)) == 0 {
		return nil
	}
	return errorf(http.StatusForbidden, errForbidden,
		"table %s is a clone, which reads files of the table it was cloned from, and Igloo hands out no storage credentials for clones; "+
			"read it through Igloo's SQL or Flight SQL, or load it without vended credentials if the engine has access of its own", o)
}

// writeAccess is what write credentials for the table at location may do:
// everything, unless clones read its files or that cannot be told.
func (s *Server) writeAccess(c *call, location string) Access {
	if s.opts.Clones == nil {
		return WriteFiles
	}
	shared, err := s.opts.Clones.Shared(c.ctx(), location)
	if err != nil {
		s.log.Warn("could not tell whether clones read a table's files; its write credentials delete nothing", "location", location, "err", err)
		return AddFiles
	}
	if shared {
		return AddFiles
	}
	return WriteFiles
}
