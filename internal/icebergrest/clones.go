package icebergrest

import (
	"context"
	"net/http"
	"strings"

	"github.com/akkio-inc/igloo/internal/auth"
)

// Zero-copy clones (package clone) are tables whose metadata names files
// under their sources' locations. Three things follow for other engines:
//
//   - Dropping a table with purgeRequested makes Lakekeeper delete every
//     file under its location, so a table whose files clones read is not
//     purged: the drop is refused, naming the clones, and the client may
//     drop it without purging (its files stay, as SQL's DROP TABLE leaves
//     them then) or drop the clones first. A table in the warehouse whose
//     files no clone reads is purged when it is dropped, whatever the
//     client asks, as in SQL, so that its files do not linger; erasure
//     requests delete whatever a drop leaves (package erasure).
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
	// Readers takes the lock of location, which making a clone of the
	// table there takes too, and names the clones that read files under
	// it, or may. release must be called.
	Readers(ctx context.Context, location string) (names []string, release func(), err error)
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

// holdForPurge refuses to purge the table at path if clones read its files.
// Otherwise it holds them until release is called, so no clone of the table
// is made before the drop is through.
func (s *Server) holdForPurge(c *call, path string) (release func(), err error) {
	r, err := s.cat.call(c.ctx(), http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	t, err := parseLoadResult(r.body)
	if err != nil {
		return nil, err
	}
	names, release, err := s.opts.Clones.Readers(c.ctx(), t.location)
	if err != nil {
		s.log.Warn("could not tell whether clones read a table's files", "location", t.location, "err", err)
		return nil, errorf(http.StatusServiceUnavailable, errUnavailable,
			"could not tell whether clones read the table's files, so it is not purged; try again")
	}
	if len(names) > 0 {
		release()
		return nil, errorf(http.StatusConflict, errConflict,
			"the table is not purged: clones read its files (%s); drop them first, or drop the table without purgeRequested, which keeps its files",
			strings.Join(names, ", "))
	}
	return release, nil
}

// holdUnread reports whether the table at path, which is dropped without
// purging, may be purged instead: it is in the warehouse (StorageRoot),
// and no clone reads its files, which none will until release is called.
// When that cannot be told, it is not.
func (s *Server) holdUnread(c *call, path string) (release func(), ok bool) {
	if s.opts.Clones == nil || s.opts.StorageRoot == "" {
		return nil, false
	}
	r, err := s.cat.call(c.ctx(), http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, false
	}
	t, err := parseLoadResult(r.body)
	if err != nil || !strings.HasPrefix(strings.TrimSuffix(t.location, "/")+"/", strings.TrimSuffix(s.opts.StorageRoot, "/")+"/") {
		return nil, false
	}
	names, release, err := s.opts.Clones.Readers(c.ctx(), t.location)
	switch {
	case err != nil:
		s.log.Warn("could not tell whether clones read a dropped table's files; it is dropped without them", "location", t.location, "err", err)
		return nil, false
	case len(names) > 0:
		release()
		return nil, false
	}
	return release, true
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
