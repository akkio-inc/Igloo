package catalogproxy

import (
	"net/http"
	"strings"
	"sync/atomic"
)

// Shared holds. The engine's catalog lock (engine.Engine.CatalogLock) keeps
// DuckDB-Iceberg's in-memory set of the catalog's tables from being walked,
// or having entries erased, while statements are bound against it: either
// frees memory a bind or a walk is still reading. Binds, and the prepares
// of queries (see pgwire's sharedprepare.go), hold the lock shared; walks
// (listings, and DuckDB's suggestions of similar names for a missing one)
// and erases (a drop or a rename committing) are to hold it exclusively.
// DuckDB-Iceberg walks a schema's tables only after listing them in the
// same transaction, and erases a table's or a schema's entry only after the
// catalog accepted the drop or rename; those requests all come through the
// proxy. So while the lock is held shared, and not exclusively, the proxy
// refuses them (see SetSharedHold): DuckDB fails the statement instead of
// walking or erasing, and Igloo can run it again holding the lock
// exclusively.
//
// The refusal is 423 Locked, which DuckDB reports as an HTTP error that
// names the status (its reason for a failed listing is the status line's),
// and Refusals counts them.

// heldShared is the message of the proxy's refusal of a listing or a drop
// while the catalog lock is held shared.
const heldShared = "igloo: the catalog's tables are not listed, dropped or renamed while statements are bound alongside"

// RefusedShared reports whether msg, the message of an error DuckDB
// returned, may be its report of the proxy's refusal while the catalog lock
// was held shared: the message, or the status, which is all DuckDB names of
// some failed requests.
func RefusedShared(msg string) bool {
	return strings.Contains(msg, heldShared) || strings.Contains(msg, "423") || strings.Contains(msg, "Locked")
}

// SetSharedHold sets held, which reports whether the engine's catalog lock
// is held shared now, and not exclusively; until it is set, nothing is
// refused.
func (p *Proxy) SetSharedHold(held func() bool) { p.shared.Store(&held) }

// Refusals is how many requests the proxy refused because the catalog lock
// was held shared.
func (p *Proxy) Refusals() uint64 {
	if p == nil {
		return 0
	}
	return p.refusals.Load()
}

// sharedHold is the proxy's record of the engine's shared holds.
type sharedHold struct {
	shared   atomic.Pointer[func() bool]
	refusals atomic.Uint64
}

// refuseShared answers r, whose path under the proxy is rest, with a
// refusal if it lists a namespace's tables or views, or drops or renames a
// table or drops a namespace, while the catalog lock is held shared.
func (p *Proxy) refuseShared(w http.ResponseWriter, r *http.Request, rest string) bool {
	held := p.shared.Load()
	if held == nil || !walksOrErases(r.Method, rest) || !(*held)() {
		return false
	}
	p.refusals.Add(1)
	// Igloo prepares or binds the statement again under the exclusive lock
	// (pgwire's prepareChecked and runBound): nothing for an operator to do.
	p.log.Debug("refused a catalog request while the catalog lock is held shared", "method", r.Method, "path", rest)
	writeError(w, http.StatusLocked, "CommitFailedException", heldShared)
	return true
}

// walksOrErases reports whether a request lists a namespace's tables or
// views, which DuckDB-Iceberg walks once they are listed, or drops or
// renames a table or drops a namespace, whose entry it then erases.
func walksOrErases(method, rest string) bool {
	switch method {
	case http.MethodGet:
		m := listPath.FindStringSubmatch(rest)
		return m != nil && m[2] != ""
	case http.MethodDelete:
		return loadTablePath.MatchString(rest) || namespaceCheckPath.MatchString(rest)
	case http.MethodPost:
		return renameTablePath.MatchString(rest)
	}
	return false
}
