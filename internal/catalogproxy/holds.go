package catalogproxy

import (
	"net/http"
	"sync/atomic"
)

// Shared prepares. The engine's catalog lock (engine.Engine.CatalogLock)
// keeps DuckDB-Iceberg's in-memory set of the catalog's tables from being
// walked, or having entries erased, while statements are bound against it:
// either frees memory a bind or a walk is still reading. Binds hold the lock
// shared; walks (listings, and DuckDB's suggestions of similar names for a
// missing one) and erases (a drop or a rename committing) are to hold it
// exclusively. Igloo prepares queries under the shared side, alongside other
// statements' prepares and binds (see pgwire's sharedprepare.go), and a
// prepare can walk: DuckDB suggests names for a table that does not exist,
// say one another engine dropped a moment before. DuckDB-Iceberg walks a
// schema's tables only after listing them in the same transaction, and
// erases a table's or a schema's entry only after the catalog accepted the
// drop or rename; those requests all come through the proxy. So while any
// statement is prepared that way, the proxy refuses them (see
// SetSharedPrepares): DuckDB fails the statement instead of walking or
// erasing, and Igloo prepares it again under the exclusive lock. (A request
// does not say which statement made it, so another's listing meanwhile is
// refused too.)
//
// The refusal is 423 Locked, which DuckDB reports as an HTTP error that
// names the status (its reason for a failed listing is the status line's),
// and Refusals counts them.

// heldShared is the message of the proxy's refusal of a listing or a drop
// while statements are prepared under the shared catalog lock.
const heldShared = "igloo: the catalog's tables are not listed, dropped or renamed while statements are prepared alongside others"

// SetSharedPrepares sets preparing, which reports whether statements are
// being prepared under the shared catalog lock now, and nothing holds it
// exclusively; until it is set, nothing is refused.
func (p *Proxy) SetSharedPrepares(preparing func() bool) { p.shared.Store(&preparing) }

// Refusals is how many requests the proxy refused while statements were
// prepared under the shared catalog lock.
func (p *Proxy) Refusals() uint64 {
	if p == nil {
		return 0
	}
	return p.refusals.Load()
}

// sharedHold is the proxy's record of the engine's shared prepares.
type sharedHold struct {
	shared   atomic.Pointer[func() bool]
	refusals atomic.Uint64
}

// refuseShared answers r, whose path under the proxy is rest, with a
// refusal if it lists a namespace's tables or views, or drops or renames a
// table or drops a namespace, while statements are prepared under the
// shared catalog lock.
func (p *Proxy) refuseShared(w http.ResponseWriter, r *http.Request, rest string) bool {
	held := p.shared.Load()
	if held == nil || !walksOrErases(r.Method, rest) || !(*held)() {
		return false
	}
	p.refusals.Add(1)
	p.log.Debug("refused a catalog request while statements are prepared under the shared catalog lock", "method", r.Method, "path", rest)
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
