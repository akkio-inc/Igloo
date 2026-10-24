package erasure

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/akkio-inc/igloo/internal/iddomain"
)

// IDSets are what Igloo derives from ID domains' ids (package iddomain),
// such as bitmap indexes: sets of ids that say whom a group of rows holds
// without holding identifiers, but that would still hold an erased
// person's id, which a dictionary never gives anyone else.
//
// A request looks up the ids its subjects' values have in each domain's
// dictionary before its first DELETE, and again under the domain's lease
// just before its last delete from the dictionary, after which no value it
// erases gets an id (see deleteFromDictionaries). It keeps them sealed
// under the erasure key with its identifiers, and forgets them when it
// ends, as it does the identifiers; an IDSets never sees them outside
// these calls.
type IDSets interface {
	// Erase deletes or rebuilds every stored set of domain that holds one
	// of ids, with the old versions and files that hold them. It runs at
	// the end of the expire step, and may run again, as on a retry or after
	// a verification that found more.
	Erase(ctx context.Context, domain string, ids []int64) error
	// Holding counts the stored sets of domain that still hold one of ids,
	// which verification adds to Verification.IDSets.
	Holding(ctx context.Context, domain string, ids []int64) (int64, error)
}

// domainIDsAAD binds request id's sealed ids to it and to their purpose.
func domainIDsAAD(id string) string { return id + "/domain-ids" }

// dictionaryDomain returns the ID domain whose dictionary t is, or "".
func dictionaryDomain(t *Table) string {
	if len(t.Ident) != 2 || !strings.EqualFold(t.Ident[0], iddomain.DictionarySchema) {
		return ""
	}
	return t.Ident[1]
}

// lookUpDomainIDs looks up, in the dictionary of each ID domain r erases
// from, the ids of the values it erases (see IDSets).
func (s *Service) lookUpDomainIDs(ctx context.Context, r *Request) error {
	if s.opts.IDSets == nil {
		return nil
	}
	for i := range r.Tables {
		t := &r.Tables[i]
		if !t.active() || dictionaryDomain(t) == "" {
			continue
		}
		if err := s.addDomainIDs(ctx, r, t); err != nil {
			return err
		}
	}
	return nil
}

// addDomainIDs adds the ids that t, a dictionary, gives the values r
// erases to r's, and saves r with them sealed if they changed.
func (s *Service) addDomainIDs(ctx context.Context, r *Request, t *Table) error {
	if s.opts.IDSets == nil {
		return nil
	}
	tbl, err := s.reload(ctx, t)
	if err != nil {
		return err
	}
	cond := currentCondition(r.salt, fields(tbl, t), r.Subjects, false)
	if cond == "" {
		return nil
	}
	ids, err := s.rows.IDs(ctx, tbl, cond)
	if err != nil {
		return fmt.Errorf("look up ids in %s: %w", t.Name, err)
	}
	domain := strings.ToLower(dictionaryDomain(t))
	merged := mergeIDs(r.domainIDs[domain], ids)
	if len(merged) == len(r.domainIDs[domain]) {
		return nil
	}
	if r.domainIDs == nil {
		r.domainIDs = map[string][]int64{}
	}
	r.domainIDs[domain] = merged
	if r.SealedIDs, err = s.sealer.sealJSON(domainIDsAAD(r.ID), r.domainIDs); err != nil {
		return err
	}
	return s.save(ctx, r)
}

// mergeIDs returns the sorted union of a and b.
func mergeIDs(a, b []int64) []int64 {
	out := append(slices.Clone(a), b...)
	slices.Sort(out)
	return slices.Compact(out)
}

// domains returns the domains r has ids in, sorted.
func (r *Request) domains() []string {
	var out []string
	for d, ids := range r.domainIDs {
		if len(ids) > 0 {
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// eraseIDSets has the derived sets of ids erase r's ids, at the end of the
// expire step.
func (s *Service) eraseIDSets(ctx context.Context, r *Request) error {
	if s.opts.IDSets == nil {
		return nil
	}
	for _, d := range r.domains() {
		if err := s.opts.IDSets.Erase(ctx, d, r.domainIDs[d]); err != nil {
			return fmt.Errorf("erase the ids of ID domain %s from the sets derived from them: %w", d, err)
		}
	}
	return nil
}

// holdingIDs counts the derived sets of ids that still hold r's ids, for
// verification.
func (s *Service) holdingIDs(ctx context.Context, r *Request) (int64, error) {
	if s.opts.IDSets == nil {
		return 0, nil
	}
	var n int64
	for _, d := range r.domains() {
		k, err := s.opts.IDSets.Holding(ctx, d, r.domainIDs[d])
		if err != nil {
			return 0, fmt.Errorf("count the sets derived from ID domain %s that hold its ids: %w", d, err)
		}
		n += k
	}
	return n, nil
}
