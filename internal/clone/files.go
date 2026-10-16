package clone

import (
	"strings"

	iceio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Under reports whether path is a file inside location. The S3 scheme's
// aliases count as the same scheme, as Iceberg engines treat them.
func Under(path, location string) bool {
	return strings.HasPrefix(canonical(path), canonical(trimSlash(location))+"/")
}

func canonical(path string) string {
	for _, alias := range []string{"s3a://", "s3n://"} {
		if strings.HasPrefix(path, alias) {
			return "s3://" + path[len(alias):]
		}
	}
	return path
}

// fileSet is a set of file paths, in canonical form.
type fileSet map[string]struct{}

func (s fileSet) add(path string)      { s[canonical(path)] = struct{}{} }
func (s fileSet) has(path string) bool { _, ok := s[canonical(path)]; return ok }

// under reports whether any file of s is inside location.
func (s fileSet) under(location string) bool {
	for p := range s {
		if Under(p, location) {
			return true
		}
	}
	return false
}

// reader reads what snapshots reference, reading each manifest once
// however many snapshots, or tables, list it.
type reader struct {
	fs   iceio.IO
	read map[string]bool
	// within, if set, is the location outside which files are left out.
	within string
}

func newReader(fs iceio.IO) *reader { return &reader{fs: fs, read: map[string]bool{}} }

func (r *reader) add(out fileSet, path string) {
	if r.within == "" || Under(path, r.within) {
		out.add(path)
	}
}

// snapshot adds to out the files snap references: its manifest list, its
// manifests and the data and delete files they list as live. With
// removed, the files they list as removed count too. A manifest this
// reader read before adds only itself, as its files are in out already;
// so does one in skip.
func (r *reader) snapshot(snap table.Snapshot, out fileSet, removed bool, skip fileSet) error {
	if snap.ManifestList != "" {
		r.add(out, snap.ManifestList)
	}
	manifests, err := snap.Manifests(r.fs)
	if err != nil {
		return err
	}
	for _, man := range manifests {
		path := canonical(man.FilePath())
		r.add(out, path)
		if r.read[path] || skip.has(path) {
			continue
		}
		r.read[path] = true
		for entry, err := range man.Entries(r.fs, !removed) {
			if err != nil {
				return err
			}
			r.add(out, entry.DataFile().FilePath())
		}
	}
	return nil
}

// referenced adds to out every file meta's snapshots and statistics
// reference.
func (r *reader) referenced(meta table.Metadata, out fileSet) error {
	for _, snap := range meta.Snapshots() {
		if err := r.snapshot(snap, out, false, nil); err != nil {
			return err
		}
	}
	for sf := range meta.Statistics() {
		if sf.StatisticsPath != "" {
			r.add(out, sf.StatisticsPath)
		}
	}
	for sf := range meta.PartitionStatistics() {
		if sf.StatisticsPath != "" {
			r.add(out, sf.StatisticsPath)
		}
	}
	return nil
}

// Unreferenced returns the files inside location that pre references and
// post, the same table after snapshots expired, no longer does: the
// manifest lists, manifests, data and delete files and statistics only the
// expired snapshots used. It is what iceberg-go deletes after expiring
// snapshots, less every file outside the table's own location: those
// belong to the table a clone was made from, which deletes them once no
// table needs them.
func Unreferenced(fs iceio.IO, pre, post table.Metadata, location string) ([]string, error) {
	kept := fileSet{}
	if err := newReader(fs).referenced(post, kept); err != nil {
		return nil, err
	}
	retained := map[int64]bool{}
	for _, s := range post.Snapshots() {
		retained[s.SnapshotID] = true
	}
	// A manifest the table still uses lists as removed only files that
	// a retained snapshot removed, which an older manifest lists as live
	// if any expired snapshot did; so only the others are read.
	gone, r := fileSet{}, newReader(fs)
	for _, snap := range pre.Snapshots() {
		if retained[snap.SnapshotID] {
			continue
		}
		if err := r.snapshot(snap, gone, true, kept); err != nil {
			return nil, err
		}
	}
	for sf := range pre.Statistics() {
		if !retained[sf.SnapshotID] && sf.StatisticsPath != "" {
			gone.add(sf.StatisticsPath)
		}
	}
	for sf := range pre.PartitionStatistics() {
		if !retained[sf.SnapshotID] && sf.StatisticsPath != "" {
			gone.add(sf.StatisticsPath)
		}
	}
	var out []string
	for p := range gone {
		if !kept.has(p) && Under(p, location) {
			out = append(out, p)
		}
	}
	return out, nil
}
