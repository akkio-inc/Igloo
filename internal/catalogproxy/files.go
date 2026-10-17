package catalogproxy

import (
	"fmt"
	"strings"

	"github.com/apache/iceberg-go"
	iceio "github.com/apache/iceberg-go/io"
)

// addedFiles lists the files a snapshot that was never committed added:
// its manifest list, the manifests it wrote and the data and delete files
// those list as added by it. Its manifest list also names the manifests of
// the snapshots before it, and its manifests may name files of earlier
// snapshots, as deleted or existing; those are left alone. Only files
// under the table's location are listed.
func addedFiles(fs iceio.IO, location string, s snapshot) ([]string, error) {
	location = strings.TrimSuffix(location, "/") + "/"
	if !strings.HasPrefix(s.manifestList, location) {
		return nil, fmt.Errorf("manifest list %s is not under the table's location %s", s.manifestList, location)
	}
	f, err := fs.Open(s.manifestList)
	if err != nil {
		return nil, err
	}
	manifests, err := iceberg.ReadManifestList(f)
	_ = f.Close()
	if err != nil {
		return nil, fmt.Errorf("read manifest list %s: %w", s.manifestList, err)
	}

	var files []string
	add := func(path string) {
		if strings.HasPrefix(path, location) {
			files = append(files, path)
		}
	}
	for _, m := range manifests {
		if m.SnapshotID() != s.id {
			continue
		}
		entries, err := m.FetchEntries(fs, false)
		if err != nil {
			return nil, fmt.Errorf("read manifest %s: %w", m.FilePath(), err)
		}
		for _, e := range entries {
			if e.Status() == iceberg.EntryStatusADDED && e.SnapshotID() == s.id {
				add(e.DataFile().FilePath())
			}
		}
		add(m.FilePath())
	}
	add(s.manifestList)
	return files, nil
}
