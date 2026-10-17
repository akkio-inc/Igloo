// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Igloo's addition to iceberg-go; see IGLOO_PATCHES.md.

package table

import (
	"context"
	"fmt"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/internal"
	iceio "github.com/apache/iceberg-go/io"
)

// ForgetFiles commits a snapshot with the current snapshot's data whose
// manifests no longer mention paths. A file that a commit removed stays in
// its manifests as a DELETED entry, with its column bounds, until the
// manifest is written again, and manifests are carried into every later
// snapshot. Manifests that mention one of paths are written again with
// only their live entries, and left out when none are left. It fails if
// one of paths is live in the current snapshot.
func (t *Transaction) ForgetFiles(ctx context.Context, paths map[string]struct{}, snapshotProps iceberg.Properties) error {
	if t.meta.currentSnapshot() == nil {
		return fmt.Errorf("%w: cannot forget files of a table without a snapshot", ErrInvalidOperation)
	}
	fs, err := t.tbl.fsF(ctx)
	if err != nil {
		return err
	}
	wfs, ok := fs.(iceio.WriteFileIO)
	if !ok {
		return ErrWriteIORequired
	}
	prod := createSnapshotProducer(OpReplace, t, wfs, nil, snapshotProps)
	prod.producerImpl = &forgetFiles{base: prod, paths: paths}
	updates, reqs, err := prod.commit(ctx)
	if err != nil {
		return err
	}

	return t.apply(updates, reqs)
}

type forgetFiles struct {
	base  *snapshotProducer
	paths map[string]struct{}
}

func (f *forgetFiles) processManifests(manifests []iceberg.ManifestFile) ([]iceberg.ManifestFile, error) {
	return manifests, nil
}

func (f *forgetFiles) deletedEntries(context.Context) ([]iceberg.ManifestEntry, error) {
	return nil, nil
}

// validate is a no-op: the commit changes no data, and, as a replace, it
// is never retried (see retriable), so its requirement on the branch
// head refuses it if another commit came first.
func (f *forgetFiles) validate(*conflictContext) error { return nil }

func (f *forgetFiles) needsValidation() bool { return false }

func (f *forgetFiles) existingManifests() ([]iceberg.ManifestFile, error) {
	manifests, err := f.base.txn.meta.currentSnapshot().Manifests(f.base.io)
	if err != nil {
		return nil, err
	}
	out := make([]iceberg.ManifestFile, 0, len(manifests))
	for _, m := range manifests {
		var live []iceberg.ManifestEntry
		mentions := false
		for entry, err := range m.Entries(f.base.io, false) {
			if err != nil {
				return nil, err
			}
			_, forget := f.paths[entry.DataFile().FilePath()]
			if entry.Status() == iceberg.EntryStatusDELETED {
				mentions = mentions || forget
				continue
			}
			if forget {
				return nil, fmt.Errorf("%w: %s is live in the current snapshot", ErrInvalidOperation, entry.DataFile().FilePath())
			}
			live = append(live, entry)
		}
		if !mentions {
			out = append(out, m)
			continue
		}
		if len(live) == 0 {
			continue
		}
		rewritten, err := f.rewrite(m, live)
		if err != nil {
			return nil, err
		}
		out = append(out, rewritten)
	}

	return out, nil
}

// rewrite writes m again with only its live entries.
func (f *forgetFiles) rewrite(m iceberg.ManifestFile, live []iceberg.ManifestEntry) (_ iceberg.ManifestFile, err error) {
	spec, err := f.base.txn.meta.GetSpecByID(int(m.PartitionSpecID()))
	if err != nil {
		return nil, err
	}
	wr, path, counter, closer, err := f.base.newManifestWriter(*spec, iceberg.WithManifestWriterContent(m.ManifestContent()))
	if err != nil {
		return nil, err
	}
	defer internal.CheckedClose(closer, &err)
	defer internal.CheckedClose(wr, &err)
	for _, entry := range live {
		if err := wr.Existing(entry); err != nil {
			return nil, err
		}
	}
	if err := wr.Close(); err != nil {
		return nil, err
	}

	return wr.ToManifestFile(path, counter.Count, iceberg.WithManifestFileContent(m.ManifestContent()))
}
