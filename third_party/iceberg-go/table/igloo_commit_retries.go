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

import "github.com/apache/iceberg-go"

// retriable reports whether doCommit may retry updates after a conflict.
// A retry writes each new snapshot's manifest list again with the new
// head's manifests added to the snapshot's own, and moves the commit's
// requirement on the branch head to the new head. That is right only for
// a commit that adds snapshots, all of them plain appends:
//
//   - a snapshot that removed or replaced files (a rewrite, a delete, an
//     overwrite) would list them again, live, from the new head's
//     manifests, beside what replaced them;
//   - a commit that adds no snapshot (expiring snapshots, moving or
//     dropping refs, a rollback) asserts the refs it read, and moving
//     those assertions to the new head would apply it to a state it never
//     saw;
//   - with commit.manifest-merge.enabled, an append's own manifests can be
//     merged with those of the head it was planned on, which the retry
//     adds again.
func retriable(updates []Update, props iceberg.Properties) bool {
	if props.GetBool(ManifestMergeEnabledKey, ManifestMergeEnabledDefault) {
		return false
	}
	added := false
	for _, u := range updates {
		su, ok := u.(*addSnapshotUpdate)
		if !ok {
			continue
		}
		if su.Snapshot == nil || su.Snapshot.Summary == nil || su.Snapshot.Summary.Operation != OpAppend {
			return false
		}
		added = true
	}

	return added
}
