// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package sqlite

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

func TestBlobRange(t *testing.T) {
	exp := `
hash < x'249249'
hash >= x'249249' AND hash < x'492492'
hash >= x'492492' AND hash < x'6db6db'
hash >= x'6db6db' AND hash < x'924924'
hash >= x'924924' AND hash < x'b6db6d'
hash >= x'b6db6d' AND hash < x'db6db6'
hash >= x'db6db6'
	`

	ranges := blobRanges(7)
	buf := new(bytes.Buffer)
	for _, r := range ranges {
		fmt.Fprintln(buf, r.SQL("hash"))
	}

	if strings.TrimSpace(buf.String()) != strings.TrimSpace(exp) {
		t.Log(buf.String())
		t.Error("unexpected output")
	}
}

func TestTombstoneGarbageCollection(t *testing.T) {
	t.Parallel()

	const folderID = "test"
	const deleteRetention = 48 * time.Hour

	sdb, err := Open(t.TempDir(), WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})
	svc := sdb.Service(time.Hour).(*Service)

	// Helper to count files in the database
	countFiles := func() int {
		fdb, err := sdb.getFolderDB(folderID, false)
		if err != nil {
			t.Fatal(err)
		}
		var count int
		if err := fdb.sql.Get(&count, `SELECT count(*) FROM files`); err != nil {
			t.Fatal(err)
		}
		return count
	}

	// Create files with different states:
	// 1. Old deleted file (should be deleted by GC)
	// 2. Recently deleted file (should NOT be deleted - within retention)
	// 3. Non-deleted file (should NOT be deleted)
	// 4. Old deleted file that is still needed (should NOT be deleted)

	now := time.Now()
	oldTime := now.Add(-deleteRetention - time.Hour) // Older than retention
	recentTime := now.Add(-time.Hour)                // Within retention

	// File 1: Old deleted file - should be garbage collected
	oldDeleted := protocol.FileInfo{
		Name:       "old-deleted",
		ModifiedS:  oldTime.Unix(),
		ModifiedNs: int32(oldTime.Nanosecond()),
		Version:    protocol.Vector{}.Update(1),
		Deleted:    true,
		Size:       0,
	}

	// File 2: Recently deleted file - should NOT be garbage collected
	recentDeleted := protocol.FileInfo{
		Name:       "recent-deleted",
		ModifiedS:  recentTime.Unix(),
		ModifiedNs: int32(recentTime.Nanosecond()),
		Version:    protocol.Vector{}.Update(1),
		Deleted:    true,
		Size:       0,
	}

	// File 3: Non-deleted file - should NOT be garbage collected
	nonDeleted := protocol.FileInfo{
		Name:       "non-deleted",
		ModifiedS:  oldTime.Unix(),
		ModifiedNs: int32(oldTime.Nanosecond()),
		Version:    protocol.Vector{}.Update(1),
		Deleted:    false,
		Size:       100,
	}

	// Insert all files
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{oldDeleted, recentDeleted, nonDeleted}); err != nil {
		t.Fatal(err)
	}

	// Verify initial state
	if count := countFiles(); count != 3 {
		t.Fatalf("expected 3 files initially, got %d", count)
	}

	// Run GC
	if err := svc.periodic(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify: old deleted file should be gone, others should remain
	if count := countFiles(); count != 2 {
		t.Errorf("expected 2 files after GC, got %d", count)
	}

	// Verify specific files
	if _, ok, err := sdb.GetDeviceFile(folderID, protocol.LocalDeviceID, "old-deleted"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Error("old-deleted file should have been garbage collected")
	}

	if _, ok, err := sdb.GetDeviceFile(folderID, protocol.LocalDeviceID, "recent-deleted"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("recent-deleted file should NOT have been garbage collected")
	}

	if _, ok, err := sdb.GetDeviceFile(folderID, protocol.LocalDeviceID, "non-deleted"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("non-deleted file should NOT have been garbage collected")
	}
}

func TestTombstoneGCWithNeededFlag(t *testing.T) {
	// Test that files with FlagLocalNeeded are not garbage collected even if old
	t.Parallel()

	const folderID = "test"
	const deleteRetention = 48 * time.Hour

	sdb, err := Open(t.TempDir(), WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})
	svc := sdb.Service(time.Hour).(*Service)

	now := time.Now()
	oldTime := now.Add(-deleteRetention - time.Hour)

	// Create an old deleted file from a remote device.
	// When a remote device announces a deleted file, we set FlagLocalNeeded
	// until we process the deletion locally.
	oldDeletedRemote := protocol.FileInfo{
		Name:       "old-deleted-remote",
		ModifiedS:  oldTime.Unix(),
		ModifiedNs: int32(oldTime.Nanosecond()),
		Version:    protocol.Vector{}.Update(42),
		Deleted:    true,
		Size:       0,
	}

	// Insert from remote device - this will set FlagLocalNeeded since we need to process it
	if err := sdb.Update(folderID, protocol.DeviceID{42}, []protocol.FileInfo{oldDeletedRemote}); err != nil {
		t.Fatal(err)
	}

	// Verify the file exists and has the needed flag
	fdb, err := sdb.getFolderDB(folderID, false)
	if err != nil {
		t.Fatal(err)
	}

	var count int
	if err := fdb.sql.Get(&count, `SELECT count(*) FROM files WHERE deleted = 1`); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected 1 deleted file, got %d", count)
	}

	// Run GC - the file should NOT be deleted because we haven't processed it locally yet
	// (it still has FlagLocalNeeded set, or it's from a remote device which means we need it)
	if err := svc.periodic(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify the file still exists
	if err := fdb.sql.Get(&count, `SELECT count(*) FROM files WHERE deleted = 1`); err != nil {
		t.Fatal(err)
	}
	// The file should still exist because it's a remote file and the local device
	// hasn't processed it yet (the local version would need to exist without FlagLocalNeeded)
	if count != 1 {
		t.Errorf("expected 1 deleted file after GC (file with needed flag should not be deleted), got %d", count)
	}
}

func TestTombstoneGCDisabled(t *testing.T) {
	// Test that GC is skipped when deleteRetention is 0 (disabled)
	t.Parallel()

	const folderID = "test"

	// Open without delete retention (disabled)
	sdb, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})
	svc := sdb.Service(time.Hour).(*Service)

	// Create an old deleted file
	oldTime := time.Now().Add(-365 * 24 * time.Hour) // Very old
	oldDeleted := protocol.FileInfo{
		Name:       "old-deleted",
		ModifiedS:  oldTime.Unix(),
		ModifiedNs: int32(oldTime.Nanosecond()),
		Version:    protocol.Vector{}.Update(1),
		Deleted:    true,
		Size:       0,
	}

	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{oldDeleted}); err != nil {
		t.Fatal(err)
	}

	// Run GC
	if err := svc.periodic(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Verify file still exists (GC is disabled)
	if _, ok, err := sdb.GetDeviceFile(folderID, protocol.LocalDeviceID, "old-deleted"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("old-deleted file should NOT have been garbage collected when retention is disabled")
	}
}

func TestTombstoneGCChunking(t *testing.T) {
	// Test that multiple tombstones are deleted correctly (tests the chunking loop)
	t.Parallel()

	const folderID = "test"
	const deleteRetention = 48 * time.Hour
	const numTombstones = 100 // Create many tombstones to exercise the loop

	sdb, err := Open(t.TempDir(), WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := sdb.Close(); err != nil {
			t.Fatal(err)
		}
	})
	svc := sdb.Service(time.Hour).(*Service)

	oldTime := time.Now().Add(-deleteRetention - time.Hour)

	// Create many old deleted files
	files := make([]protocol.FileInfo, numTombstones)
	for i := range files {
		files[i] = protocol.FileInfo{
			Name:       fmt.Sprintf("deleted-%04d", i),
			ModifiedS:  oldTime.Unix(),
			ModifiedNs: int32(oldTime.Nanosecond()),
			Version:    protocol.Vector{}.Update(1),
			Deleted:    true,
			Size:       0,
		}
	}

	if err := sdb.Update(folderID, protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}

	// Also add a non-deleted file to make sure it's not affected
	nonDeleted := protocol.FileInfo{
		Name:       "keep-me",
		ModifiedS:  oldTime.Unix(),
		ModifiedNs: int32(oldTime.Nanosecond()),
		Version:    protocol.Vector{}.Update(1),
		Deleted:    false,
		Size:       100,
	}
	if err := sdb.Update(folderID, protocol.LocalDeviceID, []protocol.FileInfo{nonDeleted}); err != nil {
		t.Fatal(err)
	}

	// Count files before GC
	fdb, err := sdb.getFolderDB(folderID, false)
	if err != nil {
		t.Fatal(err)
	}

	var countBefore int
	if err := fdb.sql.Get(&countBefore, `SELECT count(*) FROM files`); err != nil {
		t.Fatal(err)
	}
	if countBefore != numTombstones+1 {
		t.Fatalf("expected %d files before GC, got %d", numTombstones+1, countBefore)
	}

	// Run GC
	if err := svc.periodic(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Count files after GC - only the non-deleted file should remain
	var countAfter int
	if err := fdb.sql.Get(&countAfter, `SELECT count(*) FROM files`); err != nil {
		t.Fatal(err)
	}
	if countAfter != 1 {
		t.Errorf("expected 1 file after GC (the non-deleted one), got %d", countAfter)
	}

	// Verify the non-deleted file is still there
	if _, ok, err := sdb.GetDeviceFile(folderID, protocol.LocalDeviceID, "keep-me"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("non-deleted file should still exist after GC")
	}
}
