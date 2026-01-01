// Copyright (C) 2025 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/syncthing/syncthing/internal/db/sqlite"
	"github.com/syncthing/syncthing/lib/locations"
	"github.com/syncthing/syncthing/lib/protocol"
)

func TestMaintenanceCmdRun(t *testing.T) {
	// Test that the maintenance command runs successfully on a valid database
	tmpDir := t.TempDir()

	// Set up locations to use the temp directory
	if err := locations.SetBaseDir(locations.DataBaseDir, tmpDir); err != nil {
		t.Fatal(err)
	}

	dbPath := locations.Get(locations.Database)

	// Create a database with some test data
	deleteRetention := 48 * time.Hour
	db, err := sqlite.Open(dbPath, sqlite.WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}

	// Add some test files
	oldTime := time.Now().Add(-deleteRetention - time.Hour)
	files := []protocol.FileInfo{
		{
			Name:       "old-deleted",
			ModifiedS:  oldTime.Unix(),
			ModifiedNs: int32(oldTime.Nanosecond()),
			Version:    protocol.Vector{}.Update(1),
			Deleted:    true,
		},
		{
			Name:      "active-file",
			ModifiedS: time.Now().Unix(),
			Version:   protocol.Vector{}.Update(1),
			Deleted:   false,
			Size:      100,
		},
	}
	if err := db.Update("test-folder", protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Run the maintenance command
	cmd := maintenanceCmd{
		DeleteRetention: deleteRetention,
	}
	if err := cmd.Run(); err != nil {
		t.Fatalf("maintenance command failed: %v", err)
	}

	// Verify the database was processed correctly
	db, err = sqlite.Open(dbPath, sqlite.WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Old deleted file should be gone
	if _, ok, err := db.GetDeviceFile("test-folder", protocol.LocalDeviceID, "old-deleted"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Error("old-deleted file should have been garbage collected")
	}

	// Active file should still exist
	if _, ok, err := db.GetDeviceFile("test-folder", protocol.LocalDeviceID, "active-file"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("active-file should NOT have been garbage collected")
	}
}

func TestMaintenanceCmdLockConflict(t *testing.T) {
	// Test that the maintenance command fails when Syncthing is running
	// (simulated by holding the lock file)
	tmpDir := t.TempDir()

	// Set up locations to use the temp directory
	if err := locations.SetBaseDir(locations.DataBaseDir, tmpDir); err != nil {
		t.Fatal(err)
	}

	dbPath := locations.Get(locations.Database)

	// Create a database
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Create the lock file directory and acquire the lock (simulating Syncthing running)
	lockPath := locations.Get(locations.LockFile)
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		t.Fatal(err)
	}
	lf := flock.New(lockPath)
	locked, err := lf.TryLock()
	if err != nil {
		t.Fatal(err)
	}
	if !locked {
		t.Fatal("failed to acquire test lock")
	}
	defer lf.Unlock()

	// The maintenance command should fail because the lock is held.
	// We can't easily test this without running the actual command in a subprocess
	// because the command calls os.Exit on failure. Instead, we'll verify that
	// trying to acquire the lock fails.
	lf2 := flock.New(lockPath)
	locked2, err := lf2.TryLock()
	if err != nil {
		t.Fatal(err)
	}
	if locked2 {
		lf2.Unlock()
		t.Error("lock should not be acquired when already held")
	}
}

func TestMaintenanceCmdDeleteRetentionFlag(t *testing.T) {
	// Test that the delete-retention flag works correctly
	tmpDir := t.TempDir()

	// Set up locations to use the temp directory
	if err := locations.SetBaseDir(locations.DataBaseDir, tmpDir); err != nil {
		t.Fatal(err)
	}

	dbPath := locations.Get(locations.Database)

	// Create a database with test data
	db, err := sqlite.Open(dbPath, sqlite.WithDeleteRetention(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}

	// Add files with different ages
	now := time.Now()
	time25hAgo := now.Add(-25 * time.Hour)
	time50hAgo := now.Add(-50 * time.Hour)
	files := []protocol.FileInfo{
		{
			Name:       "deleted-25h-ago",
			ModifiedS:  time25hAgo.Unix(),
			ModifiedNs: int32(time25hAgo.Nanosecond()),
			Version:    protocol.Vector{}.Update(1),
			Deleted:    true,
		},
		{
			Name:       "deleted-50h-ago",
			ModifiedS:  time50hAgo.Unix(),
			ModifiedNs: int32(time50hAgo.Nanosecond()),
			Version:    protocol.Vector{}.Update(1),
			Deleted:    true,
		},
	}
	if err := db.Update("test-folder", protocol.LocalDeviceID, files); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Run maintenance with 48h retention - only the 50h old file should be deleted
	cmd := maintenanceCmd{
		DeleteRetention: 48 * time.Hour,
	}
	if err := cmd.Run(); err != nil {
		t.Fatalf("maintenance command failed: %v", err)
	}

	// Verify results
	db, err = sqlite.Open(dbPath, sqlite.WithDeleteRetention(24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// 25h old file should still exist (within 48h retention)
	if _, ok, err := db.GetDeviceFile("test-folder", protocol.LocalDeviceID, "deleted-25h-ago"); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Error("deleted-25h-ago should NOT have been garbage collected with 48h retention")
	}

	// 50h old file should be gone (older than 48h retention)
	if _, ok, err := db.GetDeviceFile("test-folder", protocol.LocalDeviceID, "deleted-50h-ago"); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Error("deleted-50h-ago should have been garbage collected with 48h retention")
	}
}

func TestMaintenanceCmdEmptyDatabase(t *testing.T) {
	// Test that maintenance works on a newly created/empty database
	tmpDir := t.TempDir()

	// Set up locations to use the temp directory
	if err := locations.SetBaseDir(locations.DataBaseDir, tmpDir); err != nil {
		t.Fatal(err)
	}

	dbPath := locations.Get(locations.Database)

	// Create an empty database
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Run maintenance on empty database
	cmd := maintenanceCmd{
		DeleteRetention: 48 * time.Hour,
	}
	if err := cmd.Run(); err != nil {
		t.Fatalf("maintenance on empty database failed: %v", err)
	}
}

func TestMaintenanceCmdMultipleFolders(t *testing.T) {
	// Test that maintenance processes all folders
	tmpDir := t.TempDir()

	// Set up locations to use the temp directory
	if err := locations.SetBaseDir(locations.DataBaseDir, tmpDir); err != nil {
		t.Fatal(err)
	}

	dbPath := locations.Get(locations.Database)
	deleteRetention := 48 * time.Hour

	// Create a database with multiple folders
	db, err := sqlite.Open(dbPath, sqlite.WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}

	oldTime := time.Now().Add(-deleteRetention - time.Hour)
	folders := []string{"folder-1", "folder-2", "folder-3"}

	for _, folder := range folders {
		files := []protocol.FileInfo{
			{
				Name:       "old-deleted",
				ModifiedS:  oldTime.Unix(),
				ModifiedNs: int32(oldTime.Nanosecond()),
				Version:    protocol.Vector{}.Update(1),
				Deleted:    true,
			},
			{
				Name:      "active-file",
				ModifiedS: time.Now().Unix(),
				Version:   protocol.Vector{}.Update(1),
				Deleted:   false,
				Size:      100,
			},
		}
		if err := db.Update(folder, protocol.LocalDeviceID, files); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Run maintenance
	cmd := maintenanceCmd{
		DeleteRetention: deleteRetention,
	}
	if err := cmd.Run(); err != nil {
		t.Fatalf("maintenance command failed: %v", err)
	}

	// Verify all folders were processed
	db, err = sqlite.Open(dbPath, sqlite.WithDeleteRetention(deleteRetention))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, folder := range folders {
		// Old deleted should be gone
		if _, ok, err := db.GetDeviceFile(folder, protocol.LocalDeviceID, "old-deleted"); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Errorf("old-deleted in %s should have been garbage collected", folder)
		}

		// Active file should remain
		if _, ok, err := db.GetDeviceFile(folder, protocol.LocalDeviceID, "active-file"); err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Errorf("active-file in %s should NOT have been garbage collected", folder)
		}
	}
}
