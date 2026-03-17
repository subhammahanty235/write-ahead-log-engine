package segment

import (
	"os"
	"testing"

	types "github.com/subhammahanty235/wal-proj/pkg/types"
)

const testDir = "/tmp/wal-test"

func setup(t *testing.T) {
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("test dir nahi bana: %v", err)
	}
}

func teardown(t *testing.T) {
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("cleanup fail hua: %v", err)
	}
}

// ── Test 1: Segment banta hai ya nahi ───────────────────────
func TestCreateSegment(t *testing.T) {
	setup(t)
	defer teardown(t)

	seg, err := CreateSegment(testDir, 1, 0)
	if err != nil {
		t.Fatalf("CreateSegment fail hua: %v", err)
	}
	defer seg.file.Close()

	// File exist karti hai ya nahi
	filename := testDir + "/segment-000001.wal"
	if _, err := os.Stat(filename); os.IsNotExist(err) {
		t.Errorf("File bani hi nahi: %s", filename)
	}
}

// ── Test 2: Write karo, wapas padho, compare karo ───────────
func TestWriteAndReadRecords(t *testing.T) {
	setup(t)
	defer teardown(t)

	seg, err := CreateSegment(testDir, 2, 0)
	if err != nil {
		t.Fatalf("CreateSegment fail hua: %v", err)
	}
	defer seg.file.Close()

	records := []types.Record{
		{
			LSN:     1,
			TxnID:   100,
			Type:    types.RecordType(1),
			PageID:  10,
			PrevLSN: 0,
			OldData: []byte("purana data 1"),
			NewData: []byte("naya data 1"),
		},
		{
			LSN:     2,
			TxnID:   101,
			Type:    types.RecordType(1),
			PageID:  11,
			PrevLSN: 1,
			OldData: []byte("purana data 2"),
			NewData: []byte("naya data 2"),
		},
	}

	// Write karo
	for _, r := range records {
		if err := WriteRecord(seg, r); err != nil {
			t.Fatalf("WriteRecord fail hua: %v", err)
		}
	}

	// Wapas padho
	result, err := ReadRecords(seg)
	if err != nil {
		t.Fatalf("ReadRecords fail hua: %v", err)
	}

	// Count check
	if len(result) != len(records) {
		t.Fatalf("Record count mismatch: want %d, got %d", len(records), len(result))
	}

	// Compare karo
	for i := range records {
		if records[i].LSN != result[i].LSN {
			t.Errorf("[%d] LSN mismatch: want %v, got %v", i, records[i].LSN, result[i].LSN)
		}
		if records[i].TxnID != result[i].TxnID {
			t.Errorf("[%d] TxnID mismatch: want %v, got %v", i, records[i].TxnID, result[i].TxnID)
		}
		if records[i].PageID != result[i].PageID {
			t.Errorf("[%d] PageID mismatch: want %v, got %v", i, records[i].PageID, result[i].PageID)
		}
		if string(records[i].OldData) != string(result[i].OldData) {
			t.Errorf("[%d] OldData mismatch: want %s, got %s", i, records[i].OldData, result[i].OldData)
		}
		if string(records[i].NewData) != string(result[i].NewData) {
			t.Errorf("[%d] NewData mismatch: want %s, got %s", i, records[i].NewData, result[i].NewData)
		}
	}
}

// ── Test 3: Segment full hone pe error ──────────────────────
func TestSegmentFull(t *testing.T) {
	setup(t)
	defer teardown(t)

	seg, err := CreateSegment(testDir, 3, 0)
	if err != nil {
		t.Fatalf("CreateSegment fail hua: %v", err)
	}
	defer seg.file.Close()

	// maxSize chhota karo — sirf 50 bytes
	seg.maxSize = 50

	r := types.Record{
		LSN:     1,
		TxnID:   100,
		Type:    types.RecordType(1),
		PageID:  10,
		PrevLSN: 0,
		OldData: []byte("bada purana data jo limit cross karega"),
		NewData: []byte("bada naya data jo limit cross karega"),
	}

	err = WriteRecord(seg, r)
	if err == nil {
		t.Fatal("Error aana chahiye tha full segment pe, par nahi aaya")
	}
}
