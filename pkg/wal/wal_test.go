package wal

import (
	"os"
	"testing"

	"github.com/subhammahanty235/write-ahead-log-engine/pkg/types"
)

const testDir = "/tmp/wal-recovery-test"

func setup(t *testing.T) {
	// Pehle purana data clean karo
	os.RemoveAll(testDir)
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("test dir nahi bana: %v", err)
	}
}

func teardown(t *testing.T) {
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("cleanup fail hua: %v", err)
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 1: Basic WAL Open/Close
// ══════════════════════════════════════════════════════════════════════════════
func TestWALOpenClose(t *testing.T) {
	setup(t)
	defer teardown(t)

	// WAL open karo
	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	// Check karo WAL bana ya nahi
	if w == nil {
		t.Fatal("WAL nil hai")
	}

	if w.currentSegment == nil {
		t.Fatal("Current segment nil hai")
	}

	if len(w.segments) == 0 {
		t.Fatal("Segments list empty hai")
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 2: Transaction Begin aur Commit
// ══════════════════════════════════════════════════════════════════════════════
func TestTransactionCommit(t *testing.T) {
	setup(t)
	defer teardown(t)

	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	// Transaction begin karo
	txnID, err := w.Begin()
	if err != nil {
		t.Fatalf("Begin fail hua: %v", err)
	}

	// Write karo
	err = w.Write(txnID, types.PageID(1), []byte("old-data-1"), []byte("new-data-1"))
	if err != nil {
		t.Fatalf("Write fail hua: %v", err)
	}

	// Commit karo
	err = w.Commit(txnID)
	if err != nil {
		t.Fatalf("Commit fail hua: %v", err)
	}

	// Verify transaction committed hai
	txn, exists := w.activeTxns[txnID]
	if !exists {
		t.Fatal("Transaction active txns mein nahi mila")
	}
	if txn.Status != TxnCommitted {
		t.Errorf("Transaction status wrong: expected %d, got %d", TxnCommitted, txn.Status)
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 3: Transaction Abort
// ══════════════════════════════════════════════════════════════════════════════
func TestTransactionAbort(t *testing.T) {
	setup(t)
	defer teardown(t)

	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	txnID, err := w.Begin()
	if err != nil {
		t.Fatalf("Begin fail hua: %v", err)
	}

	err = w.Write(txnID, types.PageID(2), []byte("old-abort"), []byte("new-abort"))
	if err != nil {
		t.Fatalf("Write fail hua: %v", err)
	}

	// Abort karo
	err = w.Abort(txnID)
	if err != nil {
		t.Fatalf("Abort fail hua: %v", err)
	}

	txn := w.activeTxns[txnID]
	if txn.Status != TxnAborted {
		t.Errorf("Transaction status wrong: expected %d, got %d", TxnAborted, txn.Status)
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 4: Recovery - Committed transactions REDO honi chahiye
// ══════════════════════════════════════════════════════════════════════════════
func TestRecoveryRedoCommitted(t *testing.T) {
	setup(t)
	defer teardown(t)

	// ─── Phase 1: WAL open, transactions karo, close karo ───
	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	// Transaction 1 - COMMIT hogi
	txn1, _ := w.Begin()
	w.Write(txn1, types.PageID(10), []byte("page10-old"), []byte("page10-new"))
	w.Write(txn1, types.PageID(11), []byte("page11-old"), []byte("page11-new"))
	w.Commit(txn1)

	// Transaction 2 - COMMIT hogi
	txn2, _ := w.Begin()
	w.Write(txn2, types.PageID(20), []byte("page20-old"), []byte("page20-new"))
	w.Commit(txn2)

	// WAL band karo (simulating close before crash)
	// Note: No explicit Close method, we'll just open again

	// ─── Phase 2: Crash simulate - WAL dobara open karo ───
	w2, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL reopen fail hua: %v", err)
	}

	// Track karo kaunse pages REDO hue
	redoPages := make(map[types.PageID][]byte)

	// Recovery karo
	err = w2.Recover(RecoveryCallbacks{
		Redo: func(pageID types.PageID, newData []byte) error {
			redoPages[pageID] = newData
			t.Logf("REDO: Page %d -> %s", pageID, string(newData))
			return nil
		},
		Undo: func(pageID types.PageID, oldData []byte) error {
			t.Logf("UNDO: Page %d -> %s", pageID, string(oldData))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Recovery fail hui: %v", err)
	}

	// ─── Phase 3: Verify ───
	// Page 10, 11, 20 sab REDO hone chahiye
	expectedPages := []types.PageID{10, 11, 20}
	for _, pageID := range expectedPages {
		if _, exists := redoPages[pageID]; !exists {
			t.Errorf("Page %d REDO nahi hua", pageID)
		}
	}

	// Data verify karo
	if string(redoPages[types.PageID(10)]) != "page10-new" {
		t.Errorf("Page 10 data wrong: got %s", string(redoPages[types.PageID(10)]))
	}
	if string(redoPages[types.PageID(20)]) != "page20-new" {
		t.Errorf("Page 20 data wrong: got %s", string(redoPages[types.PageID(20)]))
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 5: Recovery - Uncommitted transactions UNDO honi chahiye
// ══════════════════════════════════════════════════════════════════════════════
func TestRecoveryUndoUncommitted(t *testing.T) {
	setup(t)
	defer teardown(t)

	// ─── Phase 1: WAL open, kuch commit, kuch nahi ───
	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	// Transaction 1 - COMMIT hogi
	txn1, _ := w.Begin()
	w.Write(txn1, types.PageID(100), []byte("page100-old"), []byte("page100-committed"))
	w.Commit(txn1)

	// Transaction 2 - UNCOMMITTED (crash before commit)
	txn2, _ := w.Begin()
	w.Write(txn2, types.PageID(200), []byte("page200-old"), []byte("page200-uncommitted"))
	// Deliberately NOT committing txn2 - simulating crash

	// Transaction 3 - UNCOMMITTED
	txn3, _ := w.Begin()
	w.Write(txn3, types.PageID(300), []byte("page300-old"), []byte("page300-uncommitted"))
	w.Write(txn3, types.PageID(301), []byte("page301-old"), []byte("page301-uncommitted"))
	// Deliberately NOT committing txn3

	// ─── Phase 2: Crash simulate - WAL dobara open karo ───
	w2, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL reopen fail hua: %v", err)
	}

	redoPages := make(map[types.PageID][]byte)
	undoPages := make(map[types.PageID][]byte)

	err = w2.Recover(RecoveryCallbacks{
		Redo: func(pageID types.PageID, newData []byte) error {
			redoPages[pageID] = newData
			t.Logf("REDO: Page %d -> %s", pageID, string(newData))
			return nil
		},
		Undo: func(pageID types.PageID, oldData []byte) error {
			undoPages[pageID] = oldData
			t.Logf("UNDO: Page %d -> %s", pageID, string(oldData))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Recovery fail hui: %v", err)
	}

	// ─── Phase 3: Verify ───
	// Page 100 REDO honi chahiye (committed)
	if _, exists := redoPages[types.PageID(100)]; !exists {
		t.Error("Page 100 REDO nahi hua (committed transaction)")
	}

	// Page 200, 300, 301 UNDO honi chahiye (uncommitted)
	uncommittedPages := []types.PageID{200, 300, 301}
	for _, pageID := range uncommittedPages {
		if _, exists := undoPages[pageID]; !exists {
			t.Errorf("Page %d UNDO nahi hua (uncommitted transaction)", pageID)
		}
	}

	// UNDO mein old data aana chahiye
	if string(undoPages[types.PageID(200)]) != "page200-old" {
		t.Errorf("Page 200 undo data wrong: got %s", string(undoPages[types.PageID(200)]))
	}
	if string(undoPages[types.PageID(300)]) != "page300-old" {
		t.Errorf("Page 300 undo data wrong: got %s", string(undoPages[types.PageID(300)]))
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 6: Full Recovery Scenario - Mixed Transactions
// ══════════════════════════════════════════════════════════════════════════════
func TestFullRecoveryScenario(t *testing.T) {
	setup(t)
	defer teardown(t)

	// ─── Phase 1: Complex scenario setup ───
	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	// Txn1: Committed
	txn1, _ := w.Begin()
	w.Write(txn1, types.PageID(1), []byte("A-old"), []byte("A-new"))
	w.Commit(txn1)

	// Txn2: Uncommitted (crash)
	txn2, _ := w.Begin()
	w.Write(txn2, types.PageID(2), []byte("B-old"), []byte("B-new"))
	// NO COMMIT

	// Txn3: Committed
	txn3, _ := w.Begin()
	w.Write(txn3, types.PageID(3), []byte("C-old"), []byte("C-new"))
	w.Commit(txn3)

	// Txn4: Aborted explicitly
	txn4, _ := w.Begin()
	w.Write(txn4, types.PageID(4), []byte("D-old"), []byte("D-new"))
	w.Abort(txn4)

	// Txn5: Uncommitted (crash)
	txn5, _ := w.Begin()
	w.Write(txn5, types.PageID(5), []byte("E-old"), []byte("E-new"))
	// NO COMMIT

	// ─── Phase 2: Recovery ───
	w2, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL reopen fail hua: %v", err)
	}

	redoCount := 0
	undoCount := 0
	undoPages := make(map[types.PageID]bool)

	err = w2.Recover(RecoveryCallbacks{
		Redo: func(pageID types.PageID, newData []byte) error {
			redoCount++
			t.Logf("REDO #%d: Page %d -> %s", redoCount, pageID, string(newData))
			return nil
		},
		Undo: func(pageID types.PageID, oldData []byte) error {
			undoCount++
			undoPages[pageID] = true
			t.Logf("UNDO #%d: Page %d -> %s", undoCount, pageID, string(oldData))
			return nil
		},
	})
	if err != nil {
		t.Fatalf("Recovery fail hui: %v", err)
	}

	// ─── Phase 3: Verify ───
	// REDO: Pages 1, 2, 3, 4, 5 sab REDO hongi (all writes replay)
	if redoCount != 5 {
		t.Errorf("Expected 5 REDO operations, got %d", redoCount)
	}

	// UNDO: Pages 2 aur 5 UNDO hongi (uncommitted transactions)
	// Page 4 UNDO nahi hogi kyunki Abort explicitly hua (aborted txns are handled)
	if !undoPages[types.PageID(2)] {
		t.Error("Page 2 UNDO nahi hua (uncommitted)")
	}
	if !undoPages[types.PageID(5)] {
		t.Error("Page 5 UNDO nahi hua (uncommitted)")
	}

	// Page 4 UNDO nahi honi chahiye (explicitly aborted)
	if undoPages[types.PageID(4)] {
		t.Error("Page 4 UNDO hua but it was explicitly aborted - should not undo")
	}

	t.Logf("Total REDO: %d, Total UNDO: %d", redoCount, undoCount)
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 7: Analyse function test - committed vs uncommitted detection
// ══════════════════════════════════════════════════════════════════════════════
func TestAnalyseFunction(t *testing.T) {
	setup(t)
	defer teardown(t)

	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	// Txn1: Committed
	txn1, _ := w.Begin()
	w.Write(txn1, types.PageID(1), []byte("old"), []byte("new"))
	w.Commit(txn1)

	// Txn2: Uncommitted
	txn2, _ := w.Begin()
	w.Write(txn2, types.PageID(2), []byte("old"), []byte("new"))
	// NO COMMIT

	// Txn3: Aborted
	txn3, _ := w.Begin()
	w.Write(txn3, types.PageID(3), []byte("old"), []byte("new"))
	w.Abort(txn3)

	// Analyse karo
	committed, uncommitted, err := w.analyse()
	if err != nil {
		t.Fatalf("Analyse fail hua: %v", err)
	}

	// Txn1 committed mein honi chahiye
	if !committed[txn1] {
		t.Error("Txn1 committed mein nahi mili")
	}

	// Txn2 uncommitted mein honi chahiye
	if !uncommitted[txn2] {
		t.Error("Txn2 uncommitted mein nahi mili")
	}

	// Txn3 na committed na uncommitted mein honi chahiye (aborted)
	if committed[txn3] {
		t.Error("Txn3 committed mein nahi honi chahiye (it was aborted)")
	}
	if uncommitted[txn3] {
		t.Error("Txn3 uncommitted mein nahi honi chahiye (it was aborted)")
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 8: Multiple writes in same transaction
// ══════════════════════════════════════════════════════════════════════════════
func TestMultipleWritesSameTransaction(t *testing.T) {
	setup(t)
	defer teardown(t)

	w, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL open fail hua: %v", err)
	}

	txn, _ := w.Begin()

	// Multiple writes same page pe
	w.Write(txn, types.PageID(1), []byte("v0"), []byte("v1"))
	w.Write(txn, types.PageID(1), []byte("v1"), []byte("v2"))
	w.Write(txn, types.PageID(1), []byte("v2"), []byte("v3"))
	// DON'T COMMIT - simulate crash

	w2, _ := Open(Config{Dir: testDir})

	undoData := make([]string, 0)

	w2.Recover(RecoveryCallbacks{
		Redo: func(pageID types.PageID, newData []byte) error {
			return nil
		},
		Undo: func(pageID types.PageID, oldData []byte) error {
			undoData = append(undoData, string(oldData))
			return nil
		},
	})

	// UNDO order reverse hona chahiye: v2, v1, v0
	expected := []string{"v2", "v1", "v0"}
	if len(undoData) != len(expected) {
		t.Fatalf("Expected %d undos, got %d", len(expected), len(undoData))
	}

	for i, exp := range expected {
		if undoData[i] != exp {
			t.Errorf("Undo[%d] expected %s, got %s", i, exp, undoData[i])
		}
	}
}

// ══════════════════════════════════════════════════════════════════════════════
// Test 9: WAL persistence after reopen
// ══════════════════════════════════════════════════════════════════════════════
func TestWALPersistence(t *testing.T) {
	setup(t)
	defer teardown(t)

	// First session
	w1, _ := Open(Config{Dir: testDir})
	txn1, _ := w1.Begin()
	w1.Write(txn1, types.PageID(42), []byte("old"), []byte("persisted-data"))
	w1.Commit(txn1)

	// Reopen WAL (simulating restart)
	w2, err := Open(Config{Dir: testDir})
	if err != nil {
		t.Fatalf("WAL reopen fail hua: %v", err)
	}

	// Verify LSN continued
	if w2.nextLSN <= 1 {
		t.Errorf("LSN should be > 1 after reopen, got %d", w2.nextLSN)
	}

	// Recover karo aur verify karo data hai
	found := false
	w2.Recover(RecoveryCallbacks{
		Redo: func(pageID types.PageID, newData []byte) error {
			if pageID == types.PageID(42) && string(newData) == "persisted-data" {
				found = true
			}
			return nil
		},
		Undo: func(pageID types.PageID, oldData []byte) error {
			return nil
		},
	})

	if !found {
		t.Error("Persisted data not found after reopen")
	}
}
