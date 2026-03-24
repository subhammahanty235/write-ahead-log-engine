package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/subhammahanty235/wal-proj/pkg/segment"
	"github.com/subhammahanty235/wal-proj/pkg/types"
)

type WAL struct {
	mu             sync.Mutex
	currentSegment *segment.Segment
	segments       []*segment.Segment
	nextLSN        types.LSN
	nextTxnID      uint64
	activeTxns     map[types.TxnID]*TxnState
	config         Config
}

type Config struct {
	Dir            string
	MaxSegmentSize int64
}

type TxnState struct {
	TxnID   types.TxnID
	Status  TxnStatus
	LastLSN types.LSN
}

type TxnStatus uint8

const (
	TxnActive TxnStatus = iota
	TxnCommitted
	TxnAborted
)

func Open(config Config) (*WAL, error) {
	if config.Dir == "" {
		return nil, fmt.Errorf("dir is required")
	}

	if err := os.MkdirAll(config.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL dir: %w", err)
	}

	segmentFiles, err := filepath.Glob(fmt.Sprintf("%s/segment-*.wal", config.Dir))
	if err != nil {
		return nil, fmt.Errorf("failed to list segments: %w", err)
	}

	wal := &WAL{
		segments:   make([]*segment.Segment, 0),
		activeTxns: make(map[types.TxnID]*TxnState),
		config:     config,
	}

	if len(segmentFiles) > 0 {
		sort.Strings(segmentFiles)

		for i, file := range segmentFiles {
			base := filepath.Base(file)
			var id uint64
			fmt.Sscanf(base, "segment-%d.wal", &id)
			seg, err := segment.OpenSegment(config.Dir, id)
			if err != nil {
				return nil, fmt.Errorf("failed to open segment %d: %w", i+1, err)
			}
			wal.segments = append(wal.segments, seg)
		}

		wal.currentSegment = wal.segments[len(wal.segments)-1]
		records, err := segment.ReadRecords(wal.currentSegment)
		if err != nil {
			return nil, fmt.Errorf("failed to read records: %w", err)
		}
		wal.nextLSN = 1
		if len(records) > 0 {
			wal.nextLSN = records[len(records)-1].LSN + 1
		}

	} else {
		seg, err := segment.CreateSegment(config.Dir, 1, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to create first segment: %w", err)
		}
		wal.segments = append(wal.segments, seg)
		wal.currentSegment = seg
		wal.nextLSN = 1
		wal.nextTxnID = 1
	}
	return wal, nil
}

func (w *WAL) Begin() (types.TxnID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	txnId := types.TxnID(w.nextTxnID)
	w.nextTxnID++

	rec := types.Record{
		LSN:     w.nextLSN,
		TxnID:   txnId,
		Type:    types.RecordBegin,
		PrevLSN: 0,
	}
	err := segment.WriteRecord(w.currentSegment, rec)
	if err != nil {
		return 0, err
	}
	w.nextLSN++
	w.activeTxns[txnId] = &TxnState{
		TxnID:   txnId,
		Status:  TxnActive,
		LastLSN: rec.LSN,
	}
	return txnId, nil
}

func (w *WAL) Write(txnID types.TxnID, pageID types.PageID, oldData []byte, newData []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// check if the txnid exists or not
	txn, exists := w.activeTxns[txnID]
	if !exists {
		return fmt.Errorf("txn %d not found", txnID)
	}
	if txn.Status != TxnActive {
		return fmt.Errorf("txn %d is not active (status: %d)", txnID, txn.Status)
	}

	// construct the record

	rec := types.Record{
		LSN:     w.nextLSN,
		TxnID:   txnID,
		Type:    types.RecordWrite,
		PrevLSN: txn.LastLSN,
		PageID:  pageID,
		OldData: oldData,
		NewData: newData,
	}

	if err := w.rotateIfNeeded(); err != nil {
		return fmt.Errorf("rotate failed: %w", err)
	}

	if err := segment.WriteRecord(w.currentSegment, rec); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}
	w.nextLSN++
	txn.LastLSN = rec.LSN
	return nil

}

func (w *WAL) Commit(txnID types.TxnID) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Step 2 — validate karo
	txn, exists := w.activeTxns[txnID]
	if !exists {
		return fmt.Errorf("txn %d not found", txnID)
	}
	if txn.Status != TxnActive {
		return fmt.Errorf("txn %d is not active (status: %d)", txnID, txn.Status)
	}

	rec := types.Record{
		LSN:     w.nextLSN,
		TxnID:   txnID,
		Type:    types.RecordCommit,
		PrevLSN: txn.LastLSN,
	}
	if err := w.rotateIfNeeded(); err != nil {
		return fmt.Errorf("rotate failed: %w", err)
	}
	if err := segment.WriteRecord(w.currentSegment, rec); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	// sync
	if err := segment.SyncSegment(w.currentSegment); err != nil {
		return fmt.Errorf("sync failed: %w", err)
	}
	w.nextLSN++
	txn.Status = TxnCommitted

	return nil

}

func (w *WAL) Abort(txnID types.TxnID) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Step 2 — validate karo
	txn, exists := w.activeTxns[txnID]
	if !exists {
		return fmt.Errorf("txn %d not found", txnID)
	}
	if txn.Status != TxnActive {
		return fmt.Errorf("txn %d is not active (status: %d)", txnID, txn.Status)
	}

	rec := types.Record{
		LSN:     w.nextLSN,
		TxnID:   txnID,
		Type:    types.RecordAbort,
		PrevLSN: txn.LastLSN,
	}
	if err := w.rotateIfNeeded(); err != nil {
		return fmt.Errorf("rotate failed: %w", err)
	}

	if err := segment.WriteRecord(w.currentSegment, rec); err != nil {
		return fmt.Errorf("write failed: %w", err)
	}

	w.nextLSN++
	txn.Status = TxnAborted

	return nil

}

func (w *WAL) rotateIfNeeded() error {
	if !segment.IsFull(w.currentSegment) {
		return nil
	}

	if err := segment.CloseSegment(w.currentSegment); err != nil {
		return fmt.Errorf("segment close failed: %w", err)
	}

	newID := w.currentSegment.Header.SegmentID + 1
	newSeg, err := segment.CreateSegment(w.config.Dir, newID, w.nextLSN)
	if err != nil {
		return fmt.Errorf("segment create failed: %w", err)
	}
	w.segments = append(w.segments, newSeg)
	w.currentSegment = newSeg

	return nil

}
