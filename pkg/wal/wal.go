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

func (w *WAL) analyse() (commited map[types.TxnID]bool, uncommited map[types.TxnID]bool, err error) {
	commited = make(map[types.TxnID]bool)
	uncommited = make(map[types.TxnID]bool)

	// step1 --> iterate though the compelte segments
	for _, seg := range w.segments {
		records, err := segment.ReadRecords(seg)
		if err != nil {
			return nil, nil, fmt.Errorf("segment %d read failed: %w", seg.Header.SegmentID, err)
		}

		for _, rec := range records {
			switch rec.Type {
			case types.RecordBegin:
				uncommited[rec.TxnID] = true
			case types.RecordCommit:
				commited[rec.TxnID] = true
				delete(uncommited, rec.TxnID)

			case types.RecordAbort:
				delete(commited, rec.TxnID)
				delete(uncommited, rec.TxnID)
			}
		}
	}
	return commited, uncommited, nil
}

type RecoveryCallbacks struct {
	Redo func(pageID types.PageID, newData []byte) error
	Undo func(pageID types.PageID, oldData []byte) error
}

func (w *WAL) redo(callbacks RecoveryCallbacks) error {
	for _, seg := range w.segments {
		records, err := segment.ReadRecords(seg)
		if err != nil {
			return fmt.Errorf("segment %d read failed: %w", seg.Header.SegmentID, err)
		}

		for _, rec := range records {
			if rec.Type == types.RecordWrite {
				if err := callbacks.Redo(rec.PageID, rec.NewData); err != nil {
					return fmt.Errorf("redo failed txn %d page %d: %w", rec.TxnID, rec.PageID, err)
				}
			}
		}
	}

	return nil
}

func (w *WAL) undo(uncommitted map[types.TxnID]bool, callbacks RecoveryCallbacks) error {
	var allRecords []types.Record

	for _, seg := range w.segments {
		records, err := segment.ReadRecords(seg)
		if err != nil {
			return fmt.Errorf("segment %d read failed: %w", seg.Header.SegmentID, err)
		}
		allRecords = append(allRecords, records...)
	}

	for i, j := 0, len(allRecords)-1; i < j; i, j = i+1, j-1 {
		allRecords[i], allRecords[j] = allRecords[j], allRecords[i]
	}

	for _, rec := range allRecords {
		if rec.Type == types.RecordWrite && uncommitted[rec.TxnID] {
			if err := callbacks.Undo(rec.PageID, rec.OldData); err != nil {
				return fmt.Errorf("undo failed txn %d page %d: %w", rec.TxnID, rec.PageID, err)
			}
			clr := types.Record{
				LSN:     w.nextLSN,
				TxnID:   rec.TxnID,
				Type:    types.RecordCLR,
				PageID:  rec.PageID,
				PrevLSN: rec.LSN,
			}
			if err := segment.WriteRecord(w.currentSegment, clr); err != nil {
				return fmt.Errorf("CLR write failed: %w", err)
			}
			w.nextLSN++

		}
	}

	return nil
}

func (w *WAL) Recover(callbacks RecoveryCallbacks) error {
	// Phase 1
	_, uncommitted, err := w.analyse()
	if err != nil {
		return err
	}

	// Phase 2
	if err := w.redo(callbacks); err != nil {
		return err
	}

	// Phase 3
	if err := w.undo(uncommitted, callbacks); err != nil {
		return err
	}

	return nil
}

func (w *WAL) WriteCheckpoint() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	beginRec := types.Record{
		LSN:  w.nextLSN,
		Type: types.RecordCheckpointBegin,
	}

	if err := segment.WriteRecord(w.currentSegment, beginRec); err != nil {
		return fmt.Errorf("checkpoint begin write failed: %w", err)
	}
	w.nextLSN++
	activeTxns := make([]types.TxnID, 0, len(w.activeTxns))
	for txnID := range w.activeTxns {
		activeTxns = append(activeTxns, txnID)
	}

	cd := types.CheckpointData{ActiveTxns: activeTxns}
	cdBytes, err := types.SerializeCheckpointData(cd)
	if err != nil {
		return fmt.Errorf("checkpoint data serialize failed: %w", err)
	}

	endRec := types.Record{
		LSN:     w.nextLSN,
		Type:    types.RecordCheckpointEnd,
		NewData: cdBytes,
	}
	if err := segment.WriteRecord(w.currentSegment, endRec); err != nil {
		return fmt.Errorf("checkpoint end write failed: %w", err)
	}
	w.nextLSN++

	if err := segment.SyncSegment(w.currentSegment); err != nil {
		return fmt.Errorf("checkpoint sync failed: %w", err)
	}
	return nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	lastCheckpointSegIdx := -1
	for i, seg := range w.segments {
		records, err := segment.ReadRecords(seg)
		if err != nil {
			return fmt.Errorf("segment %d read failed: %w", seg.Header.SegmentID, err)
		}

		for _, rec := range records {
			if rec.Type == types.RecordCheckpointEnd {
				lastCheckpointSegIdx = i
			}
		}
	}
	if lastCheckpointSegIdx <= 0 {
		return nil
	}

	for i := range lastCheckpointSegIdx {
		seg := w.segments[i]
		path := seg.Path()
		if err := segment.CloseSegment(seg); err != nil {
			return fmt.Errorf("segment %d close failed: %w", seg.Header.SegmentID, err)
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("segment %d delete failed: %w", seg.Header.SegmentID, err)
		}
	}

	w.segments = w.segments[lastCheckpointSegIdx:]
	return nil

}
