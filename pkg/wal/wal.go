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
	}
	return wal, nil
}
