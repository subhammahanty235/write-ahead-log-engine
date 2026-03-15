package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	types "github.com/subhammahanty235/wal-proj/pkg/types"
)

type SegmentHeader struct {
	Version   uint32
	SegmentID uint64
	CreatedAt int64
	FirstLSN  types.LSN
}

type Segment struct {
	file    *os.File
	header  SegmentHeader
	size    int64
	maxSize int64
}

func serializeHeader(h SegmentHeader) ([]byte, error) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, h.Version)
	binary.Write(buf, binary.LittleEndian, h.SegmentID)
	binary.Write(buf, binary.LittleEndian, h.CreatedAt)
	binary.Write(buf, binary.LittleEndian, h.FirstLSN)
	return buf.Bytes(), nil
}

func CreateSegment(dir string, id uint64, firstLSN types.LSN) (*Segment, error) {
	filename := fmt.Sprintf("%s/segment-%06d.wal", dir, id)

	header := SegmentHeader{
		Version:   1,
		SegmentID: id,
		CreatedAt: time.Now().Unix(),
		FirstLSN:  firstLSN,
	}

	headerBytes, err := serializeHeader(header)
	if err != nil {
		return nil, err
	}

	file, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment file: %w", err)
	}
	if _, err := file.Write(headerBytes); err != nil {
		return nil, fmt.Errorf("failed to write header: %w", err)
	}

	return &Segment{
		file:    file,
		header:  header,
		size:    0,
		maxSize: 64 * 1024 * 1024,
	}, nil

}

func WriteRecord(s *Segment, r types.Record) error {
	recordBytes, err := types.Serialize(r)
	if err != nil {
		return err
	}

	if int64(len(recordBytes))+s.size >= s.maxSize {
		return fmt.Errorf("Record is exceeding the size")
	}

	file := s.file
	if _, err := file.Write(recordBytes); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	// update the size
	s.size = s.size + int64(len(recordBytes))
	return nil
}
