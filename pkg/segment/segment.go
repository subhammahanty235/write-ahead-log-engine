package segment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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

func deserializeHeader(data []byte) (SegmentHeader, error) {
	buf := bytes.NewReader(data)

	var version uint32
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return SegmentHeader{}, fmt.Errorf("failed to read Version: %w", err)
	}

	var segmentID uint64
	if err := binary.Read(buf, binary.LittleEndian, &segmentID); err != nil {
		return SegmentHeader{}, fmt.Errorf("failed to read SegmentID: %w", err)
	}

	var createdAt int64
	if err := binary.Read(buf, binary.LittleEndian, &createdAt); err != nil {
		return SegmentHeader{}, fmt.Errorf("failed to read CreatedAt: %w", err)
	}

	var firstLSN types.LSN
	if err := binary.Read(buf, binary.LittleEndian, &firstLSN); err != nil {
		return SegmentHeader{}, fmt.Errorf("failed to read FirstLSN: %w", err)
	}

	return SegmentHeader{
		Version:   version,
		SegmentID: segmentID,
		CreatedAt: createdAt,
		FirstLSN:  firstLSN,
	}, nil
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

func ReadRecords(s *Segment) ([]types.Record, error) {
	s.file.Seek(28, io.SeekStart)

	var records []types.Record

	for {
		var totalSize uint32
		if err := binary.Read(s.file, binary.LittleEndian, &totalSize); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read TotalSize: %w", err)
		}
		fmt.Printf("totalSize: %d\n", totalSize)
		remaining := make([]byte, totalSize-4)
		if _, err := io.ReadFull(s.file, remaining); err != nil {
			return nil, fmt.Errorf("failed to read record body: %w", err)
		}
		sizeBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(sizeBuf, totalSize)
		fullRecord := append(sizeBuf, remaining...)

		record, err := types.DeSerialize(fullRecord)
		if err != nil {
			return nil, fmt.Errorf("deserialize fail hua: %w", err)
		}
		records = append(records, record)
	}
	return records, nil
}

func OpenSegment(dir string, id uint64) (*Segment, error) {
	filename := fmt.Sprintf("%s/segment-%06d.wal", dir, id)
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment file: %w", err)
	}

	headerBytes := make([]byte, 28)
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}
	header, err := deserializeHeader(headerBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize header: %w", err)
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	return &Segment{
		file:    file,
		header:  header,
		size:    stat.Size() - 28,
		maxSize: 64 * 1024 * 1024,
	}, nil

}

func CloseSegment(s *Segment) error {
	return s.file.Close()
}
