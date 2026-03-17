package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

func Serialize(r Record) ([]byte, error) {
	buf := new(bytes.Buffer)
	// bytes.Buffer writes pratically never fails, but as a production best practice we are adding error handing here.
	if err := binary.Write(buf, binary.LittleEndian, r.LSN); err != nil {
		return nil, fmt.Errorf("failed to write LSN: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, r.TxnID); err != nil {
		return nil, fmt.Errorf("failed to write TxnID: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, r.Type); err != nil {
		return nil, fmt.Errorf("failed to write Type: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, r.PageID); err != nil {
		return nil, fmt.Errorf("failed to write PageID: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, r.PrevLSN); err != nil {
		return nil, fmt.Errorf("failed to write PrevLSN: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(r.OldData))); err != nil {
		return nil, fmt.Errorf("failed to write OldData length: %w", err)
	}
	if _, err := buf.Write(r.OldData); err != nil {
		return nil, fmt.Errorf("failed to write OldData: %w", err)
	}
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(r.NewData))); err != nil {
		return nil, fmt.Errorf("failed to write NewData length: %w", err)
	}
	if _, err := buf.Write(r.NewData); err != nil {
		return nil, fmt.Errorf("failed to write NewData: %w", err)
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	if err := binary.Write(buf, binary.LittleEndian, checksum); err != nil {
		return nil, fmt.Errorf("failed to write checksum: %w", err)
	}
	finalBuf := new(bytes.Buffer)
	binary.Write(finalBuf, binary.LittleEndian, uint32(buf.Len())+4)
	finalBuf.Write(buf.Bytes())

	return finalBuf.Bytes(), nil

}

func DeSerialize(data []byte) (Record, error) {
	buf := bytes.NewReader(data)
	var totalSize uint32

	if err := binary.Read(buf, binary.LittleEndian, &totalSize); err != nil {
		return Record{}, fmt.Errorf("failed to read TotalSize: %w", err)
	}

	var lsn LSN
	if err := binary.Read(buf, binary.LittleEndian, &lsn); err != nil {
		return Record{}, fmt.Errorf("failed to read LSN: %w", err)
	}

	var txnID TxnID
	if err := binary.Read(buf, binary.LittleEndian, &txnID); err != nil {
		return Record{}, fmt.Errorf("failed to read TxnID: %w", err)
	}

	var rtype RecordType
	if err := binary.Read(buf, binary.LittleEndian, &rtype); err != nil {
		return Record{}, fmt.Errorf("failed to read RecordType: %w", err)
	}

	var pageID PageID
	if err := binary.Read(buf, binary.LittleEndian, &pageID); err != nil {
		return Record{}, fmt.Errorf("failed to read PageID: %w", err)
	}

	var prevLSN LSN
	if err := binary.Read(buf, binary.LittleEndian, &prevLSN); err != nil {
		return Record{}, fmt.Errorf("failed to read PrevLSN: %w", err)
	}

	var oldDataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &oldDataLen); err != nil {
		return Record{}, fmt.Errorf("failed to read OldData length: %w", err)
	}
	oldData := make([]byte, oldDataLen)
	if _, err := io.ReadFull(buf, oldData); err != nil {
		return Record{}, fmt.Errorf("failed to read OldData: %w", err)
	}

	var newDataLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &newDataLen); err != nil {
		return Record{}, fmt.Errorf("failed to read NewData length: %w", err)
	}
	newData := make([]byte, newDataLen)
	if _, err := io.ReadFull(buf, newData); err != nil {
		return Record{}, fmt.Errorf("failed to read NewData: %w", err)
	}

	var storedChecksum uint32
	if err := binary.Read(buf, binary.LittleEndian, &storedChecksum); err != nil {
		return Record{}, fmt.Errorf("failed to read checksum: %w", err)
	}

	computedChecksum := crc32.ChecksumIEEE(data[4 : len(data)-4])
	if computedChecksum != storedChecksum {
		return Record{}, fmt.Errorf("checksum mismatch: data corrupt ho sakta hai")
	}

	return Record{
		LSN:       lsn,
		TxnID:     txnID,
		Type:      rtype,
		PageID:    pageID,
		PrevLSN:   prevLSN,
		OldData:   oldData,
		NewData:   newData,
		TotalSize: totalSize,
	}, nil
}
