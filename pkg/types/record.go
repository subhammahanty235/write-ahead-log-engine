package types

import (
	"bytes"
	"encoding/binary"
)

type LSN uint64
type TxnID uint64
type PageID uint64
type RecordType uint8

const (
	RecordBegin RecordType = iota
	RecordWrite
	RecordCommit
	RecordAbort
	RecordCLR
	RecordCheckpoint
	RecordCheckpointBegin
	RecordCheckpointEnd
)

type Record struct {
	LSN       LSN
	TxnID     TxnID
	Type      RecordType
	PageID    PageID
	OldData   []byte
	NewData   []byte
	PrevLSN   LSN
	Checksum  uint32
	TotalSize uint32
}

type CheckpointData struct {
	ActiveTxns []TxnID
}

func SerializeCheckpointData(cd CheckpointData) ([]byte, error) {
	buf := new(bytes.Buffer)
	// pehle count likho kitni txns hain
	count := uint32(len(cd.ActiveTxns))
	binary.Write(buf, binary.LittleEndian, count)
	// phir har txnID
	for _, txnID := range cd.ActiveTxns {
		binary.Write(buf, binary.LittleEndian, txnID)
	}
	return buf.Bytes(), nil
}

func DeserializeCheckpointData(data []byte) (CheckpointData, error) {
	buf := bytes.NewReader(data)
	var count uint32
	binary.Read(buf, binary.LittleEndian, &count)

	cd := CheckpointData{
		ActiveTxns: make([]TxnID, count),
	}
	for i := uint32(0); i < count; i++ {
		binary.Read(buf, binary.LittleEndian, &cd.ActiveTxns[i])
	}
	return cd, nil
}
