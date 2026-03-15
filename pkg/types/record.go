package types

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
