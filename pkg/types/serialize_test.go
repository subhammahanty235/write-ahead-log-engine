package types

import (
	"bytes"
	"testing"
)

func TestSerializeDeserialize(t *testing.T) {
	original := Record{
		LSN:     42,
		TxnID:   7,
		Type:    RecordType(1),
		PageID:  100,
		PrevLSN: 41,
		OldData: []byte("old data"),
		NewData: []byte("old data"),
	}

	serialized, err := Serialize(original)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	result, err := DeSerialize(serialized)
	if err != nil {
		t.Fatalf("DeSerialize failed: %v", err)
	}

	if original.LSN != result.LSN {
		t.Errorf("LSN mismatch: want %v, got %v", original.LSN, result.LSN)
	}
	if original.TxnID != result.TxnID {
		t.Errorf("TxnID mismatch: want %v, got %v", original.TxnID, result.TxnID)
	}
	if original.Type != result.Type {
		t.Errorf("Type mismatch: want %v, got %v", original.Type, result.Type)
	}
	if original.PageID != result.PageID {
		t.Errorf("PageID mismatch: want %v, got %v", original.PageID, result.PageID)
	}
	if original.PrevLSN != result.PrevLSN {
		t.Errorf("PrevLSN mismatch: want %v, got %v", original.PrevLSN, result.PrevLSN)
	}
	if !bytes.Equal(original.OldData, result.OldData) {
		t.Errorf("OldData mismatch: want %v, got %v", original.OldData, result.OldData)
	}
	if !bytes.Equal(original.NewData, result.NewData) {
		t.Errorf("NewData mismatch: want %v, got %v", original.NewData, result.NewData)
	}
}

func TestChecksumCorruption(t *testing.T) {
	original := Record{
		LSN:     10,
		TxnID:   3,
		Type:    RecordType(2),
		PageID:  55,
		PrevLSN: 9,
		OldData: []byte("old"),
		NewData: []byte("new"),
	}

	serialized, err := Serialize(original)
	if err != nil {
		t.Fatalf("Serialize failed: %v", err)
	}

	// Beech ka byte corrupt karo (checksum field se door)
	serialized[4] ^= 0xFF

	_, err = DeSerialize(serialized)
	if err == nil {
		t.Fatal("expected serialzation error, but not recieved")
	}
}
