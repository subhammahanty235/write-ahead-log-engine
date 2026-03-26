package main

import (
	"fmt"
	"log"

	"github.com/subhammahanty235/write-ahead-log-engine/pkg/types"
	"github.com/subhammahanty235/write-ahead-log-engine/pkg/wal"
)

func main() {
	w, err := wal.Open(wal.Config{Dir: "/tmp/wal-test"})
	if err != nil {
		log.Fatal(err)
	}

	txnID, err := w.Begin()
	if err != nil {
		log.Fatal(err)
	}
	w.Write(txnID, types.PageID(1), []byte("old"), []byte("new"))
	w.Commit(txnID)
	fmt.Println("Transaction committed")
	txnID2, _ := w.Begin()
	w.Write(txnID2, types.PageID(2), []byte("original"), []byte("changed"))
	w.Recover(wal.RecoveryCallbacks{
		Redo: func(pageID types.PageID, newData []byte) error {
			fmt.Printf("REDO page %d → %s\n", pageID, newData)
			return nil
		},
		Undo: func(pageID types.PageID, oldData []byte) error {
			fmt.Printf("UNDO page %d → %s\n", pageID, oldData)
			return nil
		},
	})
}
