# wal-engine

A Write-Ahead Log (WAL) engine in Go — a standalone, importable durability layer inspired by the internals of PostgreSQL, etcd, and RocksDB.

Built from scratch with full ARIES-based recovery, segment-based storage, checksumming, and checkpointing.

---

## What is a WAL?

A Write-Ahead Log guarantees that any change to data is recorded in a log **before** the actual data is touched. If the process crashes mid-transaction, the log is used on restart to figure out what was committed and what wasn't — and restore a consistent state automatically.

This is the same durability mechanism used inside PostgreSQL, RocksDB, etcd, and most production databases.

---

## Features

- **Durability** — committed transactions survive crashes, always
- **Atomicity** — transactions are all-or-nothing, no partial commits
- **ARIES Recovery** — Analysis → Redo → Undo on restart
- **Segment-based storage** — log split into multiple files, old segments deletable
- **Corruption detection** — every record has a CRC32 checksum, torn writes detected gracefully
- **Checkpointing** — periodic snapshots so recovery doesn't replay the entire log
- **Log compaction** — old segments truncated after checkpoints
- **Importable** — any Go project can use this as a package

---

## Installation

```bash
go get github.com/subhammahanty235/wal-proj
```

---

## Usage

```go
import "github.com/subhammahanty235/wal-proj/pkg/wal"

// Open (or create) a WAL
w, err := wal.Open(wal.Config{Dir: "/data/wal"})
if err != nil {
    log.Fatal(err)
}
defer w.Close()

// Begin a transaction
txnID, err := w.Begin()

// Log a change (pageID, oldData, newData)
w.Write(txnID, pageID, []byte("old value"), []byte("new value"))

// Commit — synced to disk, guaranteed durable
w.Commit(txnID)

// Or abort — changes will be rolled back on recovery
w.Abort(txnID)
```

### Recovery on restart

```go
w, _ := wal.Open(wal.Config{Dir: "/data/wal"})

w.Recover(wal.RecoveryCallbacks{
    Redo: func(pageID types.PageID, newData []byte) error {
        // apply newData to your storage
        return nil
    },
    Undo: func(pageID types.PageID, oldData []byte) error {
        // revert to oldData in your storage
        return nil
    },
})
```

The WAL tells you **what** to apply — you decide **how** to apply it to your pages.

---

## How it works

### Log record format

Every change is stored as a record on disk:

```
[ TotalSize | LSN | TxnID | Type | PageID | PrevLSN | OldDataLen | OldData | NewDataLen | NewData | CRC32 ]
```

- **LSN** (Log Sequence Number) — unique monotonically increasing ID for every record
- **PrevLSN** — links all records of a transaction into a chain (for fast undo traversal)
- **OldData / NewData** — redo uses NewData, undo uses OldData
- **CRC32** — detects corruption and torn writes from crashes

### Record types

| Type | Description |
|------|-------------|
| `BEGIN` | Transaction started |
| `WRITE` | Page change logged |
| `COMMIT` | Transaction committed |
| `ABORT` | Transaction aborted |
| `CLR` | Compensation Log Record — proof that an undo happened |
| `CHECKPOINT_BEGIN` | Checkpoint started |
| `CHECKPOINT_END` | Checkpoint complete, active transactions snapshot |

### Segment storage

The log is split into multiple segment files:

```
/data/wal/
  segment-000001.wal
  segment-000002.wal
  segment-000003.wal   ← current
```

Each segment has a fixed max size (default 64MB). When full, a new segment is created automatically. Old segments before the last checkpoint can be safely deleted via `Truncate()`.

### ARIES Recovery

On restart, recovery runs in three phases:

**1. Analysis** — scan the log, build two sets:
- committed transactions (need redo)
- uncommitted transactions (need undo)

**2. Redo** — replay every record in log order (committed + uncommitted both). Restores disk to the exact state before the crash.

**3. Undo** — reverse all uncommitted transactions in reverse LSN order. Writes a CLR (Compensation Log Record) after each undo to prevent double-undo if another crash happens during recovery.

### Checkpointing

Periodic checkpoints snapshot the list of active transactions so recovery doesn't have to replay the entire log from the beginning.

```go
w.WriteCheckpoint()
```

After a checkpoint, old segments can be truncated:

```go
w.Truncate()
```

---

## Package structure

```
wal-engine/
  cmd/
    main.go          → usage example
  pkg/
    types/           → Record, LSN, TxnID, Serialize/Deserialize, CRC32
    segment/         → segment files, read/write, rotation
    wal/             → core WAL logic, transactions, ARIES recovery
```

---

## What this does NOT do

- Does not manage actual data pages — that's the caller's job
- Does not implement a query engine, buffer pool, or storage engine
- Does not handle replication or distributed consensus

---

## License

MIT