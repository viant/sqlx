# Batcher

Batcher service batches individual client insert calls, into centralized bulk insert.


```text
Client1: INSERT INTO X
Client2: INSERT INTO X     ->  Batch(by time or max elements) -> Execute BULK INSERT (updated autoincrement back to clients)

ClientN: INSERT INTO X
```

## Motivation

## Usage