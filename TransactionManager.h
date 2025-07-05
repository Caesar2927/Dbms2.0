#pragma once

#include <cstdint>
#include "LockManager.h"
#include "WALManager.h"

using TransactionID = uint64_t;

/// Coordinates Strict 2PL locking and WAL for each transaction.
class TransactionManager {
public:
    TransactionManager(LockManager& lockMgr, WALManager& walMgr);

    /// Begin a new transaction; returns a unique txnID.
    TransactionID beginTransaction();

    /// Commit the transaction: write COMMIT record + release locks.
    void commit(TransactionID txnID);

    /// Abort the transaction: write ABORT record + release locks.
    void abort(TransactionID txnID);

private:
    LockManager& _lockMgr;
    WALManager& _walMgr;
    TransactionID _nextID = 1;
};
