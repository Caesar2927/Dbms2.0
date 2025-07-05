#pragma once

#include <string>

#include "LockManager.h"
#include "WALManager.h"
#include "TransactionManager.h"
#include "BufferManager.h"
#include "index_manager.h"

/// A thin CLI controller to drive one update?row transaction
class TransactionController {
public:
    /// Run the transaction workflow (BEGIN ? UPDATE ? COMMIT) for one table/row.
    static void run(
        BufferManager& bufMgr,

        LockManager& lockMgr,
        WALManager& walMgr,
        TransactionManager& txnMgr);
};
