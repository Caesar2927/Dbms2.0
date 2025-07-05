#pragma once

#include <cstdint>
#include <string>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <deque>

using TransactionID = uint64_t;

enum class LockMode { SHARED, EXCLUSIVE };

/// Standalone strict?2PL lock manager.
/// Locks are tracked per?resource (e.g. "table:users,page:3" or "users/row/42").
class LockManager {
public:
    /// Block until txnID can acquire `mode` on `resource`.
    void acquireLock(TransactionID txnID,
        const std::string& resource,
        LockMode mode);

    /// Release *all* locks held by txnID.
    void releaseAllLocks(TransactionID txnID);

private:
    struct LockInfo {
        std::mutex               mtx;
        std::condition_variable  cv;
        // Current holders: txnID ? mode
        std::unordered_map<TransactionID, LockMode> holders;
        // Waiting queue: front of queue is next in line
        std::deque<std::pair<TransactionID, LockMode>> waitQ;
    };

    // Protects access to _lockTable itself
    std::mutex                                    _tableMtx;
    std::unordered_map<std::string, LockInfo>     _lockTable;
};
