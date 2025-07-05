#include "LockManager.h"
#include <algorithm>

void LockManager::acquireLock(TransactionID txnID,
    const std::string& resource,
    LockMode mode)
{
    // 1) Get or create the LockInfo for this resource
    LockInfo* info;
    {
        std::lock_guard<std::mutex> lg(_tableMtx);
        info = &_lockTable[resource]; // default-constructs if absent
    }

    // 2) Enqueue request and wait
    {
        std::unique_lock<std::mutex> lg(info->mtx);
        info->waitQ.emplace_back(txnID, mode);

        info->cv.wait(lg, [&]() {
            // Must be at front of queue
            if (info->waitQ.front().first != txnID) return false;

            // Check for conflicts with existing holders
            for (auto& [holderTid, holderMode] : info->holders) {
                if (holderTid == txnID) continue; // reentrant
                // Shared+Shared is OK; anything else conflicts
                if (!(mode == LockMode::SHARED && holderMode == LockMode::SHARED))
                    return false;
            }
            return true;
            });

        // Granted: dequeue and record in holders
        info->waitQ.pop_front();
        info->holders[txnID] = mode;
    }
}

void LockManager::releaseAllLocks(TransactionID txnID) {
    std::lock_guard<std::mutex> lgTable(_tableMtx);
    for (auto& [resource, info] : _lockTable) {
        {
            std::lock_guard<std::mutex> lg(info.mtx);
            // Remove from holders
            info.holders.erase(txnID);
            // Remove any pending requests by this txn
            auto& Q = info.waitQ;
            Q.erase(std::remove_if(Q.begin(), Q.end(),
                [&](auto& p) { return p.first == txnID; }),
                Q.end());
        }
        // Wake up any waiters
        info.cv.notify_all();
    }
}
