#include "TransactionManager.h"
#include <iostream>

TransactionManager::TransactionManager(LockManager& lockMgr,
    WALManager& walMgr)
    : _lockMgr(lockMgr), _walMgr(walMgr)
{
}

TransactionID TransactionManager::beginTransaction() {
    TransactionID tid = _nextID++;
    _walMgr.logBegin(tid);
    std::cout << "[TXN " << tid << "] BEGIN\n";
    return tid;
}

void TransactionManager::commit(TransactionID txnID) {
    _walMgr.logCommit(txnID);
    _lockMgr.releaseAllLocks(txnID);
    std::cout << "[TXN " << txnID << "] COMMIT\n";
}

void TransactionManager::abort(TransactionID txnID) {
    _walMgr.logAbort(txnID);
    _lockMgr.releaseAllLocks(txnID);
    std::cout << "[TXN " << txnID << "] ABORT\n";
}
