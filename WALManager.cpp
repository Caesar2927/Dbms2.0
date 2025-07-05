#include "WALManager.h"
#include <sstream>
#include <fstream>
#include <iostream>
#include <cassert>

WALManager::WALManager(const std::string& path) {
    // Open in append + binary; we will flush on every record
    std::cout << "here to create wal file" << std::endl;
    _out.open(path, std::ios::app | std::ios::binary);
    if (!_out) {
        throw std::runtime_error("WALManager: cannot open " + path);
    }
    std::cout << "created file" << std::endl;
}

WALManager::~WALManager() {
    std::lock_guard<std::mutex> lg(_mtx);
    _out.flush();
    _out.close();
}

void WALManager::appendRecord(const LogRecord& rec) {
    std::cout << "send to update" << std::endl;
    std::lock_guard<std::mutex> lg(_mtx);

    // Format: txnID type tableName offset "before" "after"\n
    std::cout << "stp 2 " << std::endl;
    
    _out
        << rec.txnID << ' '
        << static_cast<int>(rec.type) << ' '
        << rec.tableName << ' '
        << rec.offset << ' '
        << std::quoted(rec.beforeImage) << ' '
        << std::quoted(rec.afterImage)
        << '\n';

    _out.flush();  // sync to disk
    std::cout << "flushed to disc " << std::endl;
}

void WALManager::logBegin(TransactionID txnID) {
    appendRecord({ txnID, LogType::BEGIN, "", 0, "", "" });
}

void WALManager::logUpdate(const LogRecord& rec) {
    appendRecord(rec);
}

void WALManager::logCommit(TransactionID txnID) {
    appendRecord({ txnID, LogType::COMMIT, "", 0, "", "" });
}

void WALManager::logAbort(TransactionID txnID) {
    appendRecord({ txnID, LogType::ABORT, "", 0, "", "" });
}

void WALManager::recover() {
    // Open for reading
    std::ifstream in("wal.log", std::ios::binary);
    if (!in) {
        std::cerr << "WALManager::recover: no log file\n";
        return;
    }

    
    std::string line;
    while (std::getline(in, line)) {
        std::cout << "[WAL] " << line << "\n";
    }
}
