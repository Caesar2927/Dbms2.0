#pragma once

#include <cstdint>
#include <string>
#include <mutex>
#include <fstream>
#include <iomanip>

using TransactionID = uint64_t;

enum class LogType : int { BEGIN = 0, UPDATE = 1, COMMIT = 2, ABORT = 3 };

struct LogRecord {
    TransactionID txnID;
    LogType       type;
    std::string   tableName;    // empty for BEGIN/COMMIT/ABORT
    long          offset;       // byte offset in data file
    std::string   beforeImage;  // quoted
    std::string   afterImage;   // quoted
};

/// Synchronous write‑ahead log manager (flushes on every append).
class WALManager {
public:
    WALManager(const std::string& path = "wal.log");
    ~WALManager();

    void logBegin(TransactionID txnID);
    void logUpdate(const LogRecord& rec);
    void logCommit(TransactionID txnID);
    void logAbort(TransactionID txnID);

    /// Perform recovery (Analysis → Redo → Undo).
    void recover();

private:
    std::ofstream _out;
    std::mutex    _mtx;

    void appendRecord(const LogRecord& rec);
};
