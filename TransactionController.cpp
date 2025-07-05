// File: TransactionController.cpp

#include "TransactionController.h"
#include "record_manager.h"   // for Schema
#include "index_manager.h"    // for IndexManager
#include <iostream>
#include <fstream>
#include <limits>
#include <cstring>

using TransactionID = uint64_t;


void TransactionController::run(BufferManager& bufMgr, LockManager& lockMgr, WALManager& walMgr, TransactionManager& txnMgr)
{
    std::string table;
    std::cout << "Enter table name: ";
    std::cin >> table;
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    // 2) Load schema & unique keys
    std::string schemaLine, keysLine;
    {
        std::ifstream meta("Tables/" + table + "/meta.txt");
        if (!meta) {
            std::cerr << "[Transaction] Table not found: " << table << "\n";
            return;
        }
        std::getline(meta, schemaLine);
        std::getline(meta, keysLine);
    }
    Schema schema(schemaLine, keysLine);

    // 3) Prompt for the unique‐key field & value
    std::string field, value;
    std::cout << "Enter unique field name: ";
    std::cin >> field;
    std::cout << "Enter its value: ";
    std::cin >> value;

    // 4) Build & load the index for that table
    IndexManager idxMgr(table, "Tables/" + table, bufMgr);
    idxMgr.loadIndexes(schema.getUniqueKeys());

    // 5) Find the record’s offset via B+ tree
    long offset = idxMgr.searchIndex(field, value);
    if (offset < 0) {
        std::cerr << "[Transaction] Row not found for "
            << field << "=" << value << "\n";
        return;
    }

    // 6) Read the "before" image by pinning the page
    auto pageNum = static_cast<uint32_t>(offset / BufferManager::PAGE_SIZE);
    char* pageBuf = bufMgr.getPage(
        "Tables/" + table + "/data.tbl",
        pageNum,
        PageType::DATA
    );
    if (!pageBuf) {
        std::cerr << "[Transaction] Cannot pin page " << pageNum << "\n";
        return;
    }
    int recSize = schema.getRecordSize();
    size_t withinPage = offset % BufferManager::PAGE_SIZE;
    std::string beforeImage(pageBuf + withinPage, recSize);

    // 7) Prompt for new comma‐separated row image
    std::cout << "Enter new comma-separated values for all fields:\n> ";
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    std::string afterImage;
    std::getline(std::cin, afterImage);

    // 8) BEGIN transaction
    TransactionID tid = txnMgr.beginTransaction();

    // 9) Acquire an exclusive lock on this record
    std::string resource = table + ":row:" + std::to_string(offset);
    lockMgr.acquireLock(tid, resource, LockMode::EXCLUSIVE);
    
    // 10) Write the UPDATE record to WAL
    LogRecord rec;
    rec.txnID = tid;
    rec.type = LogType::UPDATE;
    rec.tableName = table;
    rec.offset = offset;
    rec.beforeImage = beforeImage;
    rec.afterImage = afterImage;
    std::cout << "send to walmanager to update" << std::endl;
    walMgr.logUpdate(rec);

    // 11) Apply the new image into the in-memory page buffer
    std::memcpy(
        pageBuf + withinPage,
        afterImage.data(),
        static_cast<size_t>(std::min<int>(afterImage.size(), recSize))
    );

    // 12) Unpin & mark dirty so BufferManager knows to flush later
    bufMgr.unpinPage(
        "Tables/" + table + "/data.tbl",
        pageNum,
        PageType::DATA,
        /* isDirty = */ true
    );

    // 13) COMMIT transaction
    txnMgr.commit(tid);
    std::cout << "[Transaction] T" << tid << " committed successfully.\n";
}
