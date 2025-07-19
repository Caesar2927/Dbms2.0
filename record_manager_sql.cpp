// File: record_manager_sqlin.cpp
#include "record_manager_sql.h"
#include "record_manager.h"
#include "schema.h"
#include "index_manager.h"
#include "free_space_manager.h"

#include <optional>

/// Helper to read a single row at byte‐offset off.
static std::optional<Row> fetchRowAtOffset(
    const std::string& tableName,
    const std::vector<Schema::Field>& fields,
    long offset
) {
    const int PAGE_SIZE = 4096;
    int recordSize = 1; // isValid flag
    for (auto& f : fields) recordSize += f.length;

    // Pin page
    uint32_t pageId = offset / PAGE_SIZE;
    char* buf = RecordManager::bufMgr->getPage(
        "Tables/" + tableName + "/data.tbl",
        pageId, PageType::DATA
    );
    if (!buf) return std::nullopt;

    int slotIdx = (offset % PAGE_SIZE) / recordSize;
    char valid = buf[slotIdx * recordSize];
    if (!valid) {
        RecordManager::bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl", pageId,
            PageType::DATA, false
        );
        return std::nullopt;
    }

    Row row; row.reserve(fields.size());
    int base = slotIdx * recordSize + 1;
    for (auto& f : fields) {
        std::string val(buf + base, strnlen(buf + base, f.length));
        row.push_back(std::move(val));
        base += f.length;
    }

    RecordManager::bufMgr->unpinPage(
        "Tables/" + tableName + "/data.tbl", pageId,
        PageType::DATA, false
    );
    return row;
}

long RecordManagerSQL::insertRecord(
    const std::string& tableName,
    const std::vector<std::string>& data
) {
    // Load schema
    std::ifstream meta("Tables/" + tableName + "/meta.txt");
    if (!meta) return -1;
    std::string schemaStr, keysStr;
    std::getline(meta, schemaStr);
    std::getline(meta, keysStr);
    Schema schema(schemaStr, keysStr);
    auto fields = schema.getFields();
    auto uniqueKeys = schema.getUniqueKeys();

    // Duplicate‑key check
    IndexManager idx(tableName, "Tables/" + tableName, *RecordManager::bufMgr);
    idx.loadIndexes(uniqueKeys);
    for (size_t i = 0; i < fields.size(); ++i) {
        if (std::find(uniqueKeys.begin(), uniqueKeys.end(), fields[i].name)
            != uniqueKeys.end()
            ) {
            if (idx.existsInIndex(fields[i].name, data[i])) {
                return -1;
            }
        }
    }

    // Free space + pin page
    int payload = 0; for (auto& f : fields) payload += f.length;
    FreeSpaceManager fsm(tableName, payload, *RecordManager::bufMgr);
    fsm.load();
    uint32_t pageId = fsm.getPageWithFreeSlot();

    const int PAGE_SIZE = 4096;
    int slotWidth = 1 + payload;
    char* buf = RecordManager::bufMgr->getPage(
        "Tables/" + tableName + "/data.tbl",
        pageId, PageType::DATA
    );
    if (!buf) return -1;

    // find slot
    int slotIdx = -1, slotsPerPage = PAGE_SIZE / slotWidth;
    for (int i = 0;i < slotsPerPage;++i) {
        if (buf[i * slotWidth] == 0) { slotIdx = i; break; }
    }
    if (slotIdx < 0) {
        RecordManager::bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl", pageId,
            PageType::DATA, false
        );
        return -1;
    }

    long offset = pageId * PAGE_SIZE + slotIdx * slotWidth;
    // write
    buf[slotIdx * slotWidth] = 1;
    int off = slotIdx * slotWidth + 1;
    for (size_t i = 0;i < fields.size();++i) {
        std::memset(buf + off, 0, fields[i].length);
        std::memcpy(buf + off, data[i].c_str(),
            std::min(data[i].size(), (size_t)fields[i].length));
        off += fields[i].length;
    }

    RecordManager::bufMgr->unpinPage(
        "Tables/" + tableName + "/data.tbl", pageId,
        PageType::DATA, true
    );
    fsm.markSlotUsed(pageId);

    // update indexes
    for (size_t i = 0;i < fields.size();++i) {
        if (std::find(uniqueKeys.begin(), uniqueKeys.end(), fields[i].name)
            != uniqueKeys.end()
            ) {
            idx.insertIntoIndex(fields[i].name, data[i], offset);
        }
    }
    return offset;
}

std::optional<Row> RecordManagerSQL::findRecord(
    const std::string& tableName,
    const std::string& fieldName,
    const std::string& value
) {
    // load schema
    std::ifstream meta("Tables/" + tableName + "/meta.txt");
    if (!meta) return std::nullopt;
    std::string schemaStr, keysStr;
    std::getline(meta, schemaStr);
    std::getline(meta, keysStr);
    Schema schema(schemaStr, keysStr);
    auto fields = schema.getFields();
    auto uniqueKeys = schema.getUniqueKeys();

    // index lookup
    IndexManager idx(tableName, "Tables/" + tableName, *RecordManager::bufMgr);
    idx.loadIndexes(uniqueKeys);
    long offset = idx.searchIndex(fieldName, value);
    if (offset < 0) return std::nullopt;

    return fetchRowAtOffset(tableName, fields, offset);
}

DMLResult RecordManagerSQL::deleteRecord(
    const std::string& tableName,
    const std::string& fieldName,
    const std::string& value
) {
    // locate
    auto rec = findRecord(tableName, fieldName, value);
    if (!rec) return DMLResult::NotFound;

    // remove from index
    IndexManager idx(tableName, "Tables/" + tableName, *RecordManager::bufMgr);
    idx.loadIndexes({ fieldName });
    idx.removeFromIndex(fieldName, value);

    // mark invalid
    long offset = idx.searchIndex(fieldName, value); // old offset
    const int PAGE_SIZE = 4096;
    auto schemaRow = rec.value();
    int payload = 0; for (auto& s : schemaRow) payload += s.size();
    int slotWidth = 1 + payload;
    uint32_t pageId = offset / PAGE_SIZE;

    char* buf = RecordManager::bufMgr->getPage(
        "Tables/" + tableName + "/data.tbl",
        pageId, PageType::DATA
    );
    if (!buf) return DMLResult::Error;
    int slotIdx = (offset % PAGE_SIZE) / slotWidth;
    buf[slotIdx * slotWidth] = 0;
    RecordManager::bufMgr->unpinPage(
        "Tables/" + tableName + "/data.tbl", pageId,
        PageType::DATA, true
    );

    // free‐space
    FreeSpaceManager fsm(tableName, payload, *RecordManager::bufMgr);
    fsm.load();
    fsm.markSlotFree(pageId);
    return DMLResult::Deleted;
}

Rows RecordManagerSQL::scanAll(const std::string& tableName) {
    Rows out;
    // load schema
    std::ifstream meta("Tables/" + tableName + "/meta.txt");
    if (!meta) return out;
    std::string s1, s2; std::getline(meta, s1); std::getline(meta, s2);
    Schema schema(s1, s2);
    auto fields = schema.getFields();

    // file size
    std::ifstream f("Tables/" + tableName + "/data.tbl", std::ios::binary | std::ios::ate);
    long fileSize = f.tellg(); f.close();
    const int PAGE_SIZE = 4096;
    size_t totalPages = (fileSize + PAGE_SIZE - 1) / PAGE_SIZE;
    int recordSize = 1; for (auto& fld : fields) recordSize += fld.length;

    for (size_t pid = 0;pid < totalPages;++pid) {
        char* buf = RecordManager::bufMgr->getPage(
            "Tables/" + tableName + "/data.tbl",
            pid, PageType::DATA
        );
        if (!buf) continue;
        int slots = PAGE_SIZE / recordSize;
        for (int s = 0;s < slots;++s) {
            if (buf[s * recordSize] == 0) continue;
            long offset = pid * PAGE_SIZE + s * recordSize;
            auto row = fetchRowAtOffset(tableName, fields, offset);
            if (row) out.push_back(*row);
        }
        RecordManager::bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl",
            pid, PageType::DATA, false
        );
    }
    return out;
}

Rows RecordManagerSQL::scanGreaterEqual(
    const std::string& tableName,
    const std::string& fieldName,
    const std::string& value
) {
    Rows out;
    // load schema
    std::ifstream meta("Tables/" + tableName + "/meta.txt");
    if (!meta) return out;
    std::string s1, s2; std::getline(meta, s1); std::getline(meta, s2);
    Schema schema(s1, s2);
    auto fields = schema.getFields();
    auto ukeys = schema.getUniqueKeys();

    IndexManager idx(tableName, "Tables/" + tableName, *RecordManager::bufMgr);
    idx.loadIndexes(ukeys);
    auto offsets = idx.searchGreaterEqual(fieldName, value);
    for (auto off : offsets) {
        auto r = fetchRowAtOffset(tableName, fields, off);
        if (r) out.push_back(*r);
    }
    return out;
}

Rows RecordManagerSQL::scanLessEqual(
    const std::string& tableName,
    const std::string& fieldName,
    const std::string& value
) {
    Rows out;
    // load schema
    std::ifstream meta("Tables/" + tableName + "/meta.txt");
    if (!meta) return out;
    std::string s1, s2; std::getline(meta, s1); std::getline(meta, s2);
    Schema schema(s1, s2);
    auto fields = schema.getFields();
    auto ukeys = schema.getUniqueKeys();

    IndexManager idx(tableName, "Tables/" + tableName, *RecordManager::bufMgr);
    idx.loadIndexes(ukeys);
    auto offsets = idx.searchLessEqual(fieldName, value);
    for (auto off : offsets) {
        auto r = fetchRowAtOffset(tableName, fields, off);
        if (r) out.push_back(*r);
    }
    return out;
}

Rows RecordManagerSQL::scanBetween(
    const std::string& tableName,
    const std::string& fieldName,
    const std::string& low,
    const std::string& high
) {
    Rows out;
    // load schema
    std::ifstream meta("Tables/" + tableName + "/meta.txt");
    if (!meta) return out;
    std::string s1, s2; std::getline(meta, s1); std::getline(meta, s2);
    Schema schema(s1, s2);
    auto fields = schema.getFields();
    auto ukeys = schema.getUniqueKeys();

    IndexManager idx(tableName, "Tables/" + tableName, *RecordManager::bufMgr);
    idx.loadIndexes(ukeys);
    auto offsets = idx.searchBetween(fieldName, low, high);
    for (auto off : offsets) {
        auto r = fetchRowAtOffset(tableName, fields, off);
        if (r) out.push_back(*r);
    }
    return out;
}
