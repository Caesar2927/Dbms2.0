#include "record_manager.h"

#include <fstream>
#include <iostream>
#include <filesystem>
#include <vector>
#include <cstring>
#include <algorithm>

namespace fs = std::filesystem;

// Initialize static member

BufferManager* RecordManager::bufMgr = nullptr;
void RecordManager::addRecord(const std::string& tableName) {
    // 1) Load schema
  
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) {
        std::cerr << "[addRecord] No such table: " << tableName << "\n";
        return;
    }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();
    const auto& uniqueKeys = schema.getUniqueKeys();


    // 2) Read user data into a vector<string>
    std::vector<std::string> data(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        const auto& f = fields[i];
        std::cout << "Enter " << f.name << " (" << f.type << "): ";
        std::cin >> data[i];
        if (f.type == "int") {
            try {
                std::stoi(data[i]);
            }
            catch (...) {
                std::cerr << "[addRecord] Invalid integer for " << f.name << "\n";
                return;
            }
        }
    }


    // 3) Duplicate-key check via IndexManager
    IndexManager idxMgr(tableName, "Tables/" + tableName, *bufMgr);
    idxMgr.loadIndexes(uniqueKeys);
    for (size_t i = 0; i < fields.size(); ++i) {
        if (std::find(uniqueKeys.begin(), uniqueKeys.end(), fields[i].name)
            != uniqueKeys.end())
        {
            if (idxMgr.existsInIndex(fields[i].name, data[i])) {
                std::cerr << "[addRecord] Duplicate key on " << fields[i].name << "\n";
                return;
            }
        }
    }

    // 4) Compute payloadSize and slotWidth
    const int PAGE_SIZE = 4096;
    int payloadSize = 0;
    for (auto& f : fields) {
        payloadSize += f.length;
    }
    int slotWidth = 1 + payloadSize;  // 1 byte for isValid flag

    // 5) Free-space manager (will read/write free_space.meta via buffer)
    FreeSpaceManager fsm("Tables/" + tableName, payloadSize, *bufMgr);
    fsm.load();
    uint32_t pageId = fsm.getPageWithFreeSlot();

    // 6) Pin the target data page via BufferManager
    //    If the page does not yet exist on disk, BufferManager loads zero‐filled.

    char* pageBuf = bufMgr->getPage(
        "Tables/" + tableName + "/data.tbl",
        pageId,
        PageType::DATA
    );
    if (!pageBuf) {
        std::cerr << "[addRecord] Cannot pin data page " << pageId << "\n";
        return;
    }
    

    // 6a) If this page was just created, ensure it is zero‐filled
    //     BufferManager’s getPage already zero‐fills missing pages on disk.

    // 7) Find first free slot in that page’s buffer
    int slotsPerPage = PAGE_SIZE / slotWidth;
    int slotIdx = -1;
    for (int i = 0; i < slotsPerPage; ++i) {
        char flag = pageBuf[i * slotWidth];
        if (flag == 0) {
            slotIdx = i;
            break;
        }
    }
    if (slotIdx < 0) {
        std::cerr << "[addRecord] FSM inconsistency: no free slot on page "
            << pageId << "\n";
        bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl",
            pageId,
            PageType::DATA,
            /*isDirty=*/false
        );
        return;
    }
    std::cout << "wrote record on rought" << std::endl;
    // 8) Write record into the page buffer
    long offset = static_cast<long>(pageId) * PAGE_SIZE + slotIdx * slotWidth;
    // 8a) Set isValid = 1
    pageBuf[slotIdx * slotWidth] = 1;

    // 8b) Copy each field’s fixed‐length bytes
    for (size_t i = 0; i < fields.size(); ++i) {
        const auto& f = fields[i];
        const auto& val = data[i];
        // Destination address within pageBuf:
        char* dest = pageBuf + slotIdx * slotWidth + 1  // skip valid flag
            + static_cast<int>(i) * f.length;
        // Zero out f.length bytes
        std::memset(dest, 0, f.length);
        // Copy up to f.length
        std::memcpy(dest, val.c_str(), std::min<size_t>(val.size(), f.length));
    }

    // 9) Unpin page, marking dirty => will be flushed later
    bufMgr->unpinPage(
        "Tables/" + tableName + "/data.tbl",
        pageId,
        PageType::DATA,
        /*isDirty=*/true
    );

    // 10) Update free‐space metadata (mark slot used) – this itself calls save() via buffer
    fsm.markSlotUsed(pageId);

    // 11) Insert into each unique‐key index
    for (size_t i = 0; i < fields.size(); ++i) {
        if (std::find(uniqueKeys.begin(), uniqueKeys.end(), fields[i].name)
            != uniqueKeys.end())
        {
            idxMgr.insertIntoIndex(fields[i].name, data[i], offset);
        }
    }
    // No need to call idxMgr.saveIndexes(); writes happen as you insert

    std::cout << "[addRecord] Record added successfully at offset " << offset << "\n";
}

void RecordManager::findRecord(const std::string& tableName) {
    // 1) Load schema & unique keys
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) {
        std::cout << "[findRecord] Table not found: " << tableName << "\n";
        return;
    }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();
    const auto& uniqueKeys = schema.getUniqueKeys();

    // 2) Get user query "field=value"
    std::cout << "Enter query (field=value): ";
    std::string input;
    std::getline(std::cin, input);
    auto eq = input.find('=');
    if (eq == std::string::npos) {
        std::cout << "[findRecord] Invalid format\n";
        return;
    }
    std::string field = input.substr(0, eq);
    std::string value = input.substr(eq + 1);
    auto trim = [](std::string& s) {
        s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
        };
    trim(field);
    trim(value);

    // 3) Find field index & see if it’s unique
    int idx = -1;
    bool isUnique = false;
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i].name == field) {
            idx = static_cast<int>(i);
            isUnique = (std::find(uniqueKeys.begin(), uniqueKeys.end(), field)
                != uniqueKeys.end());
            break;
        }
    }
    if (idx < 0) {
        std::cout << "[findRecord] Field not in schema: " << field << "\n";
        return;
    }

    // 4) Prepare IndexManager
    IndexManager idxMgr(tableName, "Tables/" + tableName, *bufMgr);
    idxMgr.loadIndexes(uniqueKeys);

    if (isUnique) {
        // 5a) Use B+ Tree to find exact offset
        long off = idxMgr.searchIndex(field, value);
        if (off < 0) {
            std::cout << "[findRecord] No matching record.\n";
            return;
        }
        // 5b) Pin the page via buffer
        const int PAGE_SIZE = 4096;
        long pageId = off / PAGE_SIZE;
        char* pageBuf = bufMgr->getPage(
            "Tables/" + tableName + "/data.tbl",
            static_cast<uint32_t>(pageId),
            PageType::DATA
        );
        if (!pageBuf) {
            std::cerr << "[findRecord] Cannot pin data page " << pageId << "\n";
            return;
        }
        int slotWidth = 1;
        for (auto& f : fields) slotWidth += f.length;
        int slotIdx = static_cast<int>((off % PAGE_SIZE) / slotWidth);

        // 5c) Check valid flag
        char valid = pageBuf[slotIdx * slotWidth];
        if (valid == 0) {
            std::cout << "[findRecord] Record was deleted.\n";
            bufMgr->unpinPage(
                "Tables/" + tableName + "/data.tbl",
                static_cast<uint32_t>(pageId),
                PageType::DATA,
                /*isDirty=*/false
            );
            return;
        }
        // 5d) Print fields
        std::cout << "[findRecord] Found at offset " << off << ": ";
        int base = slotIdx * slotWidth + 1; // skip valid flag
        for (size_t i = 0; i < fields.size(); ++i) {
            const auto& f = fields[i];
            std::string val(pageBuf + base + i * f.length,
                strnlen(pageBuf + base + i * f.length, f.length));
            std::cout << f.name << ": " << val << "  ";
        }
        std::cout << "\n";

        bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl",
            static_cast<uint32_t>(pageId),
            PageType::DATA,
            /*isDirty=*/false
        );
    }
    else {
        // 5e) Linear scan all pages
        std::cout << "[findRecord] Scanning all records...\n";
        const int PAGE_SIZE = 4096;
        // Pin page 0 to find how many pages exist on disk:
        uint32_t pageNo = 0;
        // Instead of fstream, ask bufferMgr about file size:
        std::ifstream dataProbe("Tables/" + tableName + "/data.tbl", std::ios::binary | std::ios::ate);
        if (!dataProbe) {
            std::cout << "[findRecord] Data file missing.\n";
            return;
        }
        long fileSize = dataProbe.tellg();
        dataProbe.close();
        size_t totalPages = static_cast<size_t>((fileSize + PAGE_SIZE - 1) / PAGE_SIZE);
        int slotWidth = 1;
        for (auto& f : fields) slotWidth += f.length;

        for (size_t pid = 0; pid < totalPages; ++pid) {
            char* pageBuf = bufMgr->getPage(
                "Tables/" + tableName + "/data.tbl",
                static_cast<uint32_t>(pid),
                PageType::DATA
            );
            if (!pageBuf) {
                std::cerr << "[findRecord] Cannot pin data page " << pid << "\n";
                continue;
            }
            int slotsPerPage = PAGE_SIZE / slotWidth;
            for (int s = 0; s < slotsPerPage; ++s) {
                char valid = pageBuf[s * slotWidth];
                if (valid == 0) continue;
                // Read field at idx
                std::string val(pageBuf + s * slotWidth + 1 + idx * fields[idx].length,
                    strnlen(pageBuf + s * slotWidth + 1 + idx * fields[idx].length,
                        fields[idx].length));
                if (val == value) {
                    // Print entire record
                    std::cout << "[Page " << pid << " | Slot " << s << "] ";
                    for (size_t i = 0; i < fields.size(); ++i) {
                        const auto& f = fields[i];
                        std::string v(pageBuf + s * slotWidth + 1 + i * f.length,
                            strnlen(pageBuf + s * slotWidth + 1 + i * f.length,
                                f.length));
                        std::cout << f.name << ": " << v << "  ";
                    }
                    std::cout << "\n";
                }
            }
            bufMgr->unpinPage(
                "Tables/" + tableName + "/data.tbl",
                static_cast<uint32_t>(pid),
                PageType::DATA,
                /*isDirty=*/false
            );
        }
    }
}

void RecordManager::deleteRecord(const std::string& tableName) {
    // 1) Load schema & unique keys
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) {
        std::cerr << "[deleteRecord] Table not found: " << tableName << "\n";
        return;
    }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();
    const auto& uniqueKeys = schema.getUniqueKeys();

    // 2) Parse user input "field=value"
    std::cout << "Enter delete query (field=value): ";
    std::string input;
    std::getline(std::cin, input);
    auto eq = input.find('=');
    if (eq == std::string::npos) {
        std::cerr << "[deleteRecord] Invalid format. Use field=value\n";
        return;
    }
    std::string field = input.substr(0, eq);
    std::string value = input.substr(eq + 1);
    auto trim = [](std::string& s) {
        s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
        };
    trim(field);
    trim(value);

    // 3) Ensure field is unique
    if (std::find(uniqueKeys.begin(), uniqueKeys.end(), field) == uniqueKeys.end()) {
        std::cerr << "[deleteRecord] Deletion requires a unique field.\n";
        return;
    }

    // 4) Locate record offset via IndexManager
    IndexManager idxMgr(tableName, "Tables/" + tableName, *bufMgr);
    idxMgr.loadIndexes(uniqueKeys);
    long offset = idxMgr.searchIndex(field, value);
    if (offset < 0) {
        std::cout << "[deleteRecord] Record not found.\n";
        return;
    }

    // 5) Remove from index
    idxMgr.removeFromIndex(field, value);
    // (flush happens when buffer evicts if needed)

    // 6) Pin the data page, check validity
    const int PAGE_SIZE = 4096;
    long pageId = offset / PAGE_SIZE;
    char* pageBuf = bufMgr->getPage(
        "Tables/" + tableName + "/data.tbl",
        static_cast<uint32_t>(pageId),
        PageType::DATA
    );
    if (!pageBuf) {
        std::cerr << "[deleteRecord] Cannot pin data page " << pageId << "\n";
        return;
    }
    int slotWidth = 1;
    for (auto& f : fields) slotWidth += f.length;
    int slotIdx = static_cast<int>((offset % PAGE_SIZE) / slotWidth);

    char valid = pageBuf[slotIdx * slotWidth];
    if (valid == 0) {
        std::cout << "[deleteRecord] Record already deleted.\n";
        bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl",
            static_cast<uint32_t>(pageId),
            PageType::DATA,
            /*isDirty=*/false
        );
        return;
    }

    // 7) Mark isValid → 0 in buffer
    pageBuf[slotIdx * slotWidth] = 0;
    bufMgr->unpinPage(
        "Tables/" + tableName + "/data.tbl",
        static_cast<uint32_t>(pageId),
        PageType::DATA,
        /*isDirty=*/true
    );

    // 8) Update free-space metadata (mark slot free)
    int recordSize = 1;
    for (auto& f : fields) recordSize += f.length;
    FreeSpaceManager fsm("Tables/" + tableName, recordSize, *bufMgr);
    fsm.load();
    fsm.markSlotFree(static_cast<uint32_t>(pageId));

    std::cout << "[deleteRecord] Record deleted successfully.\n";
}

void RecordManager::printAllRecords(const std::string& tableName) {
    // 1) Load schema
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) {
        std::cerr << "[printAllRecords] Table not found: " << tableName << "\n";
        return;
    }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();

    // 2) Compute payload and slot widths
    const int PAGE_SIZE = 4096;
    int payloadSize = 0;
    for (auto& f : fields) payloadSize += f.length;
    int slotWidth = 1 + payloadSize;

    // 3) Find how many pages exist on disk
    std::ifstream dataProbe("Tables/" + tableName + "/data.tbl",
        std::ios::binary | std::ios::ate);
    if (!dataProbe) {
        std::cerr << "[printAllRecords] Cannot open data.tbl\n";
        return;
    }
    long fileSize = dataProbe.tellg();
    dataProbe.close();
    size_t totalPages = static_cast<size_t>((fileSize + PAGE_SIZE - 1) / PAGE_SIZE);

    // 4) For each page, pin via buffer and iterate slots
    for (size_t pid = 0; pid < totalPages; ++pid) {
        char* pageBuf = bufMgr->getPage(
            "Tables/" + tableName + "/data.tbl",
            static_cast<uint32_t>(pid),
            PageType::DATA
        );
        if (!pageBuf) {
            std::cerr << "[printAllRecords] Cannot pin data page " << pid << "\n";
            continue;
        }

        int slotsPerPage = PAGE_SIZE / slotWidth;
        for (int s = 0; s < slotsPerPage; ++s) {
            char valid = pageBuf[s * slotWidth];
            if (valid == 0) continue;

            std::cout << "[Page " << pid << " | Slot " << s << "] ";
            int base = s * slotWidth + 1;
            for (size_t i = 0; i < fields.size(); ++i) {
                const auto& f = fields[i];
                std::string val(pageBuf + base + i * f.length,
                    strnlen(pageBuf + base + i * f.length, f.length));
                std::cout << f.name << ": " << val << "  ";
            }
            std::cout << "\n";
        }

        bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl",
            static_cast<uint32_t>(pid),
            PageType::DATA,
            /*isDirty=*/false
        );
    }
}

 void RecordManager::printRecordAtOffset(const std::string& tableName,
    const std::vector<Schema::Field>& fields,
    long offset)
{
    const int PAGE_SIZE = 4096;
    long pageId = offset / PAGE_SIZE;
    int recordSize = 1;
    for (auto& f : fields) recordSize += f.length;
    int slotWidth = recordSize;
    int slotIdx = static_cast<int>((offset % PAGE_SIZE) / slotWidth);

    char* pageBuf = bufMgr->getPage(
        "Tables/" + tableName + "/data.tbl",
        static_cast<uint32_t>(pageId),
        PageType::DATA
    );
    if (!pageBuf) return;

    char valid = pageBuf[slotIdx * slotWidth];
    if (!valid) {
        bufMgr->unpinPage(
            "Tables/" + tableName + "/data.tbl",
            static_cast<uint32_t>(pageId),
            PageType::DATA,
            /*isDirty=*/false
        );
        return;
    }

    int base = slotIdx * slotWidth + 1;
    for (size_t i = 0; i < fields.size(); ++i) {
        const auto& f = fields[i];
        std::string val(pageBuf + base + i * f.length,
            strnlen(pageBuf + base + i * f.length, f.length));
        std::cout << f.name << ": " << val << "  ";
    }
    std::cout << "\n";

    bufMgr->unpinPage(
        "Tables/" + tableName + "/data.tbl",
        static_cast<uint32_t>(pageId),
        PageType::DATA,
        /*isDirty=*/false
    );
}

void RecordManager::getGreaterEqual(const std::string& tableName) {
    // 1) Load schema & unique keys
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) { std::cerr << "[getGreaterEqual] Table not found\n"; return; }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();
    const auto& uniqueKeys = schema.getUniqueKeys();

    // 2) Get user input "field=value"
    std::cout << "Enter field>=value (e.g. id=123): ";
    std::string input;
    std::getline(std::cin, input);
    auto eq = input.find('=');
    if (eq == std::string::npos) { std::cerr << "[getGreaterEqual] Bad format\n"; return; }
    std::string field = input.substr(0, eq);
    std::string value = input.substr(eq + 1);
    auto trim = [](std::string& s) {
        s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
        };
    trim(field); trim(value);

    // 3) Check field is unique
    if (std::find(uniqueKeys.begin(), uniqueKeys.end(), field) == uniqueKeys.end()) {
        std::cerr << "[getGreaterEqual] Field is not indexed (must be unique)\n";
        return;
    }

    // 4) Use IndexManager to get offsets
    IndexManager idxMgr(tableName, "Tables/" + tableName, *bufMgr);
    idxMgr.loadIndexes(uniqueKeys);
    std::vector<long> offsets = idxMgr.searchGreaterEqual(field, value);
    if (offsets.empty()) {
        std::cout << "[getGreaterEqual] No matching records\n";
        return;
    }

    // 5) Print each record
    for (long off : offsets) {
        printRecordAtOffset(tableName, fields, off);
    }
}

void RecordManager::getLessEqual(const std::string& tableName) {
    // 1) Load schema & unique keys
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) { std::cerr << "[getLessEqual] Table not found\n"; return; }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();
    const auto& uniqueKeys = schema.getUniqueKeys();

    // 2) Get user input "field=value"
    std::cout << "Enter field<=value (e.g. id=456): ";
    std::string input;
    std::getline(std::cin, input);
    auto eq = input.find('=');
    if (eq == std::string::npos) { std::cerr << "[getLessEqual] Bad format\n"; return; }
    std::string field = input.substr(0, eq);
    std::string value = input.substr(eq + 1);
    auto trim = [](std::string& s) {
        s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
        };
    trim(field); trim(value);

    // 3) Check field is unique
    if (std::find(uniqueKeys.begin(), uniqueKeys.end(), field) == uniqueKeys.end()) {
        std::cerr << "[getLessEqual] Field is not indexed (must be unique)\n";
        return;
    }

    // 4) Use IndexManager to get offsets
    IndexManager idxMgr(tableName, "Tables/" + tableName, *bufMgr);
    idxMgr.loadIndexes(uniqueKeys);
    std::vector<long> offsets = idxMgr.searchLessEqual(field, value);
    if (offsets.empty()) {
        std::cout << "[getLessEqual] No matching records\n";
        return;
    }

    // 5) Print each record
    for (long off : offsets) {
        printRecordAtOffset(tableName, fields, off);
    }
}

void RecordManager::getBetween(const std::string& tableName) {
    // 1) Load schema & unique keys
    std::ifstream metaIn("Tables/" + tableName + "/meta.txt");
    if (!metaIn) { std::cerr << "[getBetween] Table not found\n"; return; }
    std::string schemaStr, keysStr;
    std::getline(metaIn, schemaStr);
    std::getline(metaIn, keysStr);
    Schema schema(schemaStr, keysStr);
    const auto& fields = schema.getFields();
    const auto& uniqueKeys = schema.getUniqueKeys();

    // 2) Get user input "field=low:high"
    std::cout << "Enter field=low:high (e.g. id=100:200): ";
    std::string input;
    std::getline(std::cin, input);
    auto eq = input.find('=');
    auto colon = input.find(':', eq + 1);
    if (eq == std::string::npos || colon == std::string::npos) {
        std::cerr << "[getBetween] Bad format. Use field=low:high\n";
        return;
    }
    std::string field = input.substr(0, eq);
    std::string lowVal = input.substr(eq + 1, colon - (eq + 1));
    std::string highVal = input.substr(colon + 1);
    auto trim = [](std::string& s) {
        s.erase(std::remove_if(s.begin(), s.end(), ::isspace), s.end());
        };
    trim(field); trim(lowVal); trim(highVal);

    // 3) Check field is unique
    if (std::find(uniqueKeys.begin(), uniqueKeys.end(), field) == uniqueKeys.end()) {
        std::cerr << "[getBetween] Field is not indexed (must be unique)\n";
        return;
    }

    // 4) Use IndexManager to get offsets
    IndexManager idxMgr(tableName, "Tables/" + tableName, *bufMgr);
    idxMgr.loadIndexes(uniqueKeys);
    std::vector<long> offsets = idxMgr.searchBetween(field, lowVal, highVal);
    if (offsets.empty()) {
        std::cout << "[getBetween] No matching records\n";
        return;
    }

    // 5) Print each record
    for (long off : offsets) {
        printRecordAtOffset(tableName, fields, off);
    }
}
