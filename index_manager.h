#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include "BPlusTree.h"
#include "BufferManager.h"

class IndexManager {
public:
    /// tableName_: e.g. "users"
    /// tablePath_: e.g. "Tables/users"
    /// bm_:        reference to your global BufferManager
    IndexManager(const std::string& tableName_,
        const std::string& tablePath_,
        BufferManager& bm_);

    ~IndexManager();

    /// Build (or re‐build) B+ trees for each unique field in uniqueFields.
    void loadIndexes(const std::vector<std::string>& uniqueFields);

    /// Insert (key → recordOffset) into the B+ tree for fieldName.
    void insertIntoIndex(const std::string& fieldName,
        const std::string& key,
        long                offset);

    /// Returns true if key exists in that field’s index.
    bool existsInIndex(const std::string& fieldName,
        const std::string& key);

    /// Remove a key from the index of fieldName.
    void removeFromIndex(const std::string& fieldName,
        const std::string& key);

    /// Exact‐match lookup; returns recordOffset or –1 if not found.
    long searchIndex(const std::string& fieldName,
        const std::string& key);

    /// Find all recordOffsets whose key ≥ given key.
    std::vector<long> searchGreaterEqual(const std::string& fieldName,
        const std::string& key);

    /// Find all recordOffsets whose key ≤ given key.
    std::vector<long> searchLessEqual(const std::string& fieldName,
        const std::string& key);

    /// Find all recordOffsets whose lowKey ≤ key ≤ highKey.
    std::vector<long> searchBetween(const std::string& fieldName,
        const std::string& lowKey,
        const std::string& highKey);

private:
    std::string                                  tableName;
    std::string                                  tablePath;
    BufferManager& bufMgr;

    // Map: fieldName → pointer to its BPlusTree
    std::unordered_map<std::string, BPlusTree*>  trees;
};
