#include "index_manager.h"
#include <filesystem>
#include <fstream>
#include <iostream>

namespace fs = std::filesystem;

IndexManager::IndexManager(const std::string& tableName_,
    const std::string& tablePath_,
    BufferManager& bm_)
    : tableName(tableName_),
    tablePath(tablePath_),
    bufMgr(bm_)
{
}

IndexManager::~IndexManager() {
    for (auto& [_, tree] : trees) {
        delete tree;
    }
    trees.clear();
}

void IndexManager::loadIndexes(const std::vector<std::string>& uniqueFields) {
    for (const auto& field : uniqueFields) {
        std::string idxFile = tablePath + "/" + field + ".idx";
        // Ensure the table directory exists
        if (!fs::exists(tablePath)) {
            std::cerr << "[IndexManager] loadIndexes: missing table path "
                << tablePath << "\n";
            continue;
        }

        if (!fs::exists(idxFile)) {
            std::ofstream(idxFile, std::ios::binary).close();
        }
        
        trees[field] = new BPlusTree(idxFile, bufMgr);
    }
}

void IndexManager::insertIntoIndex(const std::string& fieldName,
    const std::string& key,
    long                offset)
{
    auto it = trees.find(fieldName);
    if (it == trees.end()) {
        std::cerr << "[IndexManager] insertIntoIndex: no index for field '"
            << fieldName << "'\n";
        return;
    }
    it->second->insert(key, offset);
}

bool IndexManager::existsInIndex(const std::string& fieldName,
    const std::string& key)
{
    auto it = trees.find(fieldName);
    if (it == trees.end()) return false;
    long dummy;
    return it->second->search(key, dummy);
}

void IndexManager::removeFromIndex(const std::string& fieldName,
    const std::string& key)
{
    auto it = trees.find(fieldName);
    if (it == trees.end()) return;
    it->second->remove(key);
}

long IndexManager::searchIndex(const std::string& fieldName,
    const std::string& key)
{
    auto it = trees.find(fieldName);
    if (it == trees.end()) {
        // Field not indexed
        return -1;
    }
    long offset;
    if (it->second->search(key, offset)) {
        return offset;
    }
    return -1;
}

std::vector<long> IndexManager::searchGreaterEqual(const std::string& fieldName,
    const std::string& key)
{
    std::vector<long> results;
    auto it = trees.find(fieldName);
    if (it == trees.end()) return results;
    // endKey = "" → no upper bound
    it->second->rangeSearch(key, "", results);
    return results;
}

std::vector<long> IndexManager::searchLessEqual(const std::string& fieldName,
    const std::string& key)
{
    std::vector<long> results;
    auto it = trees.find(fieldName);
    if (it == trees.end()) return results;
    // startKey = "" → start from leftmost
    it->second->rangeSearch("", key, results);
    return results;
}

std::vector<long> IndexManager::searchBetween(const std::string& fieldName,
    const std::string& lowKey,
    const std::string& highKey)
{
    std::vector<long> results;
    auto it = trees.find(fieldName);
    if (it == trees.end()) return results;
    it->second->rangeSearch(lowKey, highKey, results);
    return results;
}
