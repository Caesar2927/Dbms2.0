// File: sql/CatalogManager.cpp
#include "CatalogManager.h"
#include <fstream>
#include <sstream>
#include <iostream>

using namespace sql;

BufferManager* CatalogManager::_bufMgr = nullptr;
std::mutex CatalogManager::_mu;
std::unordered_map<std::string, Schema> CatalogManager::_cache;

void CatalogManager::init(BufferManager* bufMgr) {
    _bufMgr = bufMgr;
}

const Schema& CatalogManager::getSchema(const std::string& tableName) {
    std::lock_guard<std::mutex> lock(_mu);
    auto it = _cache.find(tableName);
    if (it != _cache.end()) {
        return it->second;
    }
    return loadSchema(tableName);
}

void CatalogManager::clearCache() {
    std::lock_guard<std::mutex> lock(_mu);
    _cache.clear();
}

const Schema& CatalogManager::loadSchema(const std::string& tableName) {
    if (!_bufMgr) {
        throw std::runtime_error("CatalogManager not initialized with BufferManager");
    }
    // Build the meta file path
    std::string metaPath = "Tables/" + tableName + "/meta.txt";

    // Pin page 0 of the meta file
    char* page = _bufMgr->getPage(
        metaPath,
        0,
        PageType::META
    );
    if (!page) {
        throw std::runtime_error("Cannot load meta for table: " + tableName);
    }

    // Read up to two lines from the 4KB buffer
    std::istringstream in(std::string(page, BufferManager::PAGE_SIZE));
    std::string schemaLine, keysLine;
    if (!std::getline(in, schemaLine) || !std::getline(in, keysLine)) {
        _bufMgr->unpinPage(metaPath, 0, PageType::META, false);
        throw std::runtime_error("Invalid meta.txt format for table: " + tableName);
    }

    // Unpin the meta page
    _bufMgr->unpinPage(metaPath, 0, PageType::META, false);

    // Parse into Schema
    Schema schema(schemaLine, keysLine);
    // Cache & return
    auto [it, inserted] = _cache.emplace(tableName, std::move(schema));
    return it->second;
}
