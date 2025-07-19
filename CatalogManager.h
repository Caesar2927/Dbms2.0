// File: sql/CatalogManager.hpp
#pragma once

#include "schema.h"
#include "BufferManager.h"
#include <string>
#include <unordered_map>
#include <mutex>
#include <memory>

namespace sql {

    /// Manages loading & caching of table schemas (meta.txt).
    /// Uses BufferManager’s META partition under the hood.
    class CatalogManager {
    public:
        /// Initialize with a pointer to your global BufferManager.
        static void init(BufferManager* bufMgr);

        /// Retrieve the Schema for a given table name.
        /// Throws std::runtime_error if table not found or parse error.
        static const Schema& getSchema(const std::string& tableName);

        /// Clears the cached schemas (for tests or reloads).
        static void clearCache();

    private:
        static BufferManager* _bufMgr;
        static std::mutex  _mu;
        static std::unordered_map<std::string, Schema> _cache;

        /// Load and cache schema for tableName; returns reference.
        static const Schema& loadSchema(const std::string& tableName);
    };

} // namespace sql
