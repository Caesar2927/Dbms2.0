#pragma once

#include <string>
#include "BufferManager.h"

/// Manages high‐level table operations (create, use, delete).
/// Internally, it will hand off to Schema, FreeSpaceManager, IndexManager, RecordManager,
/// all of which expect to share the same global BufferManager.
class TableManager {
public:
    /// Must be set exactly once (e.g. in main()):
    ///     TableManager::bufMgr = &globalBufferManager;
    static BufferManager* bufMgr;

    /// Create a brand‐new table. Prompts user for name, schema, and unique keys.
    /// Internally:
    ///   1) creates Tables/<tableName> directory
    ///   2) writes meta.txt via Schema
    ///   3) creates empty data.tbl (zero bytes)
    ///   4) initializes FreeSpaceManager (writes free_space.meta via buffer)
    ///   5) initializes empty IndexManager (each .idx is empty, but B+ tree structures are buffered)
    static void createTable();

    /// Present a CLI menu to “use” an existing table:
    ///   1) Add Record
    ///   2) Find Record
    ///   3) Delete Record
    ///   4) Print All Records
    ///   5) Exit
    static void useTable();

    /// Delete an entire table (remove Tables/<tableName> directory tree)
    static void deleteTable();
};
