#pragma once

#include <string>
#include <vector>
#include "Schema.h"
#include "index_manager.h"
#include "free_space_manager.h"
#include "BufferManager.h"

class RecordManager {
public:
     static BufferManager* bufMgr;
    /// Add a new record to 'tableName'; reads user input for each field.
    static void addRecord(const std::string& tableName);

    /// Find a single record via "field=value" syntax and print it.
    static void findRecord(const std::string& tableName);

    /// Delete a record by "field=value" (must be a unique field).
    static void deleteRecord(const std::string& tableName);

    /// Print every valid record in the table, page by page.
    static void printAllRecords(const std::string& tableName);

    /// Print all records whose unique 'field' ≥ value.
    static void getGreaterEqual(const std::string& tableName);

    /// Print all records whose unique 'field' ≤ value.
    static void getLessEqual(const std::string& tableName);

    /// Print all records whose unique 'field' ∈ [low:high].
    static void getBetween(const std::string& tableName);
   

private:
    /// Helper: print exactly one record at 'offset' (4 KB page buffer).
    static void printRecordAtOffset(const std::string& tableName,
        const std::vector<Schema::Field>& fields,
        long offset);

    /// A reference to the global BufferManager.  Defined externally.
    
};
