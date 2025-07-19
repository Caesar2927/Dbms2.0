// File: record_manager_sqlin.hpp
#pragma once

#include <string>
#include <vector>
#include <optional>
#include <fstream>

/// Row is a simple vector of stringified field values.
using Row = std::vector<std::string>;
using Rows = std::vector<Row>;

/// Outcomes for delete/update operations.
enum class DMLResult { NotFound, Deleted, Error };

/// A SQL?style interface into your storage engine.
///
/// Exactly mirrors add/find/delete/get... but via function calls.
class RecordManagerSQL {
public:
    /// Add one record (fields.size() must match table schema).
    /// Returns the byte?offset where it was stored, or -1 on error.
    static long insertRecord(
        const std::string& tableName,
        const std::vector<std::string>& fields
    );

    /// Find by unique key.  Returns one Row if found, or nullopt.
    static std::optional<Row> findRecord(
        const std::string& tableName,
        const std::string& fieldName,
        const std::string& value
    );

    /// Delete by unique key.  Returns Deleted / NotFound / Error.
    static DMLResult deleteRecord(
        const std::string& tableName,
        const std::string& fieldName,
        const std::string& value
    );

    /// Return all rows in the table, in no particular order.
    static Rows scanAll(const std::string& tableName);

    /// Return all rows with field >= value (field must be unique).
    static Rows scanGreaterEqual(
        const std::string& tableName,
        const std::string& fieldName,
        const std::string& value
    );

    /// Return all rows with field <= value (field must be unique).
    static Rows scanLessEqual(
        const std::string& tableName,
        const std::string& fieldName,
        const std::string& value
    );

    /// Return all rows with low <= field <= high (field must be unique).
    static Rows scanBetween(
        const std::string& tableName,
        const std::string& fieldName,
        const std::string& low,
        const std::string& high
    );
};
