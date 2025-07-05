#include "table_manager.h"
#include "Schema.h"
#include "record_manager.h"
#include "index_manager.h"
#include "free_space_manager.h"

#include <iostream>
#include <filesystem>
#include <fstream>
#include <limits>

namespace fs = std::filesystem;

// Define the static BufferManager* member:
BufferManager* TableManager::bufMgr = nullptr;

void TableManager::createTable() {
    if (!bufMgr) {
        std::cerr << "[createTable] ERROR: BufferManager not set.\n";
        return;
    }

    std::string tableName;
    std::cout << "Enter table name: ";
    std::cin >> tableName;
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    std::string schemaInput;
    std::cout << "Enter schema (e.g., int id, string name, int age):\n> ";
    std::getline(std::cin, schemaInput);

    std::cout << "Enter unique keys (comma separated):\n> ";
    std::string keys;
    std::getline(std::cin, keys);

    std::string tablePath = "Tables/" + tableName;
    if (fs::exists(tablePath)) {
        std::cout << "Table already exists.\n";
        return;
    }

    // 1) Create directory
    fs::create_directories(tablePath);

    // 2) Save schema (creates meta.txt)
    Schema schema(schemaInput, keys);
    schema.saveToFile(tablePath + "/meta.txt");

    // 3) Create empty data table file (data.tbl)
    {
        std::ofstream dataFile(tablePath + "/data.tbl", std::ios::binary);
        dataFile.close();
    }

    // 4) Compute recordSize from schema (sum of field.length)
    int recordSize = 0;
    for (auto& f : schema.getFields()) {
        recordSize += f.length;
    }

    // 5) Initialize free-space metadata via FreeSpaceManager
    //    This writes page 0 of "Tables/<tableName>/free_space.meta" through the buffer.
    FreeSpaceManager fsm(tablePath, recordSize, *bufMgr);
    fsm.initialize();

    // 6) Initialize empty indexes via IndexManager
    //    For each unique key, create an empty "<field>.idx" file.
    IndexManager idxMgr(tableName, tablePath, *bufMgr);
    idxMgr.loadIndexes(schema.getUniqueKeys());
    // No need to call idxMgr.saveIndexes() here because B+ Tree is empty;
    // however, calling search/insert later will lazily allocate pages in buffer.

    std::cout << "Table '" << tableName << "' created successfully.\n";
}

void TableManager::useTable() {
    if (!bufMgr) {
        std::cerr << "[useTable] ERROR: BufferManager not set.\n";
        return;
    }

    std::string tableName;
    std::cout << "Enter table name to use: ";
    std::cin >> tableName;
    std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

    std::string tablePath = "Tables/" + tableName;
    if (!fs::exists(tablePath)) {
        std::cout << "Table not found.\n";
        return;
    }

    while (true) {
        std::cout << "\n--- Table: " << tableName << " ---\n"
            << "1. Add Record\n"
            << "2. Find Record\n"
            << "3. Delete Record\n"
            << "4. Print All Records\n"
            << "5. Exit\n"
            << "Enter choice: ";
        int choice;
        std::cin >> choice;
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

        switch (choice) {
        case 1: {
          //  std::cout << "entering recoed manager" << std::endl;
            RecordManager::addRecord(tableName);
            
            break;
        }

        case 2:
            RecordManager::findRecord(tableName);
            break;

        case 3:
            RecordManager::deleteRecord(tableName);
            break;

        case 4:
            RecordManager::printAllRecords(tableName);
            break;

        case 5:
            return;

        default:
            std::cout << "Invalid choice. Please try again.\n";
        }
    }
}

void TableManager::deleteTable() {
    std::string tableName;
    std::cout << "Enter table name to delete: ";
    std::cin >> tableName;

    std::string tablePath = "Tables/" + tableName;
    if (!fs::exists(tablePath)) {
        std::cout << "Table not found.\n";
        return;
    }

    fs::remove_all(tablePath);
    std::cout << "Table '" << tableName << "' deleted.\n";
}
