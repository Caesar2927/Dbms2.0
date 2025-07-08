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

 
    fs::create_directories(tablePath);

    Schema schema(schemaInput, keys);
    schema.saveToFile(tablePath + "/meta.txt");

   
    {
        std::ofstream dataFile(tablePath + "/data.tbl", std::ios::binary);
        dataFile.close();
    }

 
    int recordSize = 0;
    for (auto& f : schema.getFields()) {
        recordSize += f.length;
    }

    FreeSpaceManager fsm(tablePath, recordSize, *bufMgr);
    fsm.initialize();


    IndexManager idxMgr(tableName, tablePath, *bufMgr);
    idxMgr.loadIndexes(schema.getUniqueKeys());


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
