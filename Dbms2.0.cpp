// File: main.cpp

#include <iostream>
#include <thread>
#include <atomic>
#include <chrono>
#include <limits>

#include "table_manager.h"
#include "record_manager.h"
#include "BufferManager.h"

#include "LockManager.h"
#include "WALManager.h"
#include "TransactionManager.h"
#include "TransactionController.h"

int main() {
    // 1) Singletons for core subsystems
    static BufferManager     bufferManager;
    static LockManager       lockMgr;
    static WALManager        walMgr("Tables/wal.log");
    static TransactionManager txnMgr(lockMgr, walMgr);

    // 2) Wire buffer into table/record managers
    TableManager::bufMgr = &bufferManager;
    RecordManager::bufMgr = &bufferManager;

    // 3) Background flusher
    std::atomic<bool> keepRunning{ true };
    std::thread flusher([&]() {
        using namespace std::chrono_literals;
        while (keepRunning) {
            std::this_thread::sleep_for(20s);
            std::cout << "[Flusher] flushAll()\n";
            bufferManager.flushAll();
        }
        });

    // 4) CLI loop
    while (true) {
        std::cout << "\n--- Simple DBMS CLI ---\n";
        std::cout << "1. Create Table\n";
        std::cout << "2. Use Table\n";
        std::cout << "3. Delete Table\n";
        std::cout << "4. Start Transaction (single-row update)\n";
        std::cout << "5. Print Buffer Cache Status\n";
        std::cout << "6. Exit\n";
        std::cout << "Enter choice: ";

        int choice;
        std::cin >> choice;
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');

        switch (choice) {
        case 1:
            TableManager::createTable();
            break;
        case 2:
            TableManager::useTable();
            break;
        case 3:
            TableManager::deleteTable();
            break;
        case 4:

            TransactionController::run(
                bufferManager,
                lockMgr,
                walMgr,
                txnMgr
            );
            break;
        case 5:
            std::cout << "[Main] Buffer Cache Status:\n";
            bufferManager.printCacheStatus();
            break;
        case 6:
            // shut down flusher
            keepRunning = false;
            if (flusher.joinable()) flusher.join();
            // final flush
            bufferManager.flushAll();
            std::cout << "Exiting.\n";
            return 0;
        default:
            std::cout << "Invalid choice, try again.\n";
        }
    }
}
