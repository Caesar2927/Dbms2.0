#pragma once

#include <string>
#include <vector>
#include <cstdint>
#include "BufferManager.h"

/// Each PageMeta occupies 6 bytes: 4 for pageId, 2 for freeSlots.
/// Storing these sequentially in 4 KB pages (so PAGE_SIZE / sizeof(PageMeta) entries per page).
struct PageMeta {
    uint32_t pageId;
    uint16_t freeSlots;
};

class FreeSpaceManager {
public:
    /// tablePath: path to the table directory (e.g. "Tables/myTable")
    /// recordSize: total bytes per record (including the 1-byte isValid flag)
    /// bm: a reference to the global BufferManager instance
    FreeSpaceManager(const std::string& tablePath,
        int                recordSize,
        BufferManager& bm);

    /// Calculate how many records (plus 1-byte valid flag) fit in 4096 bytes
    static int computeRecordsPerPage(int recordSize);

    /// Start fresh: clear all pages and create page 0 with all slots free
    void initialize();

    /// Load all PageMeta entries from free_space.meta (via buffer). If not found, pages stays empty.
    void load();

    /// Persist all PageMeta entries into free_space.meta (via buffer)
    void save() const;

    /// Return a pageId that has at least one free slot; allocate a new page if none exist.
    uint32_t getPageWithFreeSlot();

    /// Mark one slot used on pageId: decrement freeSlots, then save metadata via buffer.
    void markSlotUsed(uint32_t pageId);

    /// Mark one slot free on pageId: increment freeSlots, then save metadata via buffer.
    void markSlotFree(uint32_t pageId);

private:
    std::string          metaPath;        // e.g. "Tables/myTable/free_space.meta"
    int                  recordsPerPage;  // how many PageMeta entries fit in one 4 KB page
    std::vector<PageMeta> pages;          // in-memory list of metadata entries
    BufferManager& bufferManager;   // reference to the global buffer manager

    static constexpr int PAGE_SIZE = 4096;
};
