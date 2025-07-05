#include "free_space_manager.h"

#include <cstring>    // for std::memset
#include <iostream>   // for std::cerr, std::cout

using std::uint32_t;
using std::uint16_t;

FreeSpaceManager::FreeSpaceManager(const std::string& tablePath,
    int                recordSize,
    BufferManager& bm)
    : metaPath(tablePath + "/free_space.meta"),
    recordsPerPage(computeRecordsPerPage(recordSize)),
    bufferManager(bm)
{
    std::cout << "initilized freespace manager" << std :: endl;

}

/// Compute how many (recordSize + 1)‐byte slots fit in 4096 bytes
int FreeSpaceManager::computeRecordsPerPage(int recordSize) {
    // +1 for the isValid flag per record; we store one PageMeta (6 bytes) per record slot
    return PAGE_SIZE / (recordSize + 1);
}

/// Initialize “free_space.meta” with exactly one PageMeta at pageId 0,
/// having freeSlots = recordsPerPage. Then immediately save via buffer.
void FreeSpaceManager::initialize() {
    pages.clear();
    pages.push_back({ 0, static_cast<uint16_t>(recordsPerPage) });
    std::cout << "FreeSpaceManager: Initialized with page 0, freeSlots="
        << recordsPerPage << "\n";
    save();
}

/// Load all PageMeta entries from “free_space.meta” via the buffer. Stops once
/// it encounters a page of all-zero entries (meaning “no more saved metadata”).
void FreeSpaceManager::load() {
    std::cout << "entered load fsm" << std::endl;
    pages.clear();
    std::cout << "cleared page" << std::endl;
    int entriesPerPage = PAGE_SIZE / static_cast<int>(sizeof(PageMeta));

    // Read pages one by one, until we find an entirely‐zero page or no data at all.
    for (uint32_t pageNum = 0; ; ++pageNum) {
        BMKey key{ metaPath, pageNum };
        std::cout << "created bm keys" << std::endl;
        char* pageBuf = bufferManager.getPage(key.filePath, key.pageNum, PageType::META);
        
        if (!pageBuf) {
            std::cerr << "FreeSpaceManager::load: Cannot pin meta page " << pageNum << "\n";
            return;
        }
        std::cout << "done with page creation" << std::endl;
        PageMeta* metaArr = reinterpret_cast<PageMeta*>(pageBuf);
        bool anyNonZero = false;
        for (int i = 0; i < entriesPerPage; ++i) {
            if (metaArr[i].pageId != 0 || metaArr[i].freeSlots != 0) {
                anyNonZero = true;
                pages.push_back(metaArr[i]);
            }
            else {
                // If this is the very first entry on page 0, it might legitimately be {0,0};
                // but that means “no metadata” at all. So we break out only if we've previously
                // loaded something or this is not page 0.
                if (pageNum == 0 && pages.empty()) {
                    // No metadata saved yet; just return with pages empty.
                    bufferManager.unpinPage(key.filePath, key.pageNum, PageType::META, false);
                    return;
                }
                // Otherwise, we hit a zero entry after having loaded some metadata: end.
                break;
            }
        }

        bufferManager.unpinPage(key.filePath, key.pageNum, PageType::META, false);

        if (!anyNonZero) {
            // Entire page was empty => no more metadata to load
            break;
        }
    }
    std::cout << "loaded page without any issues" << std::endl;
}

/// Save all PageMeta entries into “free_space.meta” via the buffer, one page at a time.
/// Each 4 KB page holds up to entriesPerPage entries. We write them in order:
///   pages[0], pages[1], …, pages[n-1].
void FreeSpaceManager::save() const {
    int entriesPerPage = PAGE_SIZE / static_cast<int>(sizeof(PageMeta));
    size_t totalEntries = pages.size();

    // For each page‐index needed (0..ceil(totalEntries/entriesPerPage)-1):
    size_t numPages = (totalEntries + entriesPerPage - 1) / entriesPerPage;
    for (size_t pageNum = 0; pageNum < numPages; ++pageNum) {
        BMKey key{ metaPath, static_cast<uint32_t>(pageNum) };
        char* pageBuf = bufferManager.getPage(key.filePath, key.pageNum, PageType::META);
        if (!pageBuf) {
            std::cerr << "FreeSpaceManager::save: Cannot pin meta page " << pageNum << "\n";
            return;
        }

        // Zero out entire 4 KB page first
        std::memset(pageBuf, 0, PAGE_SIZE);

        // Copy up to entriesPerPage entries from pages[] into this buffer
        size_t baseIndex = pageNum * entriesPerPage;
        size_t limit = std::min(baseIndex + entriesPerPage, totalEntries);
        PageMeta* metaArr = reinterpret_cast<PageMeta*>(pageBuf);

        for (size_t i = baseIndex; i < limit; ++i) {
            int offset = static_cast<int>(i - baseIndex);
            metaArr[offset] = pages[i];
        }

        bufferManager.unpinPage(key.filePath, key.pageNum, PageType::META, true);
    }

    // If there are leftover pages on disk beyond numPages, we could optionally truncate:
    // (Omitted for simplicity—those extra pages will simply be overwritten next time.)
}

/// Return a pageId that has at least one free slot. If none exist in current `pages`,
/// add a new PageMeta {newId, recordsPerPage} and save. Then return newId.
uint32_t FreeSpaceManager::getPageWithFreeSlot() {
    for (auto const& pm : pages) {
        if (pm.freeSlots > 0) {
            return pm.pageId;
        }
    }
    // No existing page has free slots: allocate new pageId
    uint32_t newId = pages.empty() ? 0 : (pages.back().pageId + 1);
    pages.push_back({ newId, static_cast<uint16_t>(recordsPerPage) });
    save();
    std::cout << "giving page with new slot" << std::endl;
    return newId;
}

/// Decrement freeSlots on pageId, then save() via buffer
void FreeSpaceManager::markSlotUsed(uint32_t pageId) {
    for (auto& pm : pages) {
        if (pm.pageId == pageId) {
            if (pm.freeSlots > 0) {
                pm.freeSlots--;
            }
            else {
                std::cerr << "Warning: FreeSpaceManager::markSlotUsed: page " << pageId
                    << " has no free slots\n";
            }
            save();
            return;
        }
    }
    std::cerr << "Warning: FreeSpaceManager::markSlotUsed: page " << pageId
        << " not tracked in free_space.meta\n";
}

/// Increment freeSlots on pageId, then save() via buffer
void FreeSpaceManager::markSlotFree(uint32_t pageId) {
    for (auto& pm : pages) {
        if (pm.pageId == pageId) {
            if (pm.freeSlots < recordsPerPage) {
                pm.freeSlots++;
            }
            else {
                std::cerr << "Warning: FreeSpaceManager::markSlotFree: page " << pageId
                    << " already fully free\n";
            }
            save();
            return;
        }
    }
    std::cerr << "Warning: FreeSpaceManager::markSlotFree: page " << pageId
        << " not tracked in free_space.meta\n";
}
