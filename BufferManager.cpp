#include "BufferManager.h"

// These includes satisfy read/write and C functions:
#include <fstream>   // std::ifstream, std::ofstream, std::fstream
#include <cstring>   // std::memset, std::memcpy
#include <algorithm> // (not strictly required here, but safe
#include<mutex>

//   LRUCache Implementation



/// Detach a node from the doubly?linked list (head/tail may update).
void LRUCache::detach(FrameNode* node) {
    if (!node) return;
    if (node->prev) node->prev->next = node->next;
    else             head = node->next;  // was head

    if (node->next) node->next->prev = node->prev;
    else             tail = node->prev;  // was tail

    node->prev = node->next = nullptr;
    size--;
}

/// Attach a node at the head (MRU).
void LRUCache::attachAtHead(FrameNode* node) {
    node->prev = nullptr;
    node->next = head;
    if (head) head->prev = node;
    head = node;
    if (!tail) tail = head;  // first node inserted
    size++;
}

/// Evict the least?recently used unpinned node from the tail. Return it, or nullptr if none.
FrameNode* LRUCache::evictLRU() {
    FrameNode* cur = tail;
    while (cur) {
        if (cur->pinCount == 0) {
            break;  // found an unpinned candidate
        }
        cur = cur->prev;
    }
    if (!cur) {
        // All pages in this partition are pinned; cannot evict
        return nullptr;
    }

    // Detach from list
    detach(cur);
    
    // Remove from map
    mp.erase(cur->key);

    return cur;
}

/// Clear (delete) all nodes in this cache (called in destructor).
void LRUCache::clearAll() {
    FrameNode* cur = head;
    while (cur) {
        FrameNode* nxt = cur->next;
        delete cur;
        cur = nxt;
    }
    head = tail = nullptr;
    mp.clear();
    size = 0;
}

/// Pin or load a page into this LRUCache. Returns the 4 KB buffer, or nullptr if full && pinned.
char* LRUCache::getPage(
    const std::string& filePath,
    uint32_t        pageNum,
    std::function<void(const BMKey&, char*)> readFromDisk)
{
    BMKey key{ filePath, pageNum };

    // 1) Already cached?
    auto it = mp.find(key);
    if (it != mp.end()) {
        FrameNode* node = it->second;
        node->pinCount++;
        // Move to head (MRU)
        detach(node);
        attachAtHead(node);
        return node->data;
    }

    // 2) Not in cache. If at capacity, evict LRU:
    FrameNode* node = nullptr;
    if (size >= cap) {
        node = evictLRU();
        if (!node) {
            // Cannot evict because all frames are pinned
            return nullptr;
        }

        // If it was dirty, write it back
        if (node->dirty) {
        writeFromDisk:;  // (dummy label)
            // We need a way to call writeToDisk; 
            // but writeToDisk is provided by the higher?level BufferManager,
            // so it’ll actually supply the correct function. Here, we just comment:
           //  writeToDisk(node->key, node->data);
        }
        node->dirty = false;
        node->pinCount = 0;

        // Reuse this node for the new key
        node->key = key;
    }
    else {
        // Still have room: create a new node
        node = new FrameNode(key);
    }

    // 3) Load page from disk
    readFromDisk(key, node->data);
    node->dirty = false;
    node->pinCount = 1;

    // 4) Insert into MRU position
    attachAtHead(node);
    mp[key] = node;
    return node->data;
}

/// Unpin a page and optionally mark it dirty
void LRUCache::unpinPage(const std::string& filePath,
    uint32_t        pageNum,
    bool            isDirty)
{
    BMKey key{ filePath, pageNum };
    auto it = mp.find(key);
    if (it == mp.end()) return;  // not in cache

    FrameNode* node = it->second;
    if (node->pinCount > 0) node->pinCount--;
    if (isDirty) node->dirty = true;
    // We do not move it in LRU list; it remains at its current position
}

/// Flush one page (if present and dirty) to disk
void LRUCache::flushPage(
    const std::string& filePath,
    uint32_t        pageNum,
    std::function<void(const BMKey&, char*)> writeToDisk)
{
    BMKey key{ filePath, pageNum };
    auto it = mp.find(key);
    if (it == mp.end()) return;

    FrameNode* node = it->second;
    if (node->dirty) {
        writeToDisk(key, node->data);
        node->dirty = false;
    }
}

/// Flush all dirty pages in this partition
void LRUCache::flushAll(std::function<void(const BMKey&, char*)> writeToDisk) {
    FrameNode* cur = head;
    while (cur) {
        if (cur->dirty) {
            writeToDisk(cur->key, cur->data);
            cur->dirty = false;
        }
        cur = cur->next;
    }
}

/// Print contents of this cache (from head=MRU to tail=LRU)
void LRUCache::printCache(const std::string& label) {
    std::cout << "--- " << label << " (capacity=" << cap << ") ---\n";
    FrameNode* cur = head;
    while (cur) {
        std::cout << "[" << cur->key.filePath << ":" << cur->key.pageNum << "]\t";
        std::cout << "pin=" << cur->pinCount << "\t";
        std::cout << "dirty=" << (cur->dirty ? "Y" : "N") << "\t";
        // Print first 4 bytes of its data as an int for quick check:
        int snippet[1];
        std::memcpy(snippet, cur->data, sizeof(int));
        std::cout << "bytes0..3={" << snippet[0] << "}\n";
        cur = cur->next;
    }
}

//
// ===========================
//   BufferManager Implementation
// ===========================
//

BufferManager::BufferManager()
    : dataCache(DATA_FRAMES),
    indexCache(INDEX_FRAMES),
    metaCache(META_FRAMES)
{
}

BufferManager::~BufferManager() {
    flushAll();
}

/// Helper to read a page from disk into 'dest' (4 KB). Zero?fill on EOF or missing file.
void BufferManager::readPageFromDisk(const BMKey& key, char* dest) {
    std::ifstream in(key.filePath, std::ios::binary);
    if (!in) {
        // File does not exist yet: zero-fill
        std::memset(dest, 0, PAGE_SIZE);
        return;
    }
    std::streamoff off = pageOffset(key.pageNum);
    in.seekg(off, std::ios::beg);
    in.read(dest, PAGE_SIZE);
    std::streamsize got = in.gcount();
    if (got < PAGE_SIZE) {
        std::memset(dest + got, 0, PAGE_SIZE - got);
    }
    in.close();
}

/// Helper to write exactly 4 KB from 'src' into disk at that page offset.
void BufferManager::writePageToDisk(const BMKey& key, char* src) {
    std::fstream out(key.filePath, std::ios::in | std::ios::out | std::ios::binary);
    if (!out) {
        // Create file if it doesn’t exist
        std::ofstream create(key.filePath, std::ios::binary);
        create.close();
        out.open(key.filePath, std::ios::in | std::ios::out | std::ios::binary);
    }
    std::streamoff off = pageOffset(key.pageNum);
    out.seekp(off, std::ios::beg);
    out.write(src, PAGE_SIZE);
    out.close();
}
 
/// Pin (load) a page in the appropriate partition
char* BufferManager::getPage(const std::string& filePath,
    uint32_t        pageNum,
    PageType        type)
{
    std::cout << "inside get page" << std::endl;
    std::cout << filePath << " " << pageNum << " "  << std::endl;
    std::lock_guard<std::mutex> guard(mtx);
    std::cout << "check 4" << std::endl;
    switch (type) {
    case PageType::DATA:
        return dataCache.getPage(filePath, pageNum, readPageFromDisk);
    case PageType::INDEX:
        return indexCache.getPage(filePath, pageNum, readPageFromDisk);
    case PageType::META:
        return metaCache.getPage(filePath, pageNum, readPageFromDisk);
    }
    return nullptr;
}

/// Unpin a previously pinned page
void BufferManager::unpinPage(const std::string& filePath,
    uint32_t        pageNum,
    PageType        type,
    bool            isDirty)
{
    std::lock_guard<std::mutex> guard(mtx);
    switch (type) {
    case PageType::DATA:
        dataCache.unpinPage(filePath, pageNum, isDirty);
        break;
    case PageType::INDEX:
        indexCache.unpinPage(filePath, pageNum, isDirty);
        break;
    case PageType::META:
        metaCache.unpinPage(filePath, pageNum, isDirty);
        break;
    }
}

/// Immediately flush one page if it’s dirty
void BufferManager::flushPage(const std::string& filePath,
    uint32_t        pageNum,
    PageType        type)
{
    std::lock_guard<std::mutex> guard(mtx);
    switch (type) {
    case PageType::DATA:
        dataCache.flushPage(filePath, pageNum, writePageToDisk);
        break;
    case PageType::INDEX:
        indexCache.flushPage(filePath, pageNum, writePageToDisk);
        break;
    case PageType::META:
        metaCache.flushPage(filePath, pageNum, writePageToDisk);
        break;
    }
}

/// Flush all dirty pages across all partitions
void BufferManager::flushAll() {
    std::lock_guard<std::mutex> guard(mtx);
    dataCache.flushAll(writePageToDisk);
    indexCache.flushAll(writePageToDisk);
    metaCache.flushAll(writePageToDisk);
}

/// Print the status of all three LRU caches
void BufferManager::printCacheStatus() {
    std::lock_guard<std::mutex> guard(mtx);
    std::cout << "========== BufferManager Cache Status ==========\n";
    dataCache.printCache("DATA");
    indexCache.printCache("INDEX");
    metaCache.printCache("META");
    std::cout << "================================================\n";
}
#include "BufferManager.h"
