#pragma once

// Required C++ headers
#include <string>
#include <unordered_map>   // for std::unordered_map
#include <functional>      // for std::function
#include <cstdint>         // for std::uint32_t
#include <mutex>           // for std::mutex, std::lock_guard
#include <iostream>        // for std::cout, std::cerr

// These constants partition our 150 frames:
static constexpr int PAGE_SIZE = 4096;
static constexpr int DATA_FRAMES = 110;
static constexpr int INDEX_FRAMES = 30;
static constexpr int META_FRAMES = 10;

enum class PageType { DATA, INDEX, META };

/// A key to identify a page: file path + page number
struct BMKey {
    std::string filePath;   // e.g. "Tables/users/data.tbl"
    uint32_t    pageNum;    // page index (offset / PAGE_SIZE)

    bool operator==(BMKey const& o) const {
        return filePath == o.filePath && pageNum == o.pageNum;
    }
};

namespace std {
    // Specialize std::hash for BMKey so we can use unordered_map<BMKey, ...>
    template<>
    struct hash<BMKey> {
        size_t operator()(BMKey const& k) const {
            auto h1 = std::hash<std::string>()(k.filePath);
            auto h2 = std::hash<uint32_t>()(k.pageNum);
            return h1 ^ (h2 << 1);
        }
    };
}

/// Each node holds exactly one 4 KB page in memory (a frame).
/// We keep it in a doubly?linked list to implement LRU.
struct FrameNode {
    BMKey       key;       // Which page this node holds
    char* data;      // new char[PAGE_SIZE]
    bool        dirty;     // Was it modified since load?
    int         pinCount;  // >0 means “in use”—cannot evict
    FrameNode* prev;      // for LRU list
    FrameNode* next;

    FrameNode(const BMKey& k)
        : key(k),
        data(new char[PAGE_SIZE]),
        dirty(false),
        pinCount(0),
        prev(nullptr),
        next(nullptr) {
    }

    ~FrameNode() {
        delete[] data;
    }
};

/// LRUCache: a fixed?capacity cache for one partition (DATA/INDEX/META).
/// Internally uses a doubly?linked list (head=MRU, tail=LRU) + unordered_map.
class LRUCache {
public:
    /// capacity = number of frames/pages in this partition
    LRUCache(int capacity)
        : cap(capacity), head(nullptr), tail(nullptr), size(0) {
    }

    ~LRUCache() {
        clearAll();
    }

    /// Pin (or load) the page. Returns its 4 KB buffer (or nullptr if no free frame).
    char* getPage(
        const std::string& filePath,
        uint32_t        pageNum,
        std::function<void(const BMKey&, char*)> readFromDisk);

    /// Unpin a page; if isDirty, mark it so.
    void unpinPage(
        const std::string& filePath,
        uint32_t        pageNum,
        bool            isDirty);

    /// Flush one page to disk (calls writeToDisk if dirty).
    void flushPage(
        const std::string& filePath,
        uint32_t        pageNum,
        std::function<void(const BMKey&, char*)> writeToDisk);

    /// Flush all dirty pages in this partition to disk.
    void flushAll(
        std::function<void(const BMKey&, char*)> writeToDisk);

    /// Print contents of this LRU cache (for debugging).
    void printCache(const std::string& label);

private:
    int cap;    // maximum number of pages
    int size;   // current # of pages loaded

    FrameNode* head;  // Most Recently Used (MRU)
    FrameNode* tail;  // Least Recently Used (LRU)

    // Map from BMKey ? FrameNode* (for O(1) lookup)
    std::unordered_map<BMKey, FrameNode*> mp;

    /// Detach a node from the linked list (without deleting it).
    void detach(FrameNode* node);

    /// Attach a node at the head of the list (MRU position).
    void attachAtHead(FrameNode* node);

    /// Evict the LRU node (tail) that is not pinned. Return that node or nullptr if none.
    FrameNode* evictLRU();

    /// Delete all nodes (in destructor).
    void clearAll();
};

/// The BufferManager holds three LRUCache partitions (DATA/INDEX/META) and
/// dispatches calls based on PageType. It also provides readPageFromDisk/writePageToDisk
/// helpers.
class BufferManager {
   

public:
    static constexpr uint32_t PAGE_SIZE = 4096;
    BufferManager();
    ~BufferManager();

    /// Pin (load) the requested page into memory, returning its 4 KB buffer.
    /// Caller must eventually call unpinPage().
    char* getPage(
        const std::string& filePath,
        uint32_t        pageNum,
        PageType        type);

    /// Unpin a previously pinned page; if isDirty, mark as dirty.
    void unpinPage(
        const std::string& filePath,
        uint32_t        pageNum,
        PageType        type,
        bool            isDirty);

    /// Immediately flush a single page back to disk if it is dirty.
    void flushPage(
        const std::string& filePath,
        uint32_t        pageNum,
        PageType        type);

    /// Flush all dirty pages in all three partitions back to disk.
    void flushAll();

    /// Print status of all three caches (for debugging).
    void printCacheStatus();

private:
    LRUCache dataCache;   // capacity = DATA_FRAMES
    LRUCache indexCache;  // capacity = INDEX_FRAMES
    LRUCache metaCache;   // capacity = META_FRAMES

    std::mutex mtx;  // protect all operations

    /// Read a page from disk (pageNum*PAGE_SIZE) into dest. Zero-fill if file/EOF.
    static void readPageFromDisk(const BMKey& key, char* dest);

    /// Write a page’s 4 KB buffer to disk at pageNum*PAGE_SIZE (create file if needed).
    static void writePageToDisk(const BMKey& key, char* src);

    static inline std::streamoff pageOffset(uint32_t pageNum) {
        return static_cast<std::streamoff>(pageNum) * PAGE_SIZE;
    }
};
