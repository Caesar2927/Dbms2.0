#pragma once

#include <string>
#include <vector>
#include "BufferManager.h"

/// Disk‐based B+ Tree with fixed 4 KB pages.  Keys are std::string up to 39 bytes,
/// pointers (children or record offsets) are 8‐byte longs.  Header is 1+4+8+8 bytes.
class BPlusTree {
public:
    static constexpr int PAGE_SIZE = 4096;
    static constexpr int KEY_SIZE = 40;  // store up to 39 chars + '\0'
    static constexpr int PTR_SIZE = sizeof(long);
    static constexpr int HEADER_SIZE = sizeof(bool)    // isLeaf
        + sizeof(int)     // keyCount
        + sizeof(long)    // parentPage
        + sizeof(long);   // nextLeafPage
    // ORDER = how many keys fit in a 4 KB page:
    static constexpr int ORDER = (PAGE_SIZE - HEADER_SIZE) / (KEY_SIZE + PTR_SIZE);

    /// One node on disk (always exactly 4096 bytes):
    struct Node {
        bool    isLeaf;
        int     keyCount;
        long    parentPage;
        long    nextLeafPage;
        char    keys[ORDER][KEY_SIZE];
        long    children[ORDER + 1];
        long    selfPage;  // which page number this node occupies

        Node(bool leaf = true)
            : isLeaf(leaf),
            keyCount(0),
            parentPage(-1),
            nextLeafPage(-1),
            selfPage(-1)
        {
            std::memset(keys, 0, sizeof(keys));
            std::fill(children, children + ORDER + 1, -1L);
        }
    };

    /// filename: path to the index file (e.g. "Tables/myTable/id.idx")
    /// bm: reference to the global BufferManager.
    explicit BPlusTree(const std::string& filename, BufferManager& bm);
    ~BPlusTree();

    /// Insert a (key → recordOffset) pair into the B+ Tree.
    void insert(const std::string& key, long recordOffset);

    /// Search for an exact key; if found, recordOffset is set and returns true.
    bool search(const std::string& key, long& recordOffset);

    /// Remove an exact key from the B+ Tree. Returns true if found & removed.
    bool remove(const std::string& key);

    /// Find all recordOffsets whose key lies in [startKey, endKey], inclusive.
    void rangeSearch(const std::string& startKey,
        const std::string& endKey,
        std::vector<long>& outOffsets);

private:
    std::string  filePath;      // e.g. "Tables/myTable/id.idx"
    long         pageCount;     // how many 4 KB pages currently in the file
    BufferManager& bufMgr;      // reference to the buffer manager

    /// Allocate a brand‐new empty node page at the next pageCount index.
    long allocateNode();

    /// Write a Node object into its page in the index file (via buffer).
    void writeNode(const Node& node);

    /// Read the Node at page number 'page' from disk into a Node struct.
    Node readNode(long page);

    /// Recursively insert (key, recordOffset) under 'node'.
    void insertRecursive(Node& node, const std::string& key, long recordOffset);

    /// Split a node that has overflowed (keyCount > ORDER).
    void splitNode(Node& node);

    /// After splitting, insert the middle key into the parent.
    void insertInParent(Node& left, const std::string& key, Node& right);

    /// Recursively delete a key from subtree rooted at 'node'.
    void deleteRecursive(Node& node, const std::string& key);

    /// Merge two siblings at index 'index' under 'parent'.
    void mergeNodes(Node& parent, int index);

    /// Redistribute keys/children between siblings if one is underfull.
    void redistribute(Node& parent, int index);
};
