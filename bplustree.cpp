#include "BPlusTree.h"

#include <fstream>
#include <iostream>
#include <cstring>
#include <algorithm>

// ———————————————————————————————————————————————————————————————————————————————
// Constructor & Destructor
// ———————————————————————————————————————————————————————————————————————————————

BPlusTree::BPlusTree(const std::string& filename, BufferManager& bm)
    : filePath(filename),
    bufMgr(bm),
    pageCount(0)
{
    // On construction, determine how many pages currently exist in the file.
    std::ifstream in(filePath, std::ios::binary | std::ios::ate);
    if (!in) {
        // File doesn’t exist yet ? create it empty:
        std::ofstream create(filePath, std::ios::binary);
        create.close();
        pageCount = 0;
    }
    else {
        long size = in.tellg();
        pageCount = (size + PAGE_SIZE - 1) / PAGE_SIZE;
    }
}

BPlusTree::~BPlusTree() {
    // Nothing special: buffer manager will flush dirty pages at shutdown if needed.
}

// ———————————————————————————————————————————————————————————————————————————————
// allocateNode: create a new empty node page on disk (via buffer)
// ———————————————————————————————————————————————————————————————————————————————

long BPlusTree::allocateNode() {
    long newPage = pageCount;
    Node n;
    n.selfPage = newPage;
    n.isLeaf = true;
    n.keyCount = 0;
    n.parentPage = -1;
    n.nextLeafPage = -1;
    std::memset(n.keys, 0, sizeof(n.keys));
    std::fill(n.children, n.children + ORDER + 1, -1L);

    // Write this empty node to its page via buffer:
    writeNode(n);

    // Expand fileCount:
    ++pageCount;
    return newPage;
}

// ———————————————————————————————————————————————————————————————————————————————
// writeNode: serialize a Node object into its 4 KB buffer, then unpin dirty
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::writeNode(const Node& node) {
    // 1) Pin the page in the INDEX partition:
    char* pageBuf = bufMgr.getPage(filePath,
        static_cast<uint32_t>(node.selfPage),
        PageType::INDEX);
    if (!pageBuf) {
        std::cerr << "[BPlusTree] writeNode: cannot pin page " << node.selfPage << "\n";
        return;
    }

    // 2) Zero?fill the entire 4 KB page buffer first:
    std::memset(pageBuf, 0, PAGE_SIZE);

    // 3) Copy header fields into buffer at offset 0:
    std::size_t offset = 0;
    std::memcpy(pageBuf + offset, &node.isLeaf, sizeof(node.isLeaf));
    offset += sizeof(node.isLeaf);

    std::memcpy(pageBuf + offset, &node.keyCount, sizeof(node.keyCount));
    offset += sizeof(node.keyCount);

    std::memcpy(pageBuf + offset, &node.parentPage, sizeof(node.parentPage));
    offset += sizeof(node.parentPage);

    std::memcpy(pageBuf + offset, &node.nextLeafPage, sizeof(node.nextLeafPage));
    offset += sizeof(node.nextLeafPage);

    // 4) Copy the entire keys[][] array:
    std::memcpy(pageBuf + offset, node.keys, sizeof(node.keys));
    offset += sizeof(node.keys);

    // 5) Copy the entire children[] array:
    std::memcpy(pageBuf + offset, node.children, sizeof(node.children));
    // (offset += sizeof(node.children); // not needed further)

    // 6) Unpin, marking dirty so buffer will schedule a write later:
    bufMgr.unpinPage(filePath,
        static_cast<uint32_t>(node.selfPage),
        PageType::INDEX,
        /*isDirty=*/true);
}

// ———————————————————————————————————————————————————————————————————————————————
// readNode: fetch a 4 KB page from buffer (loading from disk if needed) and
//           deserialize into a Node struct
// ———————————————————————————————————————————————————————————————————————————————

BPlusTree::Node BPlusTree::readNode(long page) {
    Node node;
    // 1) Pin the page in INDEX partition:
    char* pageBuf = bufMgr.getPage(filePath,
        static_cast<uint32_t>(page),
        PageType::INDEX);
    if (!pageBuf) {
        std::cerr << "[BPlusTree] readNode: cannot pin page " << page << "\n";
        return node; // returns an empty node (undefined)
    }

    // 2) Deserialize header:
    std::size_t offset = 0;
    std::memcpy(&node.isLeaf, pageBuf + offset, sizeof(node.isLeaf));
    offset += sizeof(node.isLeaf);

    std::memcpy(&node.keyCount, pageBuf + offset, sizeof(node.keyCount));
    offset += sizeof(node.keyCount);

    std::memcpy(&node.parentPage, pageBuf + offset, sizeof(node.parentPage));
    offset += sizeof(node.parentPage);

    std::memcpy(&node.nextLeafPage, pageBuf + offset, sizeof(node.nextLeafPage));
    offset += sizeof(node.nextLeafPage);

    // 3) Deserialize keys[][]:
    std::memcpy(node.keys, pageBuf + offset, sizeof(node.keys));
    offset += sizeof(node.keys);

    // 4) Deserialize children[]:
    std::memcpy(node.children, pageBuf + offset, sizeof(node.children));
    offset += sizeof(node.children);

    node.selfPage = page;

    // 5) Unpin without marking dirty (read?only):
    bufMgr.unpinPage(filePath,
        static_cast<uint32_t>(page),
        PageType::INDEX,
        /*isDirty=*/false);

    return node;
}

// ———————————————————————————————————————————————————————————————————————————————
// insert: public entry point
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::insert(const std::string& key, long recordOffset) {
    if (pageCount == 0) {
        // Empty file ? create root
        long rootPage = allocateNode();
        (void)rootPage;
    }
    Node root = readNode(0);
    insertRecursive(root, key, recordOffset);
}

// ———————————————————————————————————————————————————————————————————————————————
// search: standard B+ Tree lookup
// ———————————————————————————————————————————————————————————————————————————————

bool BPlusTree::search(const std::string& key, long& recordOffset) {
    if (pageCount == 0) return false;
    long page = 0;  // start at root

    while (true) {
        Node node = readNode(page);
        int i = 0;
        // find first key ? desired
        while (i < node.keyCount && key > std::string(node.keys[i])) {
            ++i;
        }
        if (node.isLeaf) {
            if (i < node.keyCount && key == std::string(node.keys[i])) {
                recordOffset = node.children[i];
                return true;
            }
            return false;
        }
        // Not leaf ? descend into child[i]
        page = node.children[i];
    }
}

// ———————————————————————————————————————————————————————————————————————————————
// insertRecursive: insert into a node’s subtree
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::insertRecursive(Node& node,
    const std::string& key,
    long recordOffset)
{
    if (node.isLeaf) {
        // 1) Find position to insert (sorted)
        int pos = std::lower_bound(
            &node.keys[0],
            &node.keys[node.keyCount],
            key,
            [](const char a[KEY_SIZE], const std::string& k) {
                return std::string(a) < k;
            }
        ) - &node.keys[0];

        // 2) Shift keys/children right
        for (int j = node.keyCount; j > pos; --j) {
            std::memcpy(node.keys[j], node.keys[j - 1], KEY_SIZE);
            node.children[j] = node.children[j - 1];
        }

        // 3) Copy new key into keys[pos]
        size_t len = std::min(key.size(), static_cast<size_t>(KEY_SIZE - 1));
        std::memcpy(node.keys[pos], key.data(), len);
        node.keys[pos][len] = '\0';

        // 4) Insert recordOffset
        node.children[pos] = recordOffset;
        node.keyCount++;

        // 5) Write back this node
        writeNode(node);
    }
    else {
        // Not leaf: find child to descend
        int pos = std::lower_bound(
            &node.keys[0],
            &node.keys[node.keyCount],
            key,
            [](const char a[KEY_SIZE], const std::string& k) {
                return std::string(a) < k;
            }
        ) - &node.keys[0];

        Node child = readNode(node.children[pos]);
        insertRecursive(child, key, recordOffset);
    }

    // If overflowed, split
    if (node.keyCount > ORDER) {
        splitNode(node);
    }
}

// ———————————————————————————————————————————————————————————————————————————————
// splitNode: when node.keyCount > ORDER, split into two pages
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::splitNode(Node& node) {
    // 1) Create right sibling
    Node right(node.isLeaf);
    right.selfPage = allocateNode();
    right.parentPage = node.parentPage;

    int mid = node.keyCount / 2;

    // 2) Move keys[mid..end) & children into right
    for (int i = mid; i < node.keyCount; ++i) {
        std::memcpy(right.keys[i - mid], node.keys[i], KEY_SIZE);
        right.children[i - mid + (node.isLeaf ? 0 : 1)] =
            node.children[i + (node.isLeaf ? 0 : 1)];
    }
    right.keyCount = node.keyCount - mid;
    node.keyCount = mid;

    // 3) If leaf, fix next?leaf pointers
    if (node.isLeaf) {
        right.nextLeafPage = node.nextLeafPage;
        node.nextLeafPage = right.selfPage;
    }

    // 4) Write both pages out
    writeNode(node);
    writeNode(right);

    // 5) Promote the first key of right to parent
    std::string promoteKey(right.keys[0]);

    if (node.selfPage == 0) {
        // Node was root ? create a new root
        Node newRoot(false);
        newRoot.selfPage = allocateNode();
        newRoot.children[0] = node.selfPage;
        newRoot.children[1] = right.selfPage;

        size_t len = std::min(promoteKey.size(), static_cast<size_t>(KEY_SIZE - 1));
        std::memcpy(newRoot.keys[0], promoteKey.data(), len);
        newRoot.keys[0][len] = '\0';
        newRoot.keyCount = 1;

        node.parentPage = newRoot.selfPage;
        right.parentPage = newRoot.selfPage;

        writeNode(node);
        writeNode(right);
        writeNode(newRoot);
    }
    else {
        // Insert promoteKey into parent
        insertInParent(node, promoteKey, right);
    }
}

// ———————————————————————————————————————————————————————————————————————————————
// insertInParent: after splitting child into (left=node, right), push key into parent
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::insertInParent(Node& left,
    const std::string& key,
    Node& right)
{
    // 1) Read parent
    Node parent = readNode(left.parentPage);

    // 2) Find position in parent to insert key
    int pos = 0;
    while (pos <= parent.keyCount && parent.children[pos] != left.selfPage) {
        ++pos;
    }

    // 3) Shift keys/children right
    for (int i = parent.keyCount; i > pos; --i) {
        std::memcpy(parent.keys[i], parent.keys[i - 1], KEY_SIZE);
        parent.children[i + 1] = parent.children[i];
    }

    // 4) Copy the new key
    size_t len = std::min(key.size(), static_cast<size_t>(KEY_SIZE - 1));
    std::memcpy(parent.keys[pos], key.data(), len);
    parent.keys[pos][len] = '\0';
    parent.children[pos + 1] = right.selfPage;
    parent.keyCount++;

    // 5) Update right's parent pointer
    right.parentPage = parent.selfPage;

    writeNode(parent);
    writeNode(right);

    if (parent.keyCount > ORDER) {
        splitNode(parent);
    }
}

// ———————————————————————————————————————————————————————————————————————————————
// remove: public entry point for deletion
// ———————————————————————————————————————————————————————————————————————————————

bool BPlusTree::remove(const std::string& key) {
    if (pageCount == 0) return false;
    Node root = readNode(0);
    deleteRecursive(root, key);
    return true;
}

// ———————————————————————————————————————————————————————————————————————————————
// deleteRecursive: delete key from subtree under 'node'
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::deleteRecursive(Node& node, const std::string& key) {
    if (node.isLeaf) {
        // 1) Find key in this leaf
        int pos = 0;
        while (pos < node.keyCount && key != std::string(node.keys[pos])) ++pos;
        if (pos == node.keyCount) return;  // not found

        // 2) Shift keys/children left
        for (int i = pos; i < node.keyCount - 1; ++i) {
            std::memcpy(node.keys[i], node.keys[i + 1], KEY_SIZE);
            node.children[i] = node.children[i + 1];
        }
        node.keyCount--;

        writeNode(node);
    }
    else {
        // 1) Find child to recurse
        int pos = 0;
        while (pos < node.keyCount && key > std::string(node.keys[pos])) ++pos;
        Node child = readNode(node.children[pos]);
        deleteRecursive(child, key);

        // 2) After recursion, check if child is underfull
        child = readNode(node.children[pos]);
        if (child.keyCount < (ORDER + 1) / 2) {
            mergeNodes(node, pos);
        }
    }
}

// ———————————————————————————————————————————————————————————————————————————————
// mergeNodes: merge child at index `index` with its right sibling under `parent`
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::mergeNodes(Node& parent, int index) {
    Node left = readNode(parent.children[index]);
    Node right = readNode(parent.children[index + 1]);

    int start = left.keyCount;
    for (int i = 0; i < right.keyCount; ++i) {
        std::memcpy(left.keys[start + i], right.keys[i], KEY_SIZE);
        left.children[start + i + (left.isLeaf ? 0 : 1)] =
            right.children[i + (left.isLeaf ? 0 : 1)];
    }
    left.keyCount += right.keyCount;

    if (left.isLeaf) {
        left.nextLeafPage = right.nextLeafPage;
    }
    writeNode(left);

    // Remove key+pointer from parent
    for (int i = index; i < parent.keyCount - 1; ++i) {
        std::memcpy(parent.keys[i], parent.keys[i + 1], KEY_SIZE);
        parent.children[i + 1] = parent.children[i + 2];
    }
    parent.keyCount--;
    writeNode(parent);

    // (Optional) You might free the `right` page on disk; omitted here for simplicity.
}

// ———————————————————————————————————————————————————————————————————————————————
// redistribute: (left as optional; not used in this code) 
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::redistribute(Node& parent, int index) {
    // Left empty. Redistribution (borrowing) logic can be added here if desired.
}

// ———————————————————————————————————————————————————————————————————————————————
// rangeSearch: collect all offsets for keys in [startKey..endKey]
// ———————————————————————————————————————————————————————————————————————————————

void BPlusTree::rangeSearch(const std::string& startKey,
    const std::string& endKey,
    std::vector<long>& outOffsets)
{
    if (pageCount == 0) return;

    long page = 0;
    // 1) If startKey is non?empty, descend to leaf that should contain it
    if (!startKey.empty()) {
        while (true) {
            Node node = readNode(page);
            int i = 0;
            while (i < node.keyCount && startKey > std::string(node.keys[i])) ++i;
            if (node.isLeaf) {
                page = node.selfPage;
                break;
            }
            page = node.children[i];
        }
    }
    else {
        // If no startKey, go to leftmost leaf
        while (true) {
            Node node = readNode(page);
            if (node.isLeaf) {
                break;
            }
            page = node.children[0];
        }
    }

    // 2) Now scan leaf pages until key > endKey (or no more leaves)
    while (page != -1) {
        Node leaf = readNode(page);
        int i = 0;
        if (!startKey.empty()) {
            while (i < leaf.keyCount && startKey > std::string(leaf.keys[i])) ++i;
        }
        for (; i < leaf.keyCount; ++i) {
            std::string curKey(leaf.keys[i]);
            if (!endKey.empty() && curKey > endKey) {
                return;
            }
            outOffsets.push_back(leaf.children[i]);
        }
        page = leaf.nextLeafPage;
    }
}
