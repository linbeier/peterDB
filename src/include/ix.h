#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute
#include <iostream>

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {
    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {
        PagedFileManager *pm;

    public:
        static IndexManager &instance();

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

        //tools
        RC insertHiddenPage(FILE *fd);

        RC insertDummyNode(FILE *fd);

        RC readHiddenPage(FILE *fd, FileHandle &fileHandle);

        RC readDummyNode(FILE *fd, unsigned &root);

        RC writeHiddenPage(FileHandle &fileHandle);

        RC writeDummyNode(IXFileHandle &fileHandle);

        RC insertNewIndexPage(IXFileHandle &fh, unsigned &pageNum, bool isLeafNode, bool isKeyFixed);

        RC findKeyInLeaf(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum);

        RC treeSearch(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum);

        template<class T>
        RC checkIndexKeys(IXFileHandle &fh, const char *pageBuffer, const Attribute &attribute, const void *lowKey,
                          unsigned &pageNum);

        template<class T>
        RC checkLeafKeys(IXFileHandle &fh, const char *pageBuffer, const Attribute &attribute, const void *lowKey,
                         unsigned &pageNum);

    protected:
        IndexManager() : pm(&PagedFileManager::instance()) {

        }                                                  // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        //root page start from 1, 0 indicates dummy root
        unsigned rootPage;
        bool isKeyFixed;
        FileHandle fileHandle;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };
}// namespace PeterDB
#endif // _ix_h_
