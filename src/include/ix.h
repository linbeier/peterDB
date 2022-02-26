#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute
#include <iostream>

# define IX_EOF (-1)  // end of the index scan

namespace PeterDB {

    //only use for int and float
    template<class T>
    struct ChildEntry {
        T key;
        unsigned newPageNum;
    };

    template<class T>
    struct Entry {
        T key;
        RID rid;
    };

    struct ChildEntryStr {
        char *key;
        unsigned newPageNum;
    };

    struct EntryStr {
        char *key;
        RID rid;
    };

    class IX_ScanIterator;

    class IXFileHandle;


    class IndexManager {
        PagedFileManager *pm;

    public:
        const std::string keys;
        const std::string children;

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

//        RC insertDummyNode(FILE *fd);

        RC readHiddenPage(FILE *fd, FileHandle &fileHandle);

        RC readDummyNode(FILE *fd, unsigned &root);

        RC writeHiddenPage(FileHandle &fileHandle, unsigned root);

//        RC writeDummyNode(IXFileHandle &fileHandle);

        RC insertNewIndexPage(IXFileHandle &fh, unsigned &pageNum, bool isLeafNode);

        RC findKeyInLeaf(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum,
                         unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive);

        RC treeSearch(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum);

        RC
        leafSearch(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum,
                   unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive);

        template<class T>
        RC putChildEntry(IXFileHandle &fh, char *pageBuffer, ChildEntry<T> *newChildEntry, unsigned entryLen);

        RC putChildEntryStr(IXFileHandle &fh, char *pageBuffer, ChildEntryStr *newChildEntry, unsigned entryLen);

        template<class T>
        RC putLeafEntry(IXFileHandle &fh, char *pageBuffer, Entry<T> *entry, unsigned entryLen);

        RC putLeafEntryStr(IXFileHandle &fh, char *pageBuffer, EntryStr *entry, unsigned int entryLen);

        //get smallest key in page ,form with pageNum
        template<class T>
        RC formChildEntry(IXFileHandle &fh, unsigned pageNum, ChildEntry<T> *newChildEntry,
                          std::vector<ChildEntry<T> *> &vec);

        RC formChildEntryStr(IXFileHandle &fh, unsigned int newPageNum, ChildEntryStr *newChildEntry,
                             std::vector<ChildEntryStr *> &vec);

        template<class T>
        RC formLeafEntry(IXFileHandle &fh, unsigned int pageNum, Entry<T> *newEntry,
                         std::vector<Entry<T> *> &vec);

        RC formLeafEntryStr(IXFileHandle &fh, unsigned int pageNum, EntryStr *newEntry,
                            std::vector<EntryStr *> &vec);

        //split pageNum , insert a new page
        template<class T>
        RC splitIndexPage(IXFileHandle &fh, unsigned tarPage, unsigned &newPage, ChildEntry<T> *newChildEntry);

        RC splitIndexPageStr(IXFileHandle &fh, unsigned int tarPage, unsigned int &newPage,
                             ChildEntryStr *newChildEntry);

        template<class T>
        RC splitLeafPage(IXFileHandle &fh, unsigned tarPage, unsigned &newPage, Entry<T> *newEntry,
                         ChildEntry<T> *newChildEntry);

        RC splitLeafPageStr(IXFileHandle &fh, unsigned int tarPage, unsigned int &newPage,
                            EntryStr *newEntry, ChildEntryStr *newChildEntry);

        template<class T>
        RC getIndexKey(IXFileHandle &fh, const char *pageBuffer, unsigned short keyIndex, T &key);

        template<class T>
        RC getLeafKey(IXFileHandle &fh, const char *pageBuffer, unsigned short keyIndex, T &key);

        template<class T>
        RC recurInsertEntry(IXFileHandle &fh, unsigned nodePage, Entry<T> *entry, ChildEntry<T> *newChildEntry);

        RC recurInsertEntryStr(IXFileHandle &fh, unsigned int nodePage, EntryStr *entry,
                               ChildEntryStr *newChildEntry);

        template<class T>
        RC checkLeafKeys(IXFileHandle &fh, const char *pageBuffer,
                         const void *lowKey, unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive);

        RC checkLeafKeysStr(IXFileHandle &fh, const char *pageBuffer, const void *lowKey, unsigned &keyIndex,
                            bool &noMatchKey, bool lowKeyInclusive);

        template<class T>
        RC checkIndexKeys(IXFileHandle &fh, const char *pageBuffer, const void *lowKey, unsigned &pageNum);

        RC checkIndexKeysStr(IXFileHandle &fh, const char *pageBuffer, const void *lowKey, unsigned &pageNum);

        template<class T>
        RC delChildEntry(IXFileHandle &fh, char *pageBuffer, ChildEntry<T> *newChildEntry, unsigned entryLen);

        template<class T>
        RC delLeafEntry(IXFileHandle &fh, char *pageBuffer, Entry<T> *entry, unsigned entryLen);

    protected:
        IndexManager() : pm(&PagedFileManager::instance()), keys("\"keys\""), children("\"children\"") {

        }                                                  // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:
        bool closed;

        IXFileHandle *ixFileHandle;
        Attribute attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        bool noMatchedKey;

        unsigned pageIndex;
        unsigned keyIndex;

        bool to_very_left;
        bool to_very_right;

        template<class T>
        RC getRIDviaIndex(RID &rid, void *key);

        RC getRIDviaIndexStr(RID &rid, void *key);

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
