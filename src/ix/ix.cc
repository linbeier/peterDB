#include "src/include/ix.h"

namespace PeterDB {
    bool checkLeafNode(const char *pageBuffer);

    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    RC IndexManager::createFile(const std::string &fileName) {
        if (pm->is_file_exist(fileName.c_str())) {

            return RC::CREA_FILE_FAIL;
        } else {
            FILE *fd = fopen(fileName.c_str(), "wb");

            if (fd == nullptr) {
                return RC::CREA_FILE_FAIL;
            }
            insertHiddenPage(fd);
//            insertDummyNode(fd);

            fclose(fd);
        }

        return RC::ok;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        if (!pm->is_file_exist(fileName.c_str())) {
            return RC::REMV_FILE_FAIL;
        } else {
            const int result = remove(fileName.c_str());
            if (result != 0) {
                return RC::REMV_FILE_FAIL;
            }
        }
        return RC::ok;
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        if (!pm->is_file_exist(fileName.c_str())) {
            return RC::OPEN_FILE_FAIL;
        } else if (ixFileHandle.fileHandle.fd != nullptr) {
            return RC::OPEN_FILE_FAIL;
        } else {
            FILE *fd = fopen(fileName.c_str(), "r+b");
            readHiddenPage(fd, ixFileHandle.fileHandle);
            readDummyNode(fd, ixFileHandle.rootPage);

        }
        return RC::ok;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if (ixFileHandle.fileHandle.fd == nullptr) {
            return RC::FD_FAIL;
        } else {
            writeHiddenPage(ixFileHandle.fileHandle, ixFileHandle.rootPage);
//            writeDummyNode(ixFileHandle);
            fclose(ixFileHandle.fileHandle.fd);
            ixFileHandle.fileHandle.fd = nullptr;
        }
        return RC::ok;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {

        if (attribute.type == TypeReal) {
            auto *entry = new Entry<float>;
            memcpy(&entry->key, key, sizeof(int));
            entry->rid = rid;
            ChildEntry<float> *childEntry = nullptr;
            recurInsertEntry<float>(ixFileHandle, ixFileHandle.rootPage, entry, childEntry);
        } else if (attribute.type == TypeInt) {
            auto *entry = new Entry<int>;
            entry->key = *(int *) key;
            entry->rid = rid;
            ChildEntry<int> *childEntry = nullptr;
            recurInsertEntry<int>(ixFileHandle, ixFileHandle.rootPage, entry, childEntry);
        } else {
            auto *entry = new Entry<char *>;
            entry->key = (char *) key;
            entry->rid = rid;
            ChildEntry<char *> *childEntry = nullptr;
            recurInsertEntry<char *>(ixFileHandle, ixFileHandle.rootPage, entry, childEntry);
        }
        return RC::ok;
    }

    template<class T>
    RC IndexManager::recurInsertEntry(IXFileHandle &fh, unsigned int nodePage, Entry<T> *entry,
                                      ChildEntry<T> *newChildEntry) {
        char *pageBuffer = new char[PAGE_SIZE];
        fh.fileHandle.readPage(nodePage, pageBuffer);
        if (!checkLeafNode(pageBuffer)) {
            unsigned keyPage = 0;
            checkIndexKeys<T>(fh, pageBuffer, (T *) entry->key, keyPage);
        }
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return RC::ok;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        ix_ScanIterator.ixFileHandle = &ixFileHandle;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.lowKey = lowKey;
        ix_ScanIterator.highKey = highKey;
        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.pageIndex = 0;
        ix_ScanIterator.keyIndex = 0;

        if (lowKey == nullptr) {
            ix_ScanIterator.to_very_left = true;
        } else {
            ix_ScanIterator.to_very_left = false;
        }
        if (highKey == nullptr) {
            ix_ScanIterator.to_very_right = true;
        } else {
            ix_ScanIterator.to_very_right = false;
        }

        RC re = findKeyInLeaf(*(ix_ScanIterator.ixFileHandle), ix_ScanIterator.attribute,
                              ix_ScanIterator.lowKey, ix_ScanIterator.pageIndex,
                              ix_ScanIterator.keyIndex, ix_ScanIterator.noMatchedKey,
                              ix_ScanIterator.lowKeyInclusive);
        if (re != RC::ok) {
            return RC::RM_EOF;
        }

        return RC::ok;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() : ixFileHandle(nullptr), lowKey(nullptr), highKey(nullptr), closed(false),
                                         noMatchedKey(false) {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if (closed || noMatchedKey) {
            return RC::RM_EOF;
        }
        if (attribute.type == TypeInt) {
            return getRIDviaIndex<int>(rid, key);
        } else if (attribute.type == TypeReal) {
            return getRIDviaIndex<float>(rid, key);
        } else {
            return getRIDviaIndex<char *>(rid, key);
        }
        return RC::ok;
    }

    RC IX_ScanIterator::close() {
        closed = true;
        return RC::ok;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;

        rootPage = 0;
        isKeyFixed = true;
        fileHandle = FileHandle();
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return RC::ok;
    }

} // namespace PeterDB