#include "src/include/ix.h"

namespace PeterDB {
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
            insertDummyNode(fd);

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
            writeHiddenPage(ixFileHandle.fileHandle);
            writeDummyNode(ixFileHandle);
            fclose(ixFileHandle.fileHandle.fd);
            ixFileHandle.fileHandle.fd = nullptr;
        }
        return RC::ok;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        return -1;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        return -1;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
    }

    IX_ScanIterator::IX_ScanIterator() {
    }

    IX_ScanIterator::~IX_ScanIterator() {
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        return -1;
    }

    RC IX_ScanIterator::close() {
        return -1;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;

        rootPage = 0;
        fileHandle = FileHandle();
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        return -1;
    }

} // namespace PeterDB