#include "src/include/ix.h"
#include "src/ix/tools.cpp"

namespace PeterDB {
//    bool checkLeafNode(const char *pageBuffer);
//
//    template<class T>
//    unsigned getChildEntryLen(ChildEntry<T> *newChildEntry, bool isKeyFixed);
//
//    template<class T>
//    unsigned getEntryLen(Entry<T> *newEntry, bool isKeyFixed);
//
//    unsigned short getFreeSpace(const char *pageBuffer);

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
            ixFileHandle.fileName = fileName;
        }
        return RC::ok;
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        if (ixFileHandle.fileHandle.fd == nullptr) {
            return RC::FD_FAIL;
        } else {
            writeHiddenPage(ixFileHandle.fileHandle, ixFileHandle.rootPage);
            writeDummyNode(ixFileHandle);
            fclose(ixFileHandle.fileHandle.fd);
            ixFileHandle.fileHandle.fd = nullptr;
            ixFileHandle.fileName = "";
        }
        return RC::ok;
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {

        if (ixFileHandle.fileHandle.totalPage == 0) {
            insertDummyNode(ixFileHandle.fileHandle.fd, ixFileHandle.fileHandle);
            readDummyNode(ixFileHandle.fileHandle.fd, ixFileHandle.rootPage);
            unsigned tempPageNum = 0;
            insertNewIndexPage(ixFileHandle, tempPageNum, true);
        }
        if (attribute.type == TypeReal) {
            ixFileHandle.isKeyFixed = true;
            auto *entry = new Entry<float>;
            entry->key = *(float *) key;
            entry->rid = rid;
            ChildEntry<float> *childEntry = nullptr;
            recurInsertEntry<float>(ixFileHandle, ixFileHandle.rootPage, entry, childEntry);
        } else if (attribute.type == TypeInt) {
            ixFileHandle.isKeyFixed = true;
            auto *entry = new Entry<int>;
            entry->key = *(int *) key;
            entry->rid = rid;
            ChildEntry<int> *childEntry = nullptr;
            recurInsertEntry<int>(ixFileHandle, ixFileHandle.rootPage, entry, childEntry);
        } else {
            ixFileHandle.isKeyFixed = false;
            auto *entry = new EntryStr;
            entry->key = (char *) key;
            entry->rid = rid;
            ChildEntryStr *childEntry = nullptr;
            recurInsertEntryStr(ixFileHandle, ixFileHandle.rootPage, entry, childEntry);
        }
        return RC::ok;
    }

    template<class T>
    RC IndexManager::recurInsertEntry(IXFileHandle &fh, unsigned int nodePage, Entry<T> *entry,
                                      ChildEntry<T> *&newChildEntry) {
        if (fh.fileHandle.totalPage == 0) {
            unsigned tempPageNum = 0;
            insertNewIndexPage(fh, tempPageNum, true);
        }
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(nodePage, pageBuffer);
        if (!checkLeafNode(pageBuffer)) {
            unsigned keyPage = 0;
            checkIndexKeys<T>(fh, pageBuffer, &entry->key, keyPage);
            recurInsertEntry(fh, keyPage, entry, newChildEntry);
            fh.fileHandle.readPage(nodePage, pageBuffer);
            if (newChildEntry == nullptr) {
                return RC::ok;
            } else {
                //child has split, need to process
                unsigned entryLen = getChildEntryLen(newChildEntry);
                unsigned freeSpace = getFreeSpace(pageBuffer);
                if (freeSpace >= entryLen) {
                    putChildEntry(fh, pageBuffer, newChildEntry, entryLen);
                    delete newChildEntry;
                    newChildEntry = nullptr;
                    fh.fileHandle.writePage(nodePage, pageBuffer);
                    return RC::ok;
                } else {
                    unsigned newPage = 0;
                    splitIndexPage(fh, nodePage, newPage, newChildEntry);
                    unsigned childEntryLen = getChildEntryLen(newChildEntry);
                    if (nodePage == fh.rootPage) {
//                        std::vector<ChildEntry<T>*> vec;
//                        formChildEntry(fh, nodePage, newChildEntry, sortedVec);
                        unsigned newRootPage = 0;
                        insertNewIndexPage(fh, newRootPage, false);
                        char newRootBuffer[PAGE_SIZE];
                        fh.fileHandle.readPage(newRootPage, newRootBuffer);
                        memcpy(newRootBuffer, &nodePage, sizeof(int));
                        putChildEntry(fh, newRootBuffer, newChildEntry, childEntryLen);
                        delete newChildEntry;
                        newChildEntry = nullptr;

                        fh.fileHandle.writePage(newRootPage, newRootBuffer);
                        fh.rootPage = newRootPage;
                        writeDummyNode(fh);
                    }
                    return RC::ok;
                }
            }
        }
//        fh.fileHandle.readPage(nodePage, pageBuffer);
        else if (checkLeafNode(pageBuffer)) {
            unsigned freeSpace = getFreeSpace(pageBuffer);
            unsigned entryLen = getEntryLen(entry);
            if (freeSpace >= entryLen) {
                putLeafEntry(fh, pageBuffer, entry, entryLen);
                delete newChildEntry;
                newChildEntry = nullptr;
                fh.fileHandle.writePage(nodePage, pageBuffer);
                return RC::ok;
            } else {
                unsigned newPageNum = 0;
                splitLeafPage(fh, nodePage, newPageNum, entry, newChildEntry);
                if (newChildEntry != nullptr && nodePage == fh.rootPage) {
                    unsigned childEntryLen = getChildEntryLen(newChildEntry);
                    unsigned newRootPage = 0;
                    insertNewIndexPage(fh, newRootPage, false);
                    char newRootBuffer[PAGE_SIZE];
                    fh.fileHandle.readPage(newRootPage, newRootBuffer);
                    memcpy(newRootBuffer, &nodePage, sizeof(int));
                    putChildEntry(fh, newRootBuffer, newChildEntry, childEntryLen);
                    delete newChildEntry;
                    newChildEntry = nullptr;

                    fh.fileHandle.writePage(newRootPage, newRootBuffer);
                    fh.rootPage = newRootPage;
                    writeDummyNode(fh);
                }
                return RC::ok;
            }
        }
    }

    RC IndexManager::recurInsertEntryStr(IXFileHandle &fh, unsigned int nodePage, EntryStr *entry,
                                         ChildEntryStr *&newChildEntry) {
        if (fh.fileHandle.totalPage == 0) {
            unsigned tempPageNum = 0;
            insertNewIndexPage(fh, tempPageNum, true);
        }
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(nodePage, pageBuffer);
        if (!checkLeafNode(pageBuffer)) {
            unsigned keyPage = 0;
            checkIndexKeysStr(fh, pageBuffer, entry->key, keyPage);
            recurInsertEntryStr(fh, keyPage, entry, newChildEntry);
            fh.fileHandle.readPage(nodePage, pageBuffer);
            if (newChildEntry == nullptr) {
                return RC::ok;
            } else {
                //child has split, need to process
                unsigned entryLen = getChildEntryLenStr(newChildEntry);
                unsigned freeSpace = getFreeSpace(pageBuffer);
                if (freeSpace >= entryLen) {
                    putChildEntryStr(fh, pageBuffer, newChildEntry, entryLen);
                    delete newChildEntry;
                    newChildEntry = nullptr;
                    fh.fileHandle.writePage(nodePage, pageBuffer);
                    return RC::ok;
                } else {
                    unsigned newPage = 0;
                    splitIndexPageStr(fh, nodePage, newPage, newChildEntry);
                    unsigned childEntryLen = getChildEntryLenStr(newChildEntry);
                    if (nodePage == fh.rootPage) {
                        unsigned newRootPage = 0;
                        insertNewIndexPage(fh, newRootPage, false);
                        char newRootBuffer[PAGE_SIZE];
                        fh.fileHandle.readPage(newRootPage, newRootBuffer);
                        memcpy(newRootBuffer, &nodePage, sizeof(int));
                        putChildEntryStr(fh, newRootBuffer, newChildEntry, childEntryLen);
                        delete newChildEntry;
                        newChildEntry = nullptr;

                        fh.fileHandle.writePage(newRootPage, newRootBuffer);
                        fh.rootPage = newRootPage;
                    }
                    return RC::ok;
                }
            }
        }
        if (checkLeafNode(pageBuffer)) {
            unsigned freeSpace = getFreeSpace(pageBuffer);
            unsigned entryLen = getEntryLenStr(entry);
            if (freeSpace >= entryLen) {
                putLeafEntryStr(fh, pageBuffer, entry, entryLen);
                newChildEntry = nullptr;
                fh.fileHandle.writePage(nodePage, pageBuffer);
                return RC::ok;
            } else {
                unsigned newPageNum = 0;
                splitLeafPageStr(fh, nodePage, newPageNum, entry, newChildEntry);
                if (newChildEntry != nullptr && nodePage == fh.rootPage) {
                    unsigned childEntryLen = getChildEntryLenStr(newChildEntry);
                    unsigned newRootPage = 0;
                    insertNewIndexPage(fh, newRootPage, false);
                    char newRootBuffer[PAGE_SIZE];
                    fh.fileHandle.readPage(newRootPage, newRootBuffer);
                    memcpy(newRootBuffer, &nodePage, sizeof(int));
                    putChildEntryStr(fh, newRootBuffer, newChildEntry, childEntryLen);
                    delete newChildEntry;
                    newChildEntry = nullptr;

                    fh.fileHandle.writePage(newRootPage, newRootBuffer);
                    fh.rootPage = newRootPage;
                    writeDummyNode(fh);
                }
                return RC::ok;
            }
        }
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        ixFileHandle.fileHandle.readPageCounter++;
        return recurDelEntry(ixFileHandle, attribute, ixFileHandle.rootPage, key, rid);
    }

    RC IndexManager::recurDelEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, unsigned int pageNum,
                                   const void *key, const RID &rid) {
        char pageBuffer[PAGE_SIZE];
        ixFileHandle.fileHandle.readPage(pageNum, pageBuffer);
        bool isLeafNode = checkLeafNode(pageBuffer);
        if (!isLeafNode) {
            unsigned nextPageNum = 0;
            if (attribute.type == TypeInt) {
                checkIndexKeys<int>(ixFileHandle, pageBuffer, key, nextPageNum);
                return recurDelEntry(ixFileHandle, attribute, nextPageNum, key, rid);
            } else if (attribute.type == TypeReal) {
                checkIndexKeys<float>(ixFileHandle, pageBuffer, key, nextPageNum);
                return recurDelEntry(ixFileHandle, attribute, nextPageNum, key, rid);
            } else {
                checkIndexKeysStr(ixFileHandle, pageBuffer, key, nextPageNum);
                return recurDelEntry(ixFileHandle, attribute, nextPageNum, key, rid);
            }
        } else {
            if (attribute.type == TypeInt) {
                auto *entry = new Entry<int>;
                entry->key = *(int *) key;
                entry->rid = rid;
                unsigned entryLen = sizeof(int) + sizeof(int) + sizeof(short);
                RC re = delLeafEntry(ixFileHandle, pageBuffer, entry, entryLen);
                if (re != RC::ok) {
                    return re;
                }
                ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
                delete entry;
            } else if (attribute.type == TypeReal) {
                auto *entry = new Entry<float>;
                entry->key = *(float *) key;
                entry->rid = rid;
                unsigned entryLen = sizeof(int) + sizeof(int) + sizeof(short);
                RC re = delLeafEntry(ixFileHandle, pageBuffer, entry, entryLen);
                if (re != RC::ok) {
                    return re;
                }
                ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
                delete entry;
            } else {
                auto *entry = new EntryStr;
                entry->key = (char *) key;
                entry->rid = rid;
                int len = 0;
                memcpy(&len, key, sizeof(int));
                unsigned entryLen = len + sizeof(int) + sizeof(int) + sizeof(short);
                RC re = delLeafEntryStr(ixFileHandle, pageBuffer, entry, entryLen);
                if (re != RC::ok) {
                    return re;
                }
                ixFileHandle.fileHandle.writePage(pageNum, pageBuffer);
                delete entry;
            }
        }
        return RC::ok;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if (ixFileHandle.fileHandle.fd == nullptr || !pm->is_file_exist(ixFileHandle.fileName.c_str())) {
            return RC::RM_EOF;
        }
        ix_ScanIterator.ixFileHandle = &ixFileHandle;
        ix_ScanIterator.attribute = attribute;
        ix_ScanIterator.lowKey = lowKey;
        ix_ScanIterator.highKey = highKey;
        ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;
        ix_ScanIterator.pageIndex = ixFileHandle.rootPage;
        ix_ScanIterator.keyIndex = 0;
        ix_ScanIterator.closed = false;

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

        ix_ScanIterator.pageBuffer = new char[PAGE_SIZE];
        ixFileHandle.fileHandle.readPage(ix_ScanIterator.pageIndex, ix_ScanIterator.pageBuffer);
        if (re != RC::ok) {
            return RC::RM_EOF;
        }

        return RC::ok;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        if (attribute.type == TypeReal) {
            return dfsPrint<float>(ixFileHandle, out, ixFileHandle.rootPage, 0);
        } else if (attribute.type == TypeInt) {
            return dfsPrint<int>(ixFileHandle, out, ixFileHandle.rootPage, 0);
        } else {
            return dfsPrintStr(ixFileHandle, out, ixFileHandle.rootPage, 0);
        }
    }

    template<class T>
    RC IndexManager::dfsPrint(IXFileHandle &ixFileHandle, std::ostream &out, unsigned int currentNode,
                              unsigned int depth) const {
        char pageBuffer[PAGE_SIZE];
        ixFileHandle.fileHandle.readPage(currentNode, pageBuffer);
        bool isLeafNode = checkLeafNode(pageBuffer);

        if (!isLeafNode) {
            for (int i = 0; i < depth; i++) {
                out << "\t";
            }
            out << "{" << keys << ":";
            std::vector<T> keyVec;
            getNodeKeys<T>(ixFileHandle, currentNode, keyVec);
            out << "[";
            for (int i = 0; i < keyVec.size(); i++) {
                out << "\"" << keyVec[i] << "\"";
                if (i != keyVec.size() - 1) {
                    out << ",";
                }
            }
            out << "]," << std::endl;
            for (int i = 0; i < depth; i++) {
                out << "\t";
            }
            out << " " << children << ": [" << std::endl;
            depth++;
            std::vector<RID> pageVec;
            getNodePages<T>(ixFileHandle, currentNode, pageVec);
            for (int i = 0; i < pageVec.size(); ++i) {
                dfsPrint<T>(ixFileHandle, out, pageVec[i].pageNum, depth);
                if (i != pageVec.size() - 1) {
                    out << ",";
                }
            }
            for (int i = 0; i < depth - 1; i++) {
                out << "\t";
            }
            out << "]}" << std::endl;
        } else {
            for (int i = 0; i < depth; i++) {
                out << "\t";
            }
            out << "{" << keys << ": [";
            std::vector<T> keyVec;
            std::vector<RID> ridVec;
            getNodeKeys(ixFileHandle, currentNode, keyVec);
            getNodePages<T>(ixFileHandle, currentNode, ridVec);

            for (int i = 0; i < keyVec.size(); i++) {

                out << "\"" << keyVec[i] << ":[(" << ridVec[i].pageNum << "," << ridVec[i].slotNum << ")]\"";
                if (i != keyVec.size() - 1) {
                    out << ",";
                }
            }

            out << "]}," << std::endl;
        }
        return RC::ok;
    }

    RC IndexManager::dfsPrintStr(IXFileHandle &ixFileHandle, std::ostream &out, unsigned int currentNode,
                                 unsigned int depth) const {
        char pageBuffer[PAGE_SIZE];
        ixFileHandle.fileHandle.readPage(currentNode, pageBuffer);
        bool isLeafNode = checkLeafNode(pageBuffer);

        if (!isLeafNode) {
            for (int i = 0; i < depth; i++) {
                out << "\t";
            }
            out << "{" << keys << ":";
            std::vector<std::string> keyVec;
            getNodeKeysStr(ixFileHandle, currentNode, keyVec);
            out << "[";
            for (int i = 0; i < keyVec.size(); i++) {
                out << "\"" << keyVec[i] << "\"";
                if (i != keyVec.size() - 1) {
                    out << ",";
                }
            }
            out << "]," << std::endl;
            for (int i = 0; i < depth; i++) {
                out << "\t";
            }
            out << " " << children << ": [" << std::endl;
            depth++;
            std::vector<RID> pageVec;
            getNodePagesStr(ixFileHandle, currentNode, pageVec);
            for (int i = 0; i < pageVec.size(); ++i) {
                dfsPrintStr(ixFileHandle, out, pageVec[i].pageNum, depth);
                if (i != pageVec.size() - 1) {
                    out << "," << std::endl;
                }
            }
            for (int i = 0; i < depth - 1; i++) {
                out << "\t";
            }
            out << "]}" << std::endl;
        } else {
            for (int i = 0; i < depth; i++) {
                out << "\t";
            }
            out << "{" << keys << ": [";
            std::vector<std::string> keyVec;
            std::vector<RID> ridVec;
            getNodeKeysStr(ixFileHandle, currentNode, keyVec);
            getNodePagesStr(ixFileHandle, currentNode, ridVec);

            for (int i = 0; i < keyVec.size(); i++) {
                out << "\"" << keyVec[i] << ":[(" << ridVec[i].pageNum << "," << ridVec[i].slotNum << ")]\"";
                if (i != keyVec.size() - 1) {
                    out << ",";
                }
            }

//            out << "]}," << std::endl;
            out << "]}";
        }
        return RC::ok;
    }

    IX_ScanIterator::IX_ScanIterator() : ixFileHandle(nullptr), lowKey(nullptr), highKey(nullptr), closed(false),
                                         noMatchedKey(false), pageBuffer(nullptr) {
    }

    IX_ScanIterator::~IX_ScanIterator() {
        delete (char *) lowKey;
        delete (char *) highKey;
        delete pageBuffer;
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
            return getRIDviaIndexStr(rid, key);
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
        readPageCount = fileHandle.readPageCounter;
        writePageCount = fileHandle.writePageCounter;
        appendPageCount = fileHandle.appendPageCounter;
        return RC::ok;
    }

} // namespace PeterDB