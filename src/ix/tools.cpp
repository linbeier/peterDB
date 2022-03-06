//
// Created by 晓斌 on 2/18/22.
//
#include "src/include/ix.h"

namespace PeterDB {
#define FIXED '\0'
#define UNFIXED '\1'
#define IS_LEAF '\0'
#define IS_BRANCH '\1'
#define NULL_PAGE 4294967295

    unsigned short getKeyNum(const char *pageBuffer) {
        unsigned short keys = 0;
        memcpy(&keys, pageBuffer + PAGE_SIZE - 2, sizeof(short));
        return keys;
    }

    bool checkLeafNode(const char *pageBuffer) {
        unsigned char isLeaf;
        memcpy(&isLeaf, pageBuffer + PAGE_SIZE - 2 - 2 - 1, sizeof(char));
        if (isLeaf == IS_LEAF) {
            return true;
        }
        return false;
    }

    unsigned getNextPage(const char *pageBuffer) {
        unsigned pageNum = 0;
        memcpy(&pageNum, pageBuffer + PAGE_SIZE - 2 - 2 - 1 - 4, sizeof(int));
        return pageNum;
    }

    unsigned getPrevPage(const char *pageBuffer) {
        unsigned prevPage = 0;
        memcpy(&prevPage, pageBuffer + PAGE_SIZE - 2 - 2 - 1 - 4 - 4, sizeof(int));
        return prevPage;
    }

    RC updateNextPage(char *pageBuffer, unsigned nextPage) {
        memcpy(pageBuffer + PAGE_SIZE - 2 - 2 - 1 - 4, &nextPage, sizeof(int));
        return RC::ok;
    }

    RC updatePrevPage(char *pageBuffer, unsigned prevPage) {
        memcpy(pageBuffer + PAGE_SIZE - 2 - 2 - 1 - 4 - 4, &prevPage, sizeof(int));
        return RC::ok;
    }

    unsigned short getFreeSpace(const char *pageBuffer) {
        unsigned short freeSpace = 0;
        memcpy(&freeSpace, pageBuffer + PAGE_SIZE - 2 - 2, sizeof(short));
        return freeSpace;
    }

    template<class T>
    unsigned getChildEntryLen(ChildEntry<T> *newChildEntry) {
        if (newChildEntry == nullptr) {
            return 0;
        }
        return 2 * sizeof(int);
    }

    unsigned getChildEntryLenStr(ChildEntryStr *newChildEntry) {
        if (newChildEntry == nullptr) {
            return 0;
        }
        int len = 0;
        memcpy(&len, newChildEntry->key, sizeof(int));
        return len + 2 * sizeof(int);
    }

    template<class T>
    unsigned getEntryLen(Entry<T> *newEntry) {
        if (newEntry == nullptr) {
            return 0;
        }
        return 2 * sizeof(int) + sizeof(short);
    }

    unsigned getEntryLenStr(EntryStr *newEntry) {
        if (newEntry == nullptr) {
            return 0;
        }
        unsigned len = 0;
        memcpy(&len, newEntry->key, sizeof(int));
        return len + 2 * sizeof(int) + sizeof(short);
    }

    //keyIndex start from 0, total keyNum start from 1
    unsigned getIndexPath(const char *pageBuffer, unsigned keyIndex, bool isKeyFixed) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        unsigned path = sizeof(int);
        for (int i = 0; i < keyIndex; i++) {
            if (isKeyFixed) {
                path += 2 * sizeof(int);
            } else {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += len + sizeof(int) + sizeof(int);
            }
        }
        return path;
    }

    unsigned getLeafPath(const char *pageBuffer, unsigned keyIndex, bool isKeyFixed) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        unsigned path = 0;
        for (int i = 0; i < keyNum; i++) {
            if (isKeyFixed) {
                path += 2 * sizeof(int) + sizeof(short);
            } else {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += len + sizeof(int) + sizeof(int) + sizeof(short);
            }
        }
        return path;
    }

    //check if key1 is bigger than key2
    template<class T>
    bool checkBigger(T &key1, T &key2) {
        return key1 > key2;
    }

    bool checkBiggerStr(char *key1, char *key2) {
        int len1 = 0;
        memcpy(&len1, key1, sizeof(int));
        std::string str1(key1 + sizeof(int), len1);
        int len2 = 0;
        memcpy(&len2, key2, sizeof(int));
        std::string str2(key2 + sizeof(int), len2);
        return str1 > str2;
    }

    template<class T>
    bool checkEqual(T &key1, T &key2) {
        return key1 == key2;
    }

    bool checkEqualStr(char *key1, char *key2) {
        int len1 = 0;
        memcpy(&len1, key1, sizeof(int));
        std::string str1(key1 + sizeof(int), len1);
        int len2 = 0;
        memcpy(&len2, key2, sizeof(int));
        std::string str2(key2 + sizeof(int), len2);
        return str1 == str2;
    }

    RC updateKeyNum(char *pageBuffer, unsigned short keyNum) {
        memcpy(pageBuffer + PAGE_SIZE - sizeof(short), &keyNum, sizeof(short));
        return RC::ok;
    }

    RC updateFreeSpace(char *pageBuffer, unsigned short freeSpace) {
        memcpy(pageBuffer + PAGE_SIZE - sizeof(short) - sizeof(short), &freeSpace, sizeof(short));
        return RC::ok;
    }

    unsigned getIndexTotalLen(const char *pageBuffer, bool isKeyFixed) {
        unsigned keyNum = getKeyNum(pageBuffer);
        return getIndexPath(pageBuffer, keyNum, isKeyFixed);
//        unsigned totalLen = sizeof(int);
//        unsigned keyNum = getKeyNum(pageBuffer);
//        for (int i = 0; i < keyNum; i++) {
//            if (isKeyFixed) {
//                totalLen += 2 * sizeof(int);
//            } else {
//                int len = 0;
//                memcpy(&len, pageBuffer + totalLen, sizeof(int));
//                totalLen += len + sizeof(int) + sizeof(int);
//            }
//        }
//        return totalLen;
    }

    unsigned getLeafTotalLen(const char *pageBuffer, bool isKeyFixed) {
        unsigned keyNum = getKeyNum(pageBuffer);
        return getLeafPath(pageBuffer, keyNum, isKeyFixed);
//        unsigned totalLen = 0;
//        unsigned keyNum = getKeyNum(pageBuffer);
//        for (int i = 0; i < keyNum; i++) {
//            if (isKeyFixed) {
//                totalLen += 2 * sizeof(int) + sizeof(short);
//            } else {
//                int len = 0;
//                memcpy(&len, pageBuffer + totalLen, sizeof(int));
//                totalLen += len + sizeof(int) + sizeof(int) + sizeof(short);
//            }
//        }
//        return totalLen;
    }

    RC IndexManager::insertHiddenPage(FILE *fd) {
        //create hidden page: data = read, write, append counter
        char pagebuffer[PAGE_SIZE];

        int initvalue = 0;
        memcpy(pagebuffer, &initvalue, sizeof(int));
        memcpy(pagebuffer + sizeof(int), &initvalue, sizeof(int));
        memcpy(pagebuffer + 2 * sizeof(int), &initvalue, sizeof(int));
        memcpy(pagebuffer + 3 * sizeof(int), &initvalue, sizeof(int));
        //root page
        memcpy(pagebuffer + 4 * sizeof(int), &initvalue, sizeof(int));
        fseek(fd, 0, SEEK_SET);
        fwrite(pagebuffer, sizeof(char), PAGE_SIZE, fd);
        return RC::ok;
    }

    RC IndexManager::insertDummyNode(FILE *fd, FileHandle &fileHandle) {
        //create hidden page: data = read, write, append counter
        char pagebuffer[PAGE_SIZE];

        int initvalue = 1;
        memcpy(pagebuffer, &initvalue, sizeof(int));
//        fseek(fd, PAGE_SIZE, SEEK_SET);
//        fwrite(pagebuffer, sizeof(char), PAGE_SIZE, fd);
//        fileHandle.appendPageCounter++;
        fileHandle.appendPage(pagebuffer);
        return RC::ok;
    }

    RC IndexManager::readHiddenPage(FILE *fd, FileHandle &fileHandle) {

        if (fd == nullptr) {
            return RC::OPEN_FILE_FAIL;
        }
        char pagebuffer[PAGE_SIZE];
        fseek(fd, 0, SEEK_SET);
        fread(pagebuffer, 1, PAGE_SIZE, fd);
        int readcounter = 0, writecounter = 0, appendcounter = 0, totalPage = 0;
        //maybe just need to init counter and add counter to file when close file?
        memcpy(&readcounter, pagebuffer, sizeof(int));
        memcpy(&writecounter, pagebuffer + sizeof(int), sizeof(int));
        memcpy(&appendcounter, pagebuffer + 2 * sizeof(int), sizeof(int));
        memcpy(&totalPage, pagebuffer + 3 * sizeof(int), sizeof(int));

        fileHandle.readPageCounter = (unsigned int) readcounter + 1;
        fileHandle.writePageCounter = (unsigned int) writecounter;
        fileHandle.appendPageCounter = (unsigned int) appendcounter;
        fileHandle.totalPage = (unsigned int) totalPage;
        fileHandle.fd = fd;

        return RC::ok;
    }

    RC IndexManager::readDummyNode(FILE *fd, unsigned int &root) {

        if (fd == nullptr) {
            return RC::OPEN_FILE_FAIL;
        }
        char pagebuffer[PAGE_SIZE];
        fseek(fd, PAGE_SIZE, SEEK_SET);
        fread(pagebuffer, 1, PAGE_SIZE, fd);

        memcpy(&root, pagebuffer, sizeof(int));

        return RC::ok;
    }

    RC IndexManager::writeHiddenPage(FileHandle &fileHandle, unsigned root) {
        //write back hidden block
        char pagebuffer[PAGE_SIZE];

        memcpy(pagebuffer, &fileHandle.readPageCounter, sizeof(int));
        memcpy(pagebuffer + sizeof(int), &fileHandle.writePageCounter, sizeof(int));
        memcpy(pagebuffer + 2 * sizeof(int), &fileHandle.appendPageCounter, sizeof(int));
        memcpy(pagebuffer + 3 * sizeof(int), &fileHandle.totalPage, sizeof(int));
//        memcpy(pagebuffer + 4 * sizeof(int), &root, sizeof(int));

        fseek(fileHandle.fd, 0, SEEK_SET);
        fwrite(pagebuffer, 1, PAGE_SIZE, fileHandle.fd);

        return RC::ok;
    }

    RC IndexManager::writeDummyNode(IXFileHandle &fileHandle) {
        char pagebuffer[PAGE_SIZE];

        memcpy(pagebuffer, &fileHandle.rootPage, sizeof(int));

//        fseek(fileHandle.fileHandle.fd, PAGE_SIZE, SEEK_SET);
//        fwrite(pagebuffer, 1, PAGE_SIZE, fileHandle.fileHandle.fd);
        fileHandle.fileHandle.writePage(0, pagebuffer);
        return RC::ok;
    }

    RC IndexManager::insertNewIndexPage(IXFileHandle &fh, unsigned int &pageNum, bool isLeafNode) {
        char pageBuffer[PAGE_SIZE];
        unsigned short keyNumber = 0;
        unsigned short freeSpace = 0;
        unsigned char isLeaf = isLeafNode ? IS_LEAF : IS_BRANCH;

        memcpy(pageBuffer + PAGE_SIZE - 2, &keyNumber, sizeof(short));
        memcpy(pageBuffer + PAGE_SIZE - 5, &isLeaf, sizeof(char));

        //extra 8 bytes for prev page and next page
        if (isLeafNode) {
            freeSpace = PAGE_SIZE - 2 - 2 - 1 - 4 - 4;
            unsigned nextPageNum = NULL_PAGE;
            memcpy(pageBuffer + PAGE_SIZE - 4, &freeSpace, sizeof(short));
            memcpy(pageBuffer + PAGE_SIZE - 2 - 2 - 1 - 4, &nextPageNum, sizeof(int));
            memcpy(pageBuffer + PAGE_SIZE - 2 - 2 - 1 - 4 - 4, &nextPageNum, sizeof(int));
        } else {
            freeSpace = PAGE_SIZE - 2 - 2 - 1;
            memcpy(pageBuffer + PAGE_SIZE - 4, &freeSpace, sizeof(short));
        }

        pageNum = fh.fileHandle.totalPage;

        return fh.fileHandle.appendPage(pageBuffer);
    }

    RC
    IndexManager::findKeyInLeaf(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum,
                                unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive) {
        if (attribute.type == TypeReal || attribute.type == TypeInt) {
            fh.isKeyFixed = true;
        }
        pageNum = fh.rootPage;
        RC re = treeSearch(fh, attribute, lowKey, pageNum);
        if (re != RC::ok) {
            return re;
        }
        re = leafSearch(fh, attribute, lowKey, pageNum, keyIndex, noMatchKey, lowKeyInclusive);
        if (re != RC::ok) {
            return re;
        }
        return RC::ok;
    }

    RC IndexManager::treeSearch(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum) {
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(pageNum, pageBuffer);
        if (checkLeafNode(pageBuffer)) {
            return RC::ok;
        }
        if (attribute.type == TypeInt) {
            checkIndexKeys<int>(fh, pageBuffer, lowKey, pageNum);
            return treeSearch(fh, attribute, lowKey, pageNum);
        } else if (attribute.type == TypeReal) {
            checkIndexKeys<float>(fh, pageBuffer, lowKey, pageNum);
            return treeSearch(fh, attribute, lowKey, pageNum);
        } else {
            checkIndexKeysStr(fh, pageBuffer, lowKey, pageNum);
            return treeSearch(fh, attribute, lowKey, pageNum);
        }
    }

    //return pointer to first result matches search
    RC IndexManager::leafSearch(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum,
                                unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive) {
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(pageNum, pageBuffer);
        RC re = RC::ok;
        if (attribute.type == TypeInt) {
            re = checkLeafKeys<int>(fh, pageBuffer, lowKey, keyIndex, noMatchKey, lowKeyInclusive);
        } else if (attribute.type == TypeReal) {
            re = checkLeafKeys<float>(fh, pageBuffer, lowKey, keyIndex, noMatchKey, lowKeyInclusive);
        } else {
            re = checkLeafKeysStr(fh, pageBuffer, lowKey, keyIndex, noMatchKey, lowKeyInclusive);
        }

        if (re != RC::ok) {
            return re;
        }
        return RC::ok;
    }

    template<class T>
    RC IndexManager::checkIndexKeys(IXFileHandle &fh, const char *pageBuffer, const void *lowKey, unsigned &pageNum) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = getKeyNum(pageBuffer);
        bool keyFixed = fh.isKeyFixed;

        if (lowKey == nullptr) {
            memcpy(&pageNum, pageBuffer, sizeof(int));
            return RC::ok;
        }

        T key;

        for (int i = 0; i < keyNum; i++) {
            memcpy(&key, pageBuffer + 2 * sizeof(int) * i + sizeof(int), sizeof(int));
            if (*(T *) lowKey < key) {
                memcpy(&pageNum, pageBuffer + 2 * sizeof(int) * i, sizeof(int));
                return RC::ok;
            }
        }

        memcpy(&pageNum, pageBuffer + 2 * sizeof(int) * keyNum, sizeof(int));
        return RC::ok;
    }

    RC IndexManager::checkIndexKeysStr(IXFileHandle &fh, const char *pageBuffer, const void *lowKey,
                                       unsigned int &pageNum) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = getKeyNum(pageBuffer);
        if (lowKey == nullptr) {
            memcpy(&pageNum, pageBuffer, sizeof(int));
            return RC::ok;
        }

        int path = sizeof(int);
        int lowKeyLen = 0;
        memcpy(&lowKeyLen, lowKey, sizeof(int));
        std::string lowKeyStr((const char *) lowKey + sizeof(int), lowKeyLen);

        for (int i = 0; i < keyNum; i++) {
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            path += sizeof(int);

            std::string pageKey(pageBuffer + path, len);
            path += len;
            //find first key bigger than lowkey return page number
            if (pageKey > lowKeyStr) {
                memcpy(&pageNum, pageBuffer + path - len - 2 * sizeof(int), sizeof(int));
                return RC::ok;
            }

            path += sizeof(int);
        }

        memcpy(&pageNum, pageBuffer + path - sizeof(int), sizeof(int));
        return RC::ok;
    }


    template<class T>
    RC IndexManager::checkLeafKeys(IXFileHandle &fh, const char *pageBuffer,
                                   const void *lowKey, unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = getKeyNum(pageBuffer);
        bool keyFixed = fh.isKeyFixed;
        if (lowKey == nullptr) {
            keyIndex = 0;
            return RC::ok;
        }

        T key;
        for (int i = 0; i < keyNum; i++) {
            memcpy(&key, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * i, sizeof(int));

            if (lowKeyInclusive) {
                if (*(T *) lowKey <= key) {
                    keyIndex = i;
                    return RC::ok;
                }
            } else {
                if (*(T *) lowKey < key) {
                    keyIndex = i;
                    return RC::ok;
                }
            }
        }
        //find nothing matched in leaf nodes
        noMatchKey = true;
        return RC::ok;
    }

    RC IndexManager::checkLeafKeysStr(IXFileHandle &fh, const char *pageBuffer, const void *lowKey,
                                      unsigned int &keyIndex, bool &noMatchKey, bool lowKeyInclusive) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = getKeyNum(pageBuffer);
        bool keyFixed = fh.isKeyFixed;
        if (lowKey == nullptr) {
            keyIndex = 0;
            return RC::ok;
        }

        int path = 0;
        int lowKeyLen = 0;
        memcpy(&lowKeyLen, lowKey, sizeof(int));
        std::string lowKeyStr((const char *) lowKey + sizeof(int), lowKeyLen);

        for (int i = 0; i < keyNum; i++) {
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            path += sizeof(int);

            std::string pageKey(pageBuffer + path, len);
            path += len;
            //find first key equal to lowkey return page number
            if (lowKeyInclusive) {
                if (lowKeyStr <= pageKey) {
                    keyIndex = i;
                    return RC::ok;
                }
            } else {
                if (lowKeyStr < pageKey) {
                    keyIndex = i;
                    return RC::ok;
                }
            }

            path += sizeof(int) + sizeof(short);
        }
        //find nothing matched in leaf nodes
        noMatchKey = true;
        return RC::ok;
    }

    //also check high key
    template<class T>
    RC IX_ScanIterator::getRIDviaIndex(RID &rid, void *key) {
        char pageBuffer[PAGE_SIZE];
        ixFileHandle->fileHandle.readPage(pageIndex, pageBuffer);
        unsigned short keyNum = getKeyNum(pageBuffer);
        if (keyNum == keyIndex) {
            keyIndex = 0;
            pageIndex = getNextPage(pageBuffer);
            if (pageIndex == NULL_PAGE)return RM_EOF;
            return getRIDviaIndex<T>(rid, key);
        }

        memcpy(key, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex, sizeof(int));
        if (highKey == nullptr) {
            memcpy(&rid.pageNum, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex
                                 + sizeof(int), sizeof(int));
            memcpy(&rid.slotNum, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex
                                 + sizeof(int) + sizeof(int), sizeof(short));
            keyIndex++;
            return RC::ok;
        }
        if (highKeyInclusive) {
            if (*(T *) highKey >= *(T *) key) {
                memcpy(&rid.pageNum, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex
                                     + sizeof(int), sizeof(int));
                memcpy(&rid.slotNum, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex
                                     + sizeof(int) + sizeof(int), sizeof(short));
                keyIndex++;
                return RC::ok;
            }
        } else {
            if (*(T *) highKey > *(T *) key) {
                memcpy(&rid.pageNum,
                       pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex + sizeof(int),
                       sizeof(int));
                memcpy(&rid.slotNum, pageBuffer + (sizeof(int) + sizeof(int) + sizeof(short)) * keyIndex
                                     + sizeof(int) + sizeof(int), sizeof(short));
                keyIndex++;
                return RC::ok;
            }
        }
        return RC::RM_EOF;
    }

    RC IX_ScanIterator::getRIDviaIndexStr(RID &rid, void *key) {
        char pageBuffer[PAGE_SIZE];
        ixFileHandle->fileHandle.readPage(pageIndex, pageBuffer);
        unsigned short keyNum = getKeyNum(pageBuffer);
        if (keyNum == keyIndex) {
            keyIndex = 0;
            pageIndex = getNextPage(pageBuffer);
            if (pageIndex == NULL_PAGE)return RM_EOF;
            return getRIDviaIndexStr(rid, key);
        }

        int path = 0;
        int highKeyLen = 0;
        memcpy(&highKeyLen, highKey, sizeof(int));
        std::string highKeyStr((const char *) highKey + sizeof(int), highKeyLen);

        for (int i = 0; i < keyIndex; i++) {
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            path += sizeof(int);
            path += len;
            path += sizeof(int) + sizeof(short);
        }
        //reach keyIndex
        int len = 0;
        memcpy(&len, pageBuffer + path, sizeof(int));
        memcpy(key, pageBuffer + path, len + sizeof(int));
        path += sizeof(int);
        std::string pageKey(pageBuffer + path, len);
        path += len;
        //find first key equal to lowkey return page number
        if (highKey == nullptr) {
            memcpy(&rid.pageNum, pageBuffer + path, sizeof(int));
            memcpy(&rid.slotNum, pageBuffer + path + sizeof(int), sizeof(short));
            keyIndex++;
            return RC::ok;
        }
        if (highKeyInclusive) {
            if (pageKey <= highKeyStr) {
                memcpy(&(rid.pageNum), pageBuffer + path, sizeof(int));
                memcpy(&(rid.slotNum), pageBuffer + path + sizeof(int), sizeof(short));
                keyIndex++;
                return RC::ok;
            }
        } else {
            if (pageKey < highKeyStr) {
                memcpy(&rid.pageNum, pageBuffer + path, sizeof(int));
                memcpy(&rid.slotNum, pageBuffer + path + sizeof(int), sizeof(short));
                keyIndex++;
                return RC::ok;
            }
        }
        return RC::RM_EOF;
    }

    template<class T>
    RC
    IndexManager::putChildEntry(IXFileHandle &fh, char *pageBuffer, ChildEntry<T> *newChildEntry, unsigned entryLen) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        unsigned short freeSpace = getFreeSpace(pageBuffer);
        if (keyNum == 0) {
            freeSpace -= 4;
        }
        //first page key that is bigger than entry's key
        unsigned breakKey = 0;
        //first page number
        unsigned path = sizeof(int);

        T pageKey;
        for (; breakKey < keyNum; breakKey++) {
            unsigned lastPath = path;
            memcpy(&pageKey, pageBuffer + path, sizeof(int));
            path += sizeof(int);

            if (checkBigger(pageKey, newChildEntry->key)) {
                path = lastPath;
                break;
            }
            //add page num
            path += sizeof(int);
        }
        //shift right
        unsigned totalLen = getIndexPath(pageBuffer, keyNum, fh.isKeyFixed);
        memmove(pageBuffer + path + entryLen, pageBuffer + path, totalLen - path);

        memcpy(pageBuffer + path, &(newChildEntry->key), entryLen - sizeof(int));

        path += entryLen - sizeof(int);
        memcpy(pageBuffer + path, &(newChildEntry->newPageNum), sizeof(int));

        updateKeyNum(pageBuffer, keyNum + 1);
        updateFreeSpace(pageBuffer, freeSpace - entryLen);

        return RC::ok;
    }

    RC IndexManager::putChildEntryStr(IXFileHandle &fh, char *pageBuffer, ChildEntryStr *newChildEntry,
                                      unsigned int entryLen) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        unsigned short freeSpace = getFreeSpace(pageBuffer);
        if (keyNum == 0) {
            freeSpace -= 4;
        }
        //first page key that is bigger than entry's key
        unsigned breakKey = 0;
        //first page number
        unsigned path = sizeof(int);

        char *pageKey;
        for (; breakKey < keyNum; breakKey++) {
            unsigned lastPath = path;
//            memcpy(&pageKey, pageBuffer + path, sizeof(int));
//            path += sizeof(int);
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            pageKey = new char[len + sizeof(int)];
            memcpy(pageKey, pageBuffer + path, len + sizeof(int));
            path += len + sizeof(int);

            if (checkBiggerStr(pageKey, newChildEntry->key)) {
                path = lastPath;
                break;
            }
            //add page num
            path += sizeof(int);
            delete[]pageKey;
        }
        //shift right
        unsigned totalLen = getIndexPath(pageBuffer, keyNum, fh.isKeyFixed);
        memmove(pageBuffer + path + entryLen, pageBuffer + path, totalLen - path);

        memcpy(pageBuffer + path, newChildEntry->key, entryLen - sizeof(int));

        path += entryLen - sizeof(int);
        memcpy(pageBuffer + path, &(newChildEntry->newPageNum), sizeof(int));

        updateKeyNum(pageBuffer, keyNum + 1);
        updateFreeSpace(pageBuffer, freeSpace - entryLen);

        return RC::ok;
    }

    template<class T>
    RC IndexManager::putLeafEntry(IXFileHandle &fh, char *pageBuffer, Entry<T> *entry, unsigned int entryLen) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        unsigned short freeSpace = getFreeSpace(pageBuffer);
        //first page key that is bigger than entry's key
        unsigned breakKey = 0;
        //first page number
        unsigned path = 0;

        T pageKey;
        for (; breakKey < keyNum; breakKey++) {
            unsigned lastPath = path;
            memcpy(&pageKey, pageBuffer + path, sizeof(int));
            path += sizeof(int);

            if (checkBigger(pageKey, entry->key)) {
                path = lastPath;
                break;
            }
            if (checkEqual(pageKey, entry->key)) {
                unsigned int pageNum = 0;
                unsigned short slotNum = 0;
                memcpy(&pageNum, pageBuffer + path, sizeof(int));
                memcpy(&slotNum, pageBuffer + path + sizeof(int), sizeof(short));
                if (pageNum == entry->rid.pageNum && slotNum == entry->rid.slotNum) {
                    return RC::ok;
                }
            }
            //add rid
            path += sizeof(int) + sizeof(short);
        }
        //shift right
        unsigned totalLen = getLeafPath(pageBuffer, keyNum, fh.isKeyFixed);
        memmove(pageBuffer + path + entryLen, pageBuffer + path, totalLen - path);

        memcpy(pageBuffer + path, &(entry->key), entryLen - sizeof(int) - sizeof(short));

        path += entryLen - sizeof(int) - sizeof(short);
        memcpy(pageBuffer + path, &(entry->rid.pageNum), sizeof(int));
        path += sizeof(int);
        memcpy(pageBuffer + path, &(entry->rid.slotNum), sizeof(short));

        updateKeyNum(pageBuffer, keyNum + 1);
        updateFreeSpace(pageBuffer, freeSpace - entryLen);

        return RC::ok;
    }

    RC IndexManager::putLeafEntryStr(IXFileHandle &fh, char *pageBuffer, EntryStr *entry, unsigned int entryLen) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        unsigned short freeSpace = getFreeSpace(pageBuffer);
        //first page key that is bigger than entry's key
        unsigned breakKey = 0;
        //first page number
        unsigned path = 0;

        char *pageKey;
        for (; breakKey < keyNum; breakKey++) {
            unsigned lastPath = path;

            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            pageKey = new char[len + sizeof(int)];
            memcpy(pageKey, pageBuffer + path, len + sizeof(int));
            path += len + sizeof(int);

            if (checkBiggerStr(pageKey, entry->key)) {
                path = lastPath;
                break;
            }
            if (checkEqualStr(pageKey, entry->key)) {
                unsigned int pageNum = 0;
                unsigned short slotNum = 0;
                memcpy(&pageNum, pageBuffer + path, sizeof(int));
                memcpy(&slotNum, pageBuffer + path + sizeof(int), sizeof(short));
                if (pageNum == entry->rid.pageNum && slotNum == entry->rid.slotNum) {
                    return RC::ok;
                }
            }
            //add rid
            path += sizeof(int) + sizeof(short);
            delete[]pageKey;
        }
        //shift right
        unsigned totalLen = getLeafPath(pageBuffer, keyNum, fh.isKeyFixed);
        memmove(pageBuffer + path + entryLen, pageBuffer + path, totalLen - path);

        memcpy(pageBuffer + path, entry->key, entryLen - sizeof(int) - sizeof(short));

        path += entryLen - sizeof(int) - sizeof(short);
        memcpy(pageBuffer + path, &(entry->rid.pageNum), sizeof(int));
        path += sizeof(int);
        memcpy(pageBuffer + path, &(entry->rid.slotNum), sizeof(short));

        updateKeyNum(pageBuffer, keyNum + 1);
        updateFreeSpace(pageBuffer, freeSpace - entryLen);

        return RC::ok;
    }

    template<class T>
    RC IndexManager::getIndexKey(IXFileHandle &fh, const char *pageBuffer, unsigned short keyIndex, T &key) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        if (keyNum == 0) {
            return RC::RM_EOF;
        }
        unsigned path = sizeof(int);
        unsigned lastPath = path;
        for (int i = 0; i < keyIndex; ++i) {
            lastPath = path;
            if (fh.isKeyFixed) {
                path += 2 * sizeof(int);
            } else {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += len + 2 * sizeof(int);
            }
        }
        path = lastPath;
        if (!fh.isKeyFixed) {
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            key = new char[len + sizeof(int)];
            memcpy(key, pageBuffer + path, len + sizeof(int));
        } else {
            memcpy(&key, pageBuffer + path, sizeof(int));
        }
        return RC::ok;
    }

    template<class T>
    RC IndexManager::getLeafKey(IXFileHandle &fh, const char *pageBuffer, unsigned short keyIndex, T &key) {
        unsigned short keyNum = getKeyNum(pageBuffer);
        if (keyNum == 0) {
            return RC::RM_EOF;
        }
        unsigned path = 0;
        unsigned lastPath = path;
        for (int i = 0; i < keyIndex; ++i) {
            lastPath = path;
            if (fh.isKeyFixed) {
                path += 2 * sizeof(int) + sizeof(short);
            } else {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += len + 2 * sizeof(int) + sizeof(short);
            }
        }
        path = lastPath;
        if (!fh.isKeyFixed) {
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            key = new char[len + sizeof(int)];
            memcpy(key, pageBuffer + path, len + sizeof(int));
        } else {
            memcpy(&key, pageBuffer + path, sizeof(int));
        }
        return RC::ok;
    }

    //remember newChildEntry should not be null
    //add newChildEntry and form a vector with all entry
    template<class T>
    RC IndexManager::formChildEntry(IXFileHandle &fh, unsigned int newPageNum, ChildEntry<T> *newChildEntry,
                                    std::vector<ChildEntry<T> *> &vec) {
        if (newChildEntry == nullptr) {
            return RC::RM_EOF;
        }
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(newPageNum, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        unsigned path = sizeof(int);

        T key;
        bool inserted = false;
        for (int i = 0; i < keyNum; i++) {
            auto *entry = new ChildEntry<T>;
            memcpy(&(entry->key), pageBuffer + path, sizeof(int));
            path += sizeof(int);
            memcpy(&(entry->newPageNum), pageBuffer + path, sizeof(int));
            if (!inserted && newChildEntry != nullptr && checkBigger(entry->key, newChildEntry->key)) {
                auto *entry = new ChildEntry<T>;
                memcpy(&(entry->key), &newChildEntry->key, sizeof(int));
                memcpy(&(entry->newPageNum), &newChildEntry->newPageNum, sizeof(int));
                vec.push_back(entry);
                inserted = true;
            }
            vec.push_back(entry);
            path += sizeof(int);
        }
        if (!inserted) {
            auto *entry = new ChildEntry<T>;
            memcpy(&(entry->key), &newChildEntry->key, sizeof(int));
            memcpy(&(entry->newPageNum), &newChildEntry->newPageNum, sizeof(int));
            vec.push_back(entry);
        }

        return RC::ok;
    }

    RC IndexManager::formChildEntryStr(IXFileHandle &fh, unsigned int newPageNum, ChildEntryStr *newChildEntry,
                                       std::vector<ChildEntryStr *> &vec) {
        if (newChildEntry == nullptr) {
            return RC::RM_EOF;
        }
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(newPageNum, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        unsigned path = sizeof(int);

        char *key;
        bool inserted = false;
        for (int i = 0; i < keyNum; i++) {
            auto *entry = new ChildEntryStr;
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            entry->key = new char[len + 4];
            memcpy(entry->key, pageBuffer + path, sizeof(int) + len);
            path += sizeof(int) + len;
            memcpy(&entry->newPageNum, pageBuffer + path, sizeof(int));
            if (!inserted && newChildEntry != nullptr && checkBiggerStr(entry->key, newChildEntry->key)) {
                auto *entry = new ChildEntryStr;
                int len = 0;
                memcpy(&len, newChildEntry->key, sizeof(int));
                entry->key = new char[len + 4];
                memcpy(entry->key, newChildEntry->key, sizeof(int) + len);
                memcpy(&entry->newPageNum, &newChildEntry->newPageNum, sizeof(int));
                vec.push_back(entry);
                inserted = true;
            }
            vec.push_back(entry);
            path += sizeof(int);
        }
        if (!inserted) {
            path += sizeof(int);
            auto *entry = new ChildEntryStr;
            int len = 0;
            memcpy(&len, newChildEntry->key, sizeof(int));
            entry->key = new char[len + 4];
            memcpy(entry->key, newChildEntry->key, sizeof(int) + len);
            memcpy(&entry->newPageNum, &newChildEntry->newPageNum, sizeof(int));
            vec.push_back(entry);
        }

        return RC::ok;
    }

    template<class T>
    RC IndexManager::formLeafEntry(IXFileHandle &fh, unsigned int pageNum, Entry<T> *newEntry,
                                   std::vector<Entry<T> *> &vec) {
//        if (newEntry != nullptr) {
//            return RC::RM_EOF;
//        }
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(pageNum, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        unsigned path = 0;

        T key;
        for (int i = 0; i < keyNum; i++) {
            auto *entry = new Entry<T>;
            memcpy(&(entry->key), pageBuffer + path, sizeof(int));
            path += sizeof(int);
            memcpy(&(entry->rid.pageNum), pageBuffer + path, sizeof(int));
            path += sizeof(int);
            memcpy(&(entry->rid.slotNum), pageBuffer + path, sizeof(short));
            path += sizeof(short);
            if (newEntry != nullptr && checkBigger(entry->key, newEntry->key)) {
                auto *entry = new Entry<T>;
                memcpy(&(entry->key), &newEntry->key, sizeof(int));
                memcpy(&(entry->rid.pageNum), &newEntry->rid.pageNum, sizeof(int));
                memcpy(&(entry->rid.slotNum), &newEntry->rid.slotNum, sizeof(short));
                vec.push_back(entry);
                newEntry = nullptr;
            }
            vec.push_back(entry);
        }
        if (newEntry != nullptr) {
            auto *entry = new Entry<T>;
            memcpy(&(entry->key), &newEntry->key, sizeof(int));
            memcpy(&(entry->rid.pageNum), &newEntry->rid.pageNum, sizeof(int));
            memcpy(&(entry->rid.slotNum), &newEntry->rid.slotNum, sizeof(short));
            vec.push_back(entry);
            newEntry = nullptr;
        }

        return RC::ok;
    }

    RC IndexManager::formLeafEntryStr(IXFileHandle &fh, unsigned int pageNum, EntryStr *newEntry,
                                      std::vector<EntryStr *> &vec) {
//        if (newEntry != nullptr) {
//            return RC::RM_EOF;
//        }
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(pageNum, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        unsigned path = 0;

        for (int i = 0; i < keyNum; i++) {
            auto *entry = new EntryStr;
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            entry->key = new char[len + 4];
            memcpy(entry->key, pageBuffer + path, sizeof(int) + len);
            path += sizeof(int) + len;
            memcpy(&(entry->rid.pageNum), pageBuffer + path, sizeof(int));
            path += sizeof(int);
            memcpy(&(entry->rid.slotNum), pageBuffer + path, sizeof(short));
            path += sizeof(short);
            if (newEntry != nullptr && checkBiggerStr(entry->key, newEntry->key)) {
                auto *entry = new EntryStr;
                int len = 0;
                memcpy(&len, newEntry->key, sizeof(int));
                entry->key = new char[len + 4];
                memcpy(entry->key, newEntry->key, sizeof(int) + len);
                memcpy(&(entry->rid.pageNum), &newEntry->rid.pageNum, sizeof(int));
                memcpy(&(entry->rid.slotNum), &newEntry->rid.slotNum, sizeof(short));
                vec.push_back(entry);
                newEntry = nullptr;
            }
            vec.push_back(entry);
        }
        if (newEntry != nullptr) {
            auto *entry = new EntryStr;
            int len = 0;
            memcpy(&len, newEntry->key, sizeof(int));
            entry->key = new char[len + 4];
            memcpy(entry->key, newEntry->key, sizeof(int) + len);
            memcpy(&(entry->rid.pageNum), &newEntry->rid.pageNum, sizeof(int));
            memcpy(&(entry->rid.slotNum), &newEntry->rid.slotNum, sizeof(short));
            vec.push_back(entry);
        }

        return RC::ok;
    }

    //assume 2d+1 keys and 2d+2 pointers in page, first d keys and d+1 pointers stay, last d keys and d+1 pointers move to new page.
    //and push middle key(newChildEntry) to upper function
    template<class T>
    RC IndexManager::splitIndexPage(IXFileHandle &fh, unsigned int tarPage, unsigned int &newPage,
                                    ChildEntry<T> *&newChildEntry) {
        char tarPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(tarPage, tarPageBuffer);
        std::vector<ChildEntry<T> *> sortedVec;
        formChildEntry(fh, tarPage, newChildEntry, sortedVec);
        unsigned keyNum = sortedVec.size();
        unsigned halfKey = (keyNum) / 2;

        updateKeyNum(tarPageBuffer, 0);
        updateFreeSpace(tarPageBuffer, PAGE_SIZE - 5);
        for (int i = 0; i < halfKey; ++i) {
            unsigned entryLen = 0;
            entryLen = getChildEntryLen(sortedVec[i]);
            putChildEntry(fh, tarPageBuffer, sortedVec[i], entryLen);
        }
        fh.fileHandle.writePage(tarPage, tarPageBuffer);

        unsigned newPageNum = 0;
        insertNewIndexPage(fh, newPageNum, false);
        char newPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(newPageNum, newPageBuffer);

        //fill newChildEntry
        newChildEntry->key = sortedVec[halfKey]->key;
        newChildEntry->newPageNum = newPageNum;

        //move rest to new node
        memcpy(newPageBuffer, &sortedVec[halfKey]->newPageNum, sizeof(int));
        for (int i = halfKey + 1; i < keyNum; i++) {
            unsigned entryLen = getChildEntryLen(sortedVec[i]);
            putChildEntry(fh, newPageBuffer, sortedVec[i], entryLen);
        }
        fh.fileHandle.writePage(newPageNum, newPageBuffer);

        return RC::ok;
    }

    RC IndexManager::splitIndexPageStr(IXFileHandle &fh, unsigned int tarPage, unsigned int &newPage,
                                       ChildEntryStr *&newChildEntry) {
        char tarPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(tarPage, tarPageBuffer);
        std::vector<ChildEntryStr *> sortedVec;
        formChildEntryStr(fh, tarPage, newChildEntry, sortedVec);
        unsigned keyNum = sortedVec.size();
        unsigned halfKey = (keyNum) / 2;

        updateKeyNum(tarPageBuffer, 0);
        updateFreeSpace(tarPageBuffer, PAGE_SIZE - 5);
        for (int i = 0; i < halfKey; ++i) {
            unsigned entryLen = 0;
            entryLen = getChildEntryLenStr(sortedVec[i]);
            putChildEntryStr(fh, tarPageBuffer, sortedVec[i], entryLen);
        }
        fh.fileHandle.writePage(tarPage, tarPageBuffer);

        unsigned newPageNum = 0;
        insertNewIndexPage(fh, newPageNum, false);
        char newPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(newPageNum, newPageBuffer);

        //fill newChildEntry
        int len = 0;
        memcpy(&len, sortedVec[halfKey]->key, sizeof(int));
        newChildEntry->key = new char[len + sizeof(int)];
        memcpy(newChildEntry->key, sortedVec[halfKey]->key, len + sizeof(int));

        newChildEntry->newPageNum = newPageNum;

        //move rest to new node
        memcpy(newPageBuffer, &sortedVec[halfKey]->newPageNum, sizeof(int));
        for (int i = halfKey + 1; i < keyNum; i++) {
            unsigned entryLen = getChildEntryLenStr(sortedVec[i]);
            putChildEntryStr(fh, newPageBuffer, sortedVec[i], entryLen);
        }
        fh.fileHandle.writePage(newPageNum, newPageBuffer);

        return RC::ok;
    }

    template<class T>
    RC IndexManager::splitLeafPage(IXFileHandle &fh, unsigned int tarPage, unsigned int &newPage,
                                   Entry<T> *newEntry, ChildEntry<T> *&newChildEntry) {
        char tarPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(tarPage, tarPageBuffer);
        std::vector<Entry<T> *> sortedVec;
        formLeafEntry(fh, tarPage, newEntry, sortedVec);
        unsigned keyNum = sortedVec.size();
        unsigned halfKey = (keyNum) / 2;

        updateKeyNum(tarPageBuffer, 0);
        updateFreeSpace(tarPageBuffer, PAGE_SIZE - 9);
        for (int i = 0; i < halfKey; ++i) {
            unsigned entryLen = 0;
            entryLen = getEntryLen(sortedVec[i]);
            putLeafEntry(fh, tarPageBuffer, sortedVec[i], entryLen);
        }
        unsigned oriNextPage = getNextPage(tarPageBuffer);

        unsigned newPageNum = 0;
        insertNewIndexPage(fh, newPageNum, true);
        char newPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(newPageNum, newPageBuffer);

        //fill newChildEntry
        newChildEntry = new ChildEntry<T>;
        newChildEntry->key = sortedVec[halfKey]->key;
        newChildEntry->newPageNum = newPageNum;

        //move rest to new node
        for (int i = halfKey; i < keyNum; i++) {
            unsigned entryLen = getEntryLen(sortedVec[i]);
            putLeafEntry(fh, newPageBuffer, sortedVec[i], entryLen);
        }

        updateNextPage(tarPageBuffer, newPageNum);
        updatePrevPage(newPageBuffer, tarPage);
        updateNextPage(newPageBuffer, oriNextPage);
        if (oriNextPage != NULL_PAGE) {
            char *nextPageBuffer = new char[PAGE_SIZE];
            fh.fileHandle.readPage(oriNextPage, nextPageBuffer);
            updatePrevPage(nextPageBuffer, newPageNum);
            fh.fileHandle.writePage(oriNextPage, nextPageBuffer);
            delete[]nextPageBuffer;
        }
        fh.fileHandle.writePage(tarPage, tarPageBuffer);
        fh.fileHandle.writePage(newPageNum, newPageBuffer);

        return RC::ok;

    }

    RC IndexManager::splitLeafPageStr(IXFileHandle &fh, unsigned int tarPage, unsigned int &newPage, EntryStr *newEntry,
                                      ChildEntryStr *&newChildEntry) {
        char tarPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(tarPage, tarPageBuffer);
        std::vector<EntryStr *> sortedVec;
        formLeafEntryStr(fh, tarPage, newEntry, sortedVec);
        unsigned keyNum = sortedVec.size();
        unsigned halfKey = (keyNum) / 2;

        updateKeyNum(tarPageBuffer, 0);
        updateFreeSpace(tarPageBuffer, PAGE_SIZE - 9);
        for (int i = 0; i < halfKey; ++i) {
            unsigned entryLen = 0;
            entryLen = getEntryLenStr(sortedVec[i]);
            putLeafEntryStr(fh, tarPageBuffer, sortedVec[i], entryLen);
        }
        unsigned oriNextPage = getNextPage(tarPageBuffer);

        unsigned newPageNum = 0;
        insertNewIndexPage(fh, newPageNum, true);
        char newPageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(newPageNum, newPageBuffer);

        //fill newChildEntry
        newChildEntry = new ChildEntryStr;
        int len = 0;
        memcpy(&len, sortedVec[halfKey]->key, sizeof(int));
        newChildEntry->key = new char[len + sizeof(int)];
        memcpy(newChildEntry->key, sortedVec[halfKey]->key, len + sizeof(int));
        newChildEntry->newPageNum = newPageNum;

        //move rest to new node
        for (int i = halfKey; i < keyNum; i++) {
            unsigned entryLen = getEntryLenStr(sortedVec[i]);
            putLeafEntryStr(fh, newPageBuffer, sortedVec[i], entryLen);
        }

        updateNextPage(tarPageBuffer, newPageNum);
        updatePrevPage(newPageBuffer, tarPage);
        updateNextPage(newPageBuffer, oriNextPage);
        if (oriNextPage != NULL_PAGE) {
            char *nextPageBuffer = new char[PAGE_SIZE];
            fh.fileHandle.readPage(oriNextPage, nextPageBuffer);
            updatePrevPage(nextPageBuffer, newPageNum);
            fh.fileHandle.writePage(oriNextPage, nextPageBuffer);
            delete[]nextPageBuffer;
        }
        fh.fileHandle.writePage(tarPage, tarPageBuffer);
        fh.fileHandle.writePage(newPageNum, newPageBuffer);

        return RC::ok;

    }

    template<class T>
    RC IndexManager::getNodeKeys(IXFileHandle &fh, const unsigned int currentNode, std::vector<T> &vec) const {
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(currentNode, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        bool isLeaf = checkLeafNode(pageBuffer);
        if (!isLeaf) {
            int path = sizeof(int);

            T key;
            for (int i = 0; i < keyNum; i++) {
                memcpy(&key, pageBuffer + path, sizeof(int));
                vec.push_back(key);
                path += sizeof(int) * 2;
            }
        } else {
            int path = 0;

            T key;
            for (int i = 0; i < keyNum; i++) {
                memcpy(&key, pageBuffer + path, sizeof(int));
                vec.push_back(key);
                path += sizeof(int) * 2 + sizeof(short);
            }
        }

        return RC::ok;
    }

    RC IndexManager::getNodeKeysStr(IXFileHandle &fh, const unsigned int currentNode,
                                    std::vector<std::string> &vec) const {
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(currentNode, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        bool isLeafNode = checkLeafNode(pageBuffer);
        if (!isLeafNode) {
            int path = sizeof(int);

            for (int i = 0; i < keyNum; i++) {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += sizeof(int);
                std::string key(pageBuffer + path, len);
                vec.push_back(key);
                path += sizeof(int) + len;
            }
        } else {
            int path = 0;

            for (int i = 0; i < keyNum; i++) {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += sizeof(int);
                std::string key(pageBuffer + path, len);
                vec.push_back(key);
                path += sizeof(int) + len + sizeof(short);
            }
        }

        return RC::ok;
    }

    template<class T>
    RC IndexManager::getNodePages(IXFileHandle &fh, const unsigned int currentNode, std::vector<RID> &vec) const {
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(currentNode, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        bool isLeaf = checkLeafNode(pageBuffer);

        int path = 0;
        unsigned pageNum = 0;
        if (!isLeaf) {
            memcpy(&pageNum, pageBuffer + path, sizeof(int));
            path += sizeof(int);
            vec.push_back({pageNum, 0});
            for (int i = 0; i < keyNum; i++) {
                path += sizeof(int);
                memcpy(&pageNum, pageBuffer + path, sizeof(int));
                vec.push_back({pageNum, 0});
                path += sizeof(int);
            }
        } else {
            unsigned short slotNum = 0;
            for (int i = 0; i < keyNum; i++) {
                //jump over key
                path += sizeof(int);
                memcpy(&pageNum, pageBuffer + path, sizeof(int));
                path += sizeof(int);
                memcpy(&slotNum, pageBuffer + path, sizeof(short));
                path += sizeof(short);
                vec.push_back({pageNum, slotNum});
            }
        }
        return RC::ok;
    }

    RC IndexManager::getNodePagesStr(IXFileHandle &fh, const unsigned int currentNode, std::vector<RID> &vec) const {
        char pageBuffer[PAGE_SIZE];
        fh.fileHandle.readPage(currentNode, pageBuffer);
        unsigned keyNum = getKeyNum(pageBuffer);
        bool isLeaf = checkLeafNode(pageBuffer);

        int path = 0;
        unsigned pageNum = 0;
        if (!isLeaf) {
            memcpy(&pageNum, pageBuffer + path, sizeof(int));
            path += sizeof(int);
            vec.push_back({pageNum, 0});
            for (int i = 0; i < keyNum; i++) {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += sizeof(int) + len;
                memcpy(&pageNum, pageBuffer + path, sizeof(int));
                path += sizeof(int);
                vec.push_back({pageNum, 0});
            }
        } else {
            for (int i = 0; i < keyNum; i++) {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += sizeof(int) + len;
                memcpy(&pageNum, pageBuffer + path, sizeof(int));
                path += sizeof(int);
                unsigned short slotNum = 0;
                memcpy(&slotNum, pageBuffer + path, sizeof(short));
                path += sizeof(short);
                vec.push_back({pageNum, slotNum});
            }
        }
        return RC::ok;
    }

    template<class T>
    RC IndexManager::delLeafEntry(IXFileHandle &fh, char *pageBuffer, Entry<T> *entry, unsigned entryLen) {
        unsigned keyNum = getKeyNum(pageBuffer);
        unsigned totalLen = getLeafTotalLen(pageBuffer, true);
        unsigned freeSpace = getFreeSpace(pageBuffer);
        unsigned path = 0;
        T pageKey;
        for (int i = 0; i < keyNum; i++) {
            memcpy(&pageKey, pageBuffer + path, sizeof(int));
            if (checkEqual(pageKey, entry->key)) {
                unsigned moveLen = totalLen - path - entryLen;
                memmove(pageBuffer + path, pageBuffer + path + entryLen, moveLen);
                freeSpace += entryLen;
                keyNum--;
                updateFreeSpace(pageBuffer, freeSpace);
                updateKeyNum(pageBuffer, keyNum);
                return RC::ok;
            }
            path += entryLen;
        }
        //not found
        return RC::RM_EOF;
    }

    RC IndexManager::delLeafEntryStr(IXFileHandle &fh, char *pageBuffer, EntryStr *entry, unsigned int entryLen) {
        unsigned keyNum = getKeyNum(pageBuffer);
        unsigned totalLen = getLeafTotalLen(pageBuffer, true);
        unsigned freeSpace = getFreeSpace(pageBuffer);
        unsigned path = 0;
        char *pageKey;
        for (int i = 0; i < keyNum; i++) {
            int len = 0;
            memcpy(&len, pageBuffer + path, sizeof(int));
            pageKey = new char[len + 4];
            memcpy(pageKey, pageBuffer + path, len + sizeof(int));
            if (checkEqual(pageKey, entry->key)) {
                unsigned moveLen = totalLen - path - entryLen;
                memmove(pageBuffer + path, pageBuffer + path + entryLen, moveLen);
                freeSpace += entryLen;
                keyNum--;
                updateFreeSpace(pageBuffer, freeSpace);
                updateKeyNum(pageBuffer, keyNum);
                return RC::ok;
            }
            delete[]pageKey;
            path += len + 2 * sizeof(int) + sizeof(short);
        }
        //not found
        return RC::RM_EOF;
    }

};