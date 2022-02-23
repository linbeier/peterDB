//
// Created by 晓斌 on 2/18/22.
//
#include "src/include/ix.h"

namespace PeterDB {
#define FIXED '\0'
#define UNFIXED '\1'
#define IS_LEAF '\0'
#define IS_BRANCH '\1'

    unsigned short readKey(const char *pageBuffer) {
        unsigned short keys = 0;
        memcpy(&keys, pageBuffer + PAGE_SIZE - 2, sizeof(short));
        return keys;
    }

    bool checkLeafNode(const char *pageBuffer) {
        unsigned char isLeaf;
        memcpy(&isLeaf, pageBuffer + PAGE_SIZE - 5, sizeof(char));
        if (isLeaf == IS_LEAF) {
            return true;
        }
        return false;
    }

    unsigned getNextPage(const char *pageBuffer) {
        unsigned pageNum = 0;
        memcpy(&pageNum, pageBuffer + PAGE_SIZE - 4 - 4 - 1 - 4, sizeof(int));
        return pageNum;
    }

    RC IndexManager::insertHiddenPage(FILE *fd) {
        //create hidden page: data = read, write, append counter
        char pagebuffer[PAGE_SIZE];

        int initvalue = 0;
        memcpy(pagebuffer, &initvalue, sizeof(int));
        memcpy(pagebuffer + sizeof(int), &initvalue, sizeof(int));
        memcpy(pagebuffer + 2 * sizeof(int), &initvalue, sizeof(int));
        memcpy(pagebuffer + 3 * sizeof(int), &initvalue, sizeof(int));
        fseek(fd, 0, SEEK_SET);
        fwrite(pagebuffer, sizeof(char), PAGE_SIZE, fd);
        return RC::ok;
    }

    RC IndexManager::insertDummyNode(FILE *fd) {
        //create hidden page: data = read, write, append counter
        char pagebuffer[PAGE_SIZE];

        int initvalue = 0;
        memcpy(pagebuffer, &initvalue, sizeof(int));
        fseek(fd, PAGE_SIZE, SEEK_SET);
        fwrite(pagebuffer, sizeof(char), PAGE_SIZE, fd);
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

    RC IndexManager::writeHiddenPage(FileHandle &fileHandle) {
        //write back hidden block
        char pagebuffer[PAGE_SIZE];

        memcpy(pagebuffer, &fileHandle.readPageCounter, sizeof(int));
        memcpy(pagebuffer + sizeof(int), &fileHandle.writePageCounter, sizeof(int));
        memcpy(pagebuffer + 2 * sizeof(int), &fileHandle.appendPageCounter, sizeof(int));
        memcpy(pagebuffer + 3 * sizeof(int), &fileHandle.totalPage, sizeof(int));

        fseek(fileHandle.fd, 0, SEEK_SET);
        fwrite(pagebuffer, 1, PAGE_SIZE, fileHandle.fd);

        return RC::ok;
    }

    RC IndexManager::writeDummyNode(IXFileHandle &fileHandle) {
        char pagebuffer[PAGE_SIZE];

        memcpy(pagebuffer, &fileHandle.rootPage, sizeof(int));

        fseek(fileHandle.fileHandle.fd, PAGE_SIZE, SEEK_SET);
        fwrite(pagebuffer, 1, PAGE_SIZE, fileHandle.fileHandle.fd);

        return RC::ok;
    }

    RC IndexManager::insertNewIndexPage(IXFileHandle &fh, unsigned int &pageNum, bool isLeafNode, bool isKeyFixed) {
        char pageBuffer[PAGE_SIZE];
        unsigned short keyNumber = 0;
        unsigned short freeSpace = 0;
        unsigned char isLeaf = isLeafNode ? IS_LEAF : IS_BRANCH;

        memcpy(pageBuffer + PAGE_SIZE - 2, &keyNumber, sizeof(short));
        memcpy(pageBuffer + PAGE_SIZE - 5, &isLeaf, sizeof(char));

        //extra 8 bytes for prev page and next page
        if (isLeafNode) {
            freeSpace = PAGE_SIZE - 2 - 2 - 1 - 4 - 4;
            memcpy(pageBuffer + PAGE_SIZE - 4, &freeSpace, sizeof(short));
        } else {
            freeSpace = PAGE_SIZE - 2 - 2 - 1;
            memcpy(pageBuffer + PAGE_SIZE - 4, &freeSpace, sizeof(short));
        }

        pageNum = fh.fileHandle.totalPage;

        return fh.fileHandle.appendPage(pageBuffer);
    }

    //todo key is null
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
            checkIndexKeys<int>(fh, pageBuffer, attribute, lowKey, pageNum);
            return treeSearch(fh, attribute, lowKey, pageNum);
        } else if (attribute.type == TypeReal) {
            checkIndexKeys<float>(fh, pageBuffer, attribute, lowKey, pageNum);
            return treeSearch(fh, attribute, lowKey, pageNum);
        } else {
            checkIndexKeys<char *>(fh, pageBuffer, attribute, lowKey, pageNum);
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
            re = checkLeafKeys<int>(fh, pageBuffer, attribute, lowKey, keyIndex, noMatchKey, lowKeyInclusive);
        } else if (attribute.type == TypeReal) {
            re = checkLeafKeys<float>(fh, pageBuffer, attribute, lowKey, keyIndex, noMatchKey, lowKeyInclusive);
        } else {
            re = checkLeafKeys<char *>(fh, pageBuffer, attribute, lowKey, keyIndex, noMatchKey, lowKeyInclusive);
        }

        if (re != RC::ok) {
            return re;
        }
        return RC::ok;
    }

    template<class T>
    RC checkIndexKeys(IXFileHandle &fh, const char *pageBuffer, const Attribute &attribute,
                      const void *lowKey, unsigned &pageNum) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = readKey(pageBuffer);
        bool keyFixed = fh.isKeyFixed;

        if (lowKey == nullptr) {
            memcpy(&pageNum, pageBuffer, sizeof(int));
            return RC::ok;
        }

        T key;

        if (keyFixed) {

            for (int i = 0; i < keyNum; i++) {
                memcpy(&key, pageBuffer + 2 * sizeof(int) * i + sizeof(int), sizeof(int));
                if (*(T *) lowKey < key) {
                    memcpy(&pageNum, pageBuffer + 2 * sizeof(int) * i, sizeof(int));
                    return RC::ok;
                }
            }

            memcpy(&pageNum, pageBuffer + 2 * sizeof(int) * keyNum, sizeof(int));
            return RC::ok;

        } else {
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
    }

    template<class T>
    RC checkLeafKeys(IXFileHandle &fh, const char *pageBuffer, const Attribute &attribute,
                     const void *lowKey, unsigned &keyIndex, bool &noMatchKey, bool lowKeyInclusive) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = readKey(pageBuffer);
        bool keyFixed = fh.isKeyFixed;
        if (lowKey == nullptr) {
            keyIndex = 0;
            return RC::ok;
        }

        T key;

        if (keyFixed) {
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

        } else {
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
        }
        //find nothing matched in leaf nodes
        noMatchKey = true;
        return RC::ok;
    }

    //check high key
    template<class T>
    RC IX_ScanIterator::getRIDviaIndex(RID &rid, void *key) {
        char pageBuffer[PAGE_SIZE];
        ixFileHandle->fileHandle.readPage(pageIndex, pageBuffer);
        unsigned short keyNum = readKey(pageBuffer);
        if (keyNum - 1 <= keyIndex) {
            keyIndex = 0;
            pageIndex = getNextPage(pageBuffer);
            if (pageIndex == 0)return RM_EOF;
            return getRIDviaIndex<T>(rid, key);
        }

        if (attribute.type == TypeReal || attribute.type == TypeInt) {
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
        } else {
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
            memcpy(&key, pageBuffer + path, len + sizeof(int));
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
                    memcpy(&rid.pageNum, pageBuffer + path, sizeof(int));
                    memcpy(&rid.slotNum, pageBuffer + path + sizeof(int), sizeof(short));
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
        }
        return RC::RM_EOF;
    }
};