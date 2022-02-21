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

    RC
    IndexManager::findKeyInLeaf(IXFileHandle &fh, const Attribute &attribute, const void *lowKey, unsigned &pageNum) {
        if (attribute.type == TypeReal || attribute.type == TypeInt) {
            fh.isKeyFixed = true;
        }
        pageNum = fh.rootPage;
        return treeSearch(fh, attribute, lowKey, pageNum);
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

    template<class T>
    RC IndexManager::checkIndexKeys(IXFileHandle &fh, const char *pageBuffer, const Attribute &attribute,
                                    const void *lowKey, unsigned &pageNum) {
        if (pageBuffer == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned short keyNum = readKey(pageBuffer);
        bool keyFixed = fh.isKeyFixed;

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
            for (int i = 0; i < keyNum; i++) {
                int len = 0;
                memcpy(&len, pageBuffer + path, sizeof(int));
                path += sizeof(int);
                char *buffer = new char[len];
                memcpy(buffer, pageBuffer + path, len);
                path += len;

                if (memcmp(lowKey, &buffer, len) < 0) {
                    memcpy(&pageNum, pageBuffer + path - len - 2 * sizeof(int), sizeof(int));
                    delete[]buffer;
                    return RC::ok;
                }

                path += sizeof(int);
                delete[]buffer;
            }

            memcpy(&pageNum, pageBuffer + path - sizeof(int), sizeof(int));
            return RC::ok;

        }
    }

    template<class T>
    RC IndexManager::checkLeafKeys(IXFileHandle &fh, const char *pageBuffer, const Attribute &attribute,
                                   const void *lowKey, unsigned int &pageNum) {
        
    }
};