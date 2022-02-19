//
// Created by 晓斌 on 2/18/22.
//
#include "src/include/ix.h"

namespace PeterDB {
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
};