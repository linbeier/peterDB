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
};