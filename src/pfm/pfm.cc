#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() : FilePath("./"), HiddenPage(1) {

    }

    PagedFileManager::~PagedFileManager() {

    }

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {

        if (is_file_exist(fileName.c_str())) {
            return RC::CREA_FILE_FAIL;
        } else {
            FILE *fd = fopen(fileName.c_str(), "wb");
            if (fd == nullptr) {
                return RC::CREA_FILE_FAIL;
            }

            //create hidden page: data = read, write, append counter
            char pagebuffer[PAGE_SIZE];

            int initvalue = 0;
            memcpy(pagebuffer, &initvalue, sizeof(int));
            memcpy(pagebuffer + sizeof(int), &initvalue, sizeof(int));
            memcpy(pagebuffer + 2 * sizeof(int), &initvalue, sizeof(int));
            memcpy(pagebuffer + 3 * sizeof(int), &initvalue, sizeof(int));
            fseek(fd, 0, SEEK_SET);
            fwrite(pagebuffer, sizeof(char), PAGE_SIZE, fd);

            fclose(fd);
        }
        return RC::ok;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {

        if (!is_file_exist(fileName.c_str())) {
            return RC::REMV_FILE_FAIL;
        } else {
            const int result = remove(fileName.c_str());
            if (result != 0) {
                return RC::REMV_FILE_FAIL;
            }
        }
        return RC::ok;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {

        if (!is_file_exist(fileName.c_str())) {
            return RC::OPEN_FILE_FAIL;
        } else {
            FILE *fd = fopen(fileName.c_str(), "r+b");
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

            fileHandle = FileHandle();
            fileHandle.readPageCounter = (unsigned int) readcounter + 1;
            fileHandle.writePageCounter = (unsigned int) writecounter;
            fileHandle.appendPageCounter = (unsigned int) appendcounter;
            fileHandle.totalPage = (unsigned int) totalPage;
            fileHandle.fd = fd;

            for (int p = 0; p < fileHandle.totalPage; p++) {
                fseek(fd, p * PAGE_SIZE, SEEK_SET);
                fread(pagebuffer, PAGE_SIZE, 1, fileHandle.fd);
                char freespace[2];
                memcpy(freespace, pagebuffer + 4094, 2 * sizeof(char));
                unsigned short result = *((unsigned short *) freespace);
                fileHandle.freeSpaceList.push_back(result);
            }

            return RC::ok;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if (fileHandle.fd == nullptr) {
            return RC::FD_FAIL;
        } else {
            //write back hidden block
            char pagebuffer[PAGE_SIZE];

            memcpy(pagebuffer, &fileHandle.readPageCounter, sizeof(int));
            memcpy(pagebuffer + sizeof(int), &fileHandle.writePageCounter, sizeof(int));
            memcpy(pagebuffer + 2 * sizeof(int), &fileHandle.appendPageCounter, sizeof(int));
            memcpy(pagebuffer + 3 * sizeof(int), &fileHandle.totalPage, sizeof(int));

            fseek(fileHandle.fd, 0, SEEK_SET);
            fwrite(pagebuffer, 1, PAGE_SIZE, fileHandle.fd);

//            unsigned iteredPage = 0;
//            for (int p = 0; p < fileHandle.HiddenPage; p++) {
//
//                unsigned readSize = 0;
//                if (p == 0)readSize = 4 * sizeof(int);
//
//                for (; iteredPage < fileHandle.totalPage || readSize < PAGE_SIZE; iteredPage++) {
//
//                    memcpy(pagebuffer + readSize, &fileHandle.freeSpaceList.at(iteredPage), sizeof(short));
//                    readSize += sizeof(short);
//
//                }
//                fseek(fileHandle.fd, p * PAGE_SIZE, SEEK_SET);
//                fwrite(pagebuffer + p * PAGE_SIZE, 1, PAGE_SIZE, fileHandle.fd);
//            }

            //close file
            fclose(fileHandle.fd);
            fileHandle.fd = nullptr;

            return RC::ok;
        }
    }

    FileHandle::FileHandle() : fd(nullptr), HiddenPage(1), freeSpaceList() {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
        totalPage = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (fd == nullptr) {
            return RC::FD_FAIL;
        } else if (getNumberOfPages() <= pageNum) {
            return RC::OUT_OF_PAGE;
        } else {

            fseek(fd, PAGE_SIZE * (pageNum + HiddenPage), SEEK_SET);
            if (data != nullptr) {
                fread(data, sizeof(char), PAGE_SIZE, fd);
            }
            readPageCounter++;
            return RC::ok;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (fd == nullptr) {
            return RC::FD_FAIL;
        } else if (getNumberOfPages() <= pageNum) {
            return RC::OUT_OF_PAGE;
        } else {

            fseek(fd, PAGE_SIZE * (pageNum + HiddenPage), SEEK_SET);
            if (data != nullptr) {
                fwrite(data, sizeof(char), PAGE_SIZE, fd);
            }
            writePageCounter++;
            return RC::ok;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        if (fd == nullptr) {
            return RC::FD_FAIL;
        } else {

            fseek(fd, 0, SEEK_END);
            if (data != nullptr) {
                fwrite(data, sizeof(char), PAGE_SIZE, fd);
            }
            appendPageCounter++;
            totalPage++;
            return RC::ok;
        }
    }

    //not include hidden page
    unsigned FileHandle::getNumberOfPages() {
        if (fd == nullptr) {
            return 0;
        } else {
            return totalPage;
        }
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return RC::ok;
    }

} // namespace PeterDB