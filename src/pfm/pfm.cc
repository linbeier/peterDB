#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager():FilePath("./"), HiddenPage(1){

    }

    PagedFileManager::~PagedFileManager(){
        //todo

    }

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    RC PagedFileManager::createFile(const std::string &fileName) {

        if(is_file_exist(fileName.c_str())){
//            std::cout<<"Info: "<<filepath<<" already exist"<<std::endl;
            return RC::CREA_FILE_FAIL;
        }else{
            FILE *fd = fopen(fileName.c_str(),"wb");
            if(fd == nullptr){
//                std::cout<<"Error: Fail to create "<<fileName<<std::endl;
                return RC::CREA_FILE_FAIL;
            }

            //create hidden page: data = read, write, append counter
            char *pagebuffer = new char[PAGE_SIZE];

            int initvalue = 0;
            memcpy(pagebuffer, &initvalue, sizeof (int));
            memcpy(pagebuffer + sizeof (int), &initvalue, sizeof (int));
            memcpy(pagebuffer + 2*sizeof (int), &initvalue, sizeof (int));
            fseek(fd, 0, SEEK_SET);
            unsigned result = fwrite(pagebuffer, sizeof (char), PAGE_SIZE, fd);
            if(result != PAGE_SIZE) {
//                std::cout << "Error: Fail to write whole page, totally write: " << result << std::endl;
            }
            fclose(fd);
            delete []pagebuffer;
        }
        return RC::ok;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {

        if(!is_file_exist(fileName.c_str())){
//            std::cout<<"Info: "<<fileName<<" doesn't exist"<<std::endl;
            return RC::REMV_FILE_FAIL;
        }else{
            const int result = remove(fileName.c_str());
            if( result != 0 ){
//                std::cout<<"Error: opening file"<<fileName<<"with error:"<<strerror(errno)<<std::endl; // No such file or directory
                return RC::REMV_FILE_FAIL;
            }
        }
        return RC::ok;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {

        if(!is_file_exist(fileName.c_str())) {
//            std::cout<<"Error: Open file "<<fileName<<" doesn't exist"<<std::endl;
            return RC::OPEN_FILE_FAIL;
        }else{
            FILE *fd = fopen(fileName.c_str(),"r+b");
            if(fd == nullptr){
//                std::cout<<"Error: Open file "<<fileName<<std::endl;
                return RC::OPEN_FILE_FAIL;
            }
            char *pagebuffer = new char[PAGE_SIZE];
            fseek(fd, 0, SEEK_SET);
            fread(pagebuffer, 1, PAGE_SIZE, fd);
            int readcounter = 0, writecounter = 0, appendcounter = 0;
            //maybe just need to init counter and add counter to file when close file?
            memcpy(&readcounter, pagebuffer, sizeof (int));
            memcpy(&writecounter, pagebuffer + sizeof (int), sizeof (int));
            memcpy(&appendcounter, pagebuffer + 2*sizeof (int), sizeof (int));

            fileHandle = FileHandle();
            fileHandle.readPageCounter = (unsigned int)readcounter;
            fileHandle.writePageCounter = (unsigned int)writecounter;
            fileHandle.appendPageCounter = (unsigned int)appendcounter;
            fileHandle.fd = fd;

            delete []pagebuffer;
            return RC::ok;
        }
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        if(fileHandle.fd == nullptr){
//            std::cout<<"Error: lost file description"<<std::endl;
            return RC::FD_FAIL;
        }else{
            //write back hidden block
            char *pagebuffer = new char[PAGE_SIZE];

            memcpy(pagebuffer, &fileHandle.readPageCounter, sizeof (int));
            memcpy(pagebuffer + sizeof (int), &fileHandle.writePageCounter, sizeof (int));
            memcpy(pagebuffer + 2*sizeof (int), &fileHandle.appendPageCounter, sizeof (int));
            fseek(fileHandle.fd, 0, SEEK_SET);
            fwrite(pagebuffer,sizeof (char), PAGE_SIZE, fileHandle.fd);

            //close file
            fclose(fileHandle.fd);
            fileHandle.fd = nullptr;
            delete []pagebuffer;

            return RC::ok;
        }
    }

    FileHandle::FileHandle():fd(nullptr), HiddenPage(1) {
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    }

    FileHandle::~FileHandle() = default;

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if(fd == nullptr){
//            std::cout<< "Error: lost file description"<<std::endl;
            return RC::FD_FAIL;
        }else if(getNumberOfPages() <= pageNum){
//            std::cout<<"Error: Pagenum out of range, got"<<pageNum<<"total have: "<<getNumberOfPages()<<std::endl;
            return RC::OUT_OF_PAGE;
        }else{

            fseek(fd, PAGE_SIZE*(pageNum + HiddenPage), SEEK_SET);
            if(data != nullptr){
                fread(data, sizeof(char), PAGE_SIZE, fd);
            }
            readPageCounter++;
            return RC::ok;
        }
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if(fd == nullptr){
//            std::cout<< "Error: lost file description"<<std::endl;
            return RC::FD_FAIL;
        }else if(getNumberOfPages() <= pageNum){
//            std::cout<<"Error: Pagenum out of range, got"<<pageNum<<"total have: "<<getNumberOfPages()<<std::endl;
            return RC::OUT_OF_PAGE;
        }else{

            fseek(fd, PAGE_SIZE*(pageNum + HiddenPage), SEEK_SET);
            if(data != nullptr){
                fwrite(data, sizeof (char), PAGE_SIZE, fd);
            }
            writePageCounter++;
            return RC::ok;
        }
    }

    RC FileHandle::appendPage(const void *data) {
        if(fd == nullptr){
//            std::cout<< "Error: lost file description"<<std::endl;
            return RC::FD_FAIL;
        }else{

            fseek(fd, 0, SEEK_END);
            if(data != nullptr){
                fwrite(data, sizeof (char), PAGE_SIZE, fd);
            }
            appendPageCounter++;
            return RC::ok;
        }
    }

    //not include hidden page
    unsigned FileHandle::getNumberOfPages() {
        if(fd == nullptr){
//            std::cout<< "Error: lost file description"<<std::endl;
            return 0;
        }else{
            fseek(fd, 0L, SEEK_END);
            long len = ftell(fd);

            unsigned numpage = (len/PAGE_SIZE) - HiddenPage;
            return numpage;
        }
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return RC::ok;
    }

} // namespace PeterDB