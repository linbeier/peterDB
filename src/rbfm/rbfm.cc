#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        PagedFileManager &p = PeterDB::PagedFileManager::instance();
        return p.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        PagedFileManager &p = PeterDB::PagedFileManager::instance();
        return p.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        PagedFileManager &p = PeterDB::PagedFileManager::instance();
        return p.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        PagedFileManager &p = PeterDB::PagedFileManager::instance();
        return p.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        //construct record
        char* recordbuf = new char[PAGE_SIZE];
        unsigned buflen = 0;
        unsigned result = constructRecord(static_cast<const char *>(data), recordDescriptor, recordbuf, buflen);

        unsigned totalpage = fileHandle.getNumberOfPages();
        if(totalpage == 0){
            result = insertNewRecordPage(fileHandle);
            totalpage++;
        }

        //check last page
        char pagebuffer[PAGE_SIZE];
        fileHandle.readPage(totalpage - 1, pagebuffer);
        unsigned freespace = getFreeSpace(pagebuffer);
        if(buflen <= freespace - 4){
            unsigned short offset = getRecordOffset(pagebuffer);
            memcpy(pagebuffer + offset, recordbuf, buflen);
            unsigned slotnum = writeSlotInfo(pagebuffer, offset, buflen);
            rid.pageNum = totalpage - 1;
            rid.slotNum = slotnum;
        }else{
            unsigned pagenum = 0;
            bool inserted = false;
            for(pagenum = 0; pagenum < totalpage - 1; ++pagenum){
                fileHandle.readPage(pagenum, pagebuffer);
                freespace = getFreeSpace(pagebuffer);
                if(buflen <= freespace - 4){
                    unsigned short offset = getRecordOffset(pagebuffer);
                    memcpy(pagebuffer + offset, recordbuf, buflen);
                    unsigned slotnum = writeSlotInfo(pagebuffer, offset, buflen);
                    inserted = true;
                    rid.pageNum = pagenum;
                    rid.slotNum = slotnum;
                    break;
                }
            }
            if(!inserted){
                result = insertNewRecordPage(fileHandle);
                totalpage++;
                fileHandle.readPage(totalpage - 1, pagebuffer);
                unsigned short offset = getRecordOffset(pagebuffer);
                memcpy(pagebuffer + offset, recordbuf, buflen);
                unsigned slotnum = writeSlotInfo(pagebuffer, offset, buflen);
                rid.pageNum = pagenum;
                rid.slotNum = slotnum;
            }
        }
        delete []recordbuf;
        return RC::ok;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {

        return -1;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

