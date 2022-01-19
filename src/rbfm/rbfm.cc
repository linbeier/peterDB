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
        unsigned short buflen = 0;
        unsigned short result = constructRecord(recordDescriptor,static_cast<const char *>(data), recordbuf, buflen);
        if(result > 4094){
            delete []recordbuf;
            return RC::RECORD_TOO_BIG;
        }

        unsigned short totalpage = fileHandle.getNumberOfPages();
        if(totalpage == 0){
            result = insertNewRecordPage(fileHandle);
            totalpage++;
        }

        //check last page
        char pagebuffer[PAGE_SIZE];
        fileHandle.readPage(totalpage - 1, pagebuffer);
        unsigned short freespace = getFreeSpace(pagebuffer);
        if(buflen <= freespace - 4){
            unsigned short offset = getRecordOffset(pagebuffer);
            memcpy(pagebuffer + offset, recordbuf, buflen);
            unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen);
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
                    unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen);
                    inserted = true;
                    rid.pageNum = pagenum;
                    rid.slotNum = slotnum;
                    break;
                }
            }
            if(!inserted){
                result = insertNewRecordPage(fileHandle);
                totalpage++;
                pagenum++;
                fileHandle.readPage(totalpage - 1, pagebuffer);
                unsigned short offset = getRecordOffset(pagebuffer);
                memcpy(pagebuffer + offset, recordbuf, buflen);
                unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen);
                rid.pageNum = pagenum;
                rid.slotNum = slotnum;
            }
        }

        fileHandle.writePage(rid.pageNum, pagebuffer);
        delete []recordbuf;
        return RC::ok;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        //fetch page data
        char* pageData = new char[PAGE_SIZE];
        unsigned short recordOffset= 0;
        unsigned short recordLen = 0;
        char* recordData = new char[PAGE_SIZE];

        RC result = fileHandle.readPage(rid.pageNum, pageData);
        if(result != RC::ok){
            delete []pageData;
            delete []recordData;
            return result;
        }
        if(checkRecordDeleted(pageData, rid)){
            delete []pageData;
            delete []recordData;
            return RC::RECORD_HAS_DEL;
        }

        //fetch slot data
        readSlotInfo(pageData, rid, recordOffset, recordLen);
        memcpy(recordData, pageData + recordOffset, recordLen );

        //construct record
        char* datap = static_cast<char *>(data);
        deconstructRecord(recordDescriptor, recordData, datap);

        delete []pageData;
        delete []recordData;
        return RC::ok;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        char* pageData = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, pageData);
        unsigned recordNum = getRecordNum(pageData);

        //first check if it has been deleted
        if(checkRecordDeleted(pageData, rid)){
            delete []pageData;
            return RC::RECORD_HAS_DEL;
        }

        //Figure out where and length to move
        unsigned short recordOffset = 0, recordLen = 0;
        unsigned short moveOffset = 0, moveLen = 0;
        readSlotInfo(pageData, rid, recordOffset, recordLen);
        moveOffset = recordOffset + recordLen;

        //no records after current record
        if( !checkRecordtoMove(pageData, rid) ){
            markDeleteRecord(pageData, rid);
            fileHandle.writePage(rid.pageNum, pageData);

            delete []pageData;
            return RC::ok;
        }

        for(unsigned short i = rid.slotNum + 1; i <= recordNum; ++i){
            RID r = {rid.pageNum, i};

            if( !checkRecordDeleted(pageData, r) ){
                unsigned short offset = 0, len = 0;
                readSlotInfo(pageData, r, offset, len);
                offset -= recordLen;
                moveLen += len;
                updateSlotInfo(pageData, r, offset, len);

            }else{
                continue;
            }
        }

        markDeleteRecord(pageData, rid);
        memcpy(pageData + recordOffset, pageData + moveOffset, moveLen);
        fileHandle.writePage(rid.pageNum, pageData);

        delete []pageData;
        return RC::ok;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        unsigned short fieldnum = recordDescriptor.size();
        unsigned short fieldbyte = recordDescriptor.size()/8 + (recordDescriptor.size()%8 == 0 ? 0: 1);
        unsigned short datapointer = fieldbyte;

        char* nullbuffer = new char[fieldbyte];
        memcpy(nullbuffer, data, fieldbyte);

        for(int i = 0; i < fieldnum; ++i){

            if(checkNull(nullbuffer, i, fieldnum)){

                if(i != fieldnum - 1) {
                    out << recordDescriptor[i].name << ": " << "NULL" << ", ";
                }else{
                    out << recordDescriptor[i].name << ": " << "NULL" << "\n";
                }

            }else if(recordDescriptor[i].type == AttrType::TypeVarChar){

                char bytelen[4];
                memcpy(bytelen, (char *)data + datapointer, 4);
                unsigned short len = *((unsigned short*)bytelen);
                datapointer += sizeof (int);

                char* value = new char[len];
                memcpy(value, (char *)data + datapointer, len);
                datapointer += len;
                if(i != fieldnum - 1) {
                    out << recordDescriptor[i].name << ": ";
                    out.write(value, len);
                    out << ", ";
                }else{
                    out << recordDescriptor[i].name << ": ";
                    out.write(value, len);
                    out << "\n";
                }
                delete []value;

            }else if(recordDescriptor[i].type == AttrType::TypeInt){
                char value[4];
                memcpy(value, (char *)data + datapointer, sizeof (int));
                datapointer += sizeof (int);
                int valuenum = *((int *)value);
                if(i != fieldnum - 1){
                    out<< recordDescriptor[i].name << ": "<<valuenum<<", ";
                }else{
                    out<< recordDescriptor[i].name << ": "<<valuenum<<"\n";
                }
            }else if(recordDescriptor[i].type == AttrType::TypeReal){
                char value[4];
                memcpy(value, (char *)data + datapointer, sizeof (float));
                datapointer += sizeof (float);
                float valuenum = *((float *)value);
                if(i != fieldnum - 1){
                    out<< recordDescriptor[i].name << ": "<<valuenum<<", ";
                }else{
                    out<< recordDescriptor[i].name << ": "<<valuenum<<"\n";
                }
            }
        }
        delete []nullbuffer;
        return RC::ok;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return RC::ok;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return RC::ok;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return RC::ok;
    }

} // namespace PeterDB

