//
// Created by 晓斌 on 1/9/22.
//

#include "src/include/rbfm.h"

namespace PeterDB{
    RC RecordBasedFileManager::insertNewRecordPage(FileHandle &fileHandle){
        char pagebuffer[PAGE_SIZE];
        unsigned short free = 4092;
        unsigned short reco = 0;
        char* freespace = (char *)&free;
        char *recordenum = (char *)&reco;
        //todo: does this directly read work?
        memcpy(pagebuffer + 4092*sizeof (char), &recordenum, 2*sizeof (char));
        memcpy(pagebuffer + 4094*sizeof (char), &freespace, 2*sizeof (char));
        return fileHandle.appendPage(pagebuffer);
    }

    unsigned RecordBasedFileManager::getFreeSpace(const char*pagebuffer){
//        char pagebuffer[PAGE_SIZE];
//        RC rc = fileHandle.readPage(pageNum, pagebuffer);
//        if(rc != RC::ok){
//            return 0;
//        }
        char freespace[2];
        memcpy(freespace, pagebuffer + 4094, 2*sizeof (char));
        unsigned result = *((unsigned *)freespace);
        return result;
    }

    unsigned RecordBasedFileManager::getRecordNum(const char*pagebuffer){
//        char pagebuffer[PAGE_SIZE];
//        RC rc = fileHandle.readPage(pageNum, pagebuffer);
//        if(rc != RC::ok){
//            return 0;
//        }
        char recordnumbuf[2];
        memcpy(recordnumbuf, pagebuffer + 4092, 2*sizeof (char));
        unsigned result = *((unsigned *)recordnumbuf);
        return result;
    }

    bool RecordBasedFileManager::checkNull(char *nullbuffer, unsigned bitnum, unsigned totalbyte){
        if(bitnum >= totalbyte*8){
            std::cout<<"Error: bitnum bigger than total bits"<<std::endl;
            return false;
        }
        char buf;
        memcpy(&buf, nullbuffer + bitnum/8, sizeof (char));
        bool result = (1<<abs(7-(int)bitnum%8)) & buf;
        return result;
    }

    unsigned RecordBasedFileManager::getRecordOffset(const char *pagebuffer) {
        unsigned recordnum = getRecordNum(pagebuffer);
        char buf[2];
        memcpy(buf, pagebuffer + PAGE_SIZE - 4*(recordnum + 1), sizeof (short));
        unsigned short offset = *((short *)buf);
        memcpy(buf, pagebuffer + PAGE_SIZE - 4*(recordnum + 1) + 2, sizeof (short));
        unsigned short len = *((short *)buf);
        return offset + len;
    }

    unsigned RecordBasedFileManager::writeSlotInfo(char *pagedata, unsigned offset, unsigned len) {
        char buffer[4];
        unsigned recordnum = getRecordNum(pagedata);
        unsigned freespace = getFreeSpace(pagedata);

        recordnum++;
        freespace -= len;

        memset(buffer, offset, sizeof (short));
        memset(buffer, len, sizeof (short));

        //write slot
        memcpy(pagedata + PAGE_SIZE - 4*(recordnum + 1), buffer, 4);
        //write recordnum and freespace
        memcpy(pagedata + PAGE_SIZE - 4, &recordnum, 2);
        memcpy(pagedata + PAGE_SIZE - 2, &freespace, 2);

        return recordnum;
    }

    //todo: before call this function, allocate mem for recordbuffer.
    RC RecordBasedFileManager::constructRecord(const std::vector<Attribute> &recordDescriptor, const char* data,  char*& recordbuffer, unsigned short &len){

        unsigned short fieldbyte = recordDescriptor.size()/8 + (recordDescriptor.size()%8 == 0 ? 0: 1);
        char *nullbuffer = new char[fieldbyte];
        unsigned short datapointer = fieldbyte*sizeof (char);

//        char *recordbuffer = new char[4000];
        unsigned short bufpointer = 0;
        unsigned short fieldnum = recordDescriptor.size();

        memcpy(nullbuffer, data, fieldbyte*sizeof (char));

        memcpy(recordbuffer, &fieldnum, sizeof(short));
        bufpointer += (fieldnum + 1)*sizeof (short);

        for(int i = 0; i < recordDescriptor.size(); ++i){
            if(checkNull(nullbuffer, i, fieldnum)){
                memset(recordbuffer + (i+1)*sizeof (short), 0, sizeof (short));
                continue;
            }

            if(recordDescriptor[i].type == AttrType::TypeInt || recordDescriptor[i].type == AttrType::TypeReal){
                //write data to record buffer
                memcpy(recordbuffer + bufpointer, (char *)data + datapointer, sizeof (int));

                bufpointer += sizeof (int);

                //change offset
                memset(recordbuffer + (i+1)*sizeof (short), bufpointer, sizeof (short));

            }else if(recordDescriptor[i].type == AttrType::TypeVarChar){

                //get length of field
                char integer[4];
                memcpy(integer, (char *)data + datapointer, sizeof (int));
                unsigned short fieldlen = (unsigned short) *((int*)integer);
                datapointer += sizeof (int);

                //write data to record buffer
                memcpy(recordbuffer + bufpointer, (char *)data + datapointer, fieldlen);
                //change pointer
                bufpointer += fieldlen;
                datapointer += fieldlen;

                //change offset
                memset(recordbuffer + (i + 1)*sizeof (short), bufpointer, sizeof (short));
            }
        }
        //todo: re-allocate buffer with len?
        len = bufpointer;
        delete []nullbuffer;
        return RC::ok;
    }


    RC RecordBasedFileManager::readSlotInfo(const char *pagedata, const RID &rid, unsigned short &offset,
                                            unsigned short &len) {
        char buf[2];
        if(getRecordNum(pagedata) < rid.slotNum){
            return RC::OUT_OF_SLOT;
        }
        memcpy(buf, pagedata + PAGE_SIZE - 4 * (rid.slotNum + 1), sizeof (short));
        offset = *((unsigned short*) buf);
        memcpy(buf, pagedata + PAGE_SIZE - 4 * (rid.slotNum + 1) + 2, sizeof (short));
        len = *((unsigned short*) buf);
        return RC::ok;
    }

    RC RecordBasedFileManager::deconstructRecord(const std::vector<Attribute> &recordDescriptor, const char *data,
                                                  char *&record) {
        char buf[2];
        memcpy(buf, data, sizeof (short));
        //total field number
        unsigned short fieldnum = *((unsigned short*) buf);
        //field num convert to null indicator
        std::vector<char> nullp(fieldnum / 8 + (fieldnum % 8 == 0 ? 0 : 1));
        //pointer of current data
        unsigned short datapointer = (fieldnum + 1) * 2;
        //pointer of current record
        unsigned short recordpointer = nullp.size();

        for(int i = 0; i < fieldnum; i++){
            memcpy(buf, data + (i + 1) * sizeof (short), sizeof (short));
            unsigned short loffset = *((unsigned short*) buf);
            if(loffset == 0){
                //null field
                nullp[i/8] = nullp[i/8] | (1 << (7 - (i % 8)));

            }else{
                unsigned short bytelen = datapointer - loffset;
                if(recordDescriptor[i].type == TypeVarChar){
                    memcpy(record + recordpointer, &bytelen, sizeof (short));
                    recordpointer += sizeof (short);
                }
                memcpy(record + recordpointer, data + datapointer, bytelen);
                recordpointer += bytelen;
                datapointer += bytelen;
            }
        }

        for(int i = 0; i < nullp.size(); ++i){
            memcpy(record + i, &nullp[i], sizeof (char));
        }

        return RC::ok;
    }
}