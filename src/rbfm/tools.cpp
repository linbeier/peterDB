//
// Created by 晓斌 on 1/9/22.
//

#include "src/include/rbfm.h"

namespace PeterDB{
    RC RecordBasedFileManager::insertNewRecordPage(FileHandle &fileHandle){
        char pagebuffer[PAGE_SIZE];
        unsigned short free = 4092;
        unsigned short reco = 0;

        //todo: does this directly read work?
        memcpy(pagebuffer + 4092*sizeof (char), &reco, 2*sizeof (char));
        memcpy(pagebuffer + 4094*sizeof (char), &free, 2*sizeof (char));
        return fileHandle.appendPage(pagebuffer);
    }

    unsigned short RecordBasedFileManager::getFreeSpace(const char*pagebuffer){
//        char pagebuffer[PAGE_SIZE];
//        RC rc = fileHandle.readPage(pageNum, pagebuffer);
//        if(rc != RC::ok){
//            return 0;
//        }
        char freespace[2];
        memcpy(freespace, pagebuffer + 4094, 2*sizeof (char));
        unsigned short result = *((unsigned short*)freespace);
        return result;
    }

    unsigned short RecordBasedFileManager::getRecordNum(const char*pagebuffer){
//        char pagebuffer[PAGE_SIZE];
//        RC rc = fileHandle.readPage(pageNum, pagebuffer);
//        if(rc != RC::ok){
//            return 0;
//        }
        char recordnumbuf[2];
        memcpy(recordnumbuf, pagebuffer + 4092, 2*sizeof (char));
        unsigned short result = *((unsigned short*)recordnumbuf);
        return result;
    }

    //bitnum start from 0
    bool RecordBasedFileManager::checkNull(char *nullbuffer, unsigned short bitnum, unsigned short totalbit){
//        if(bitnum >= totalbit){
//            std::cout<<"Error: bitnum bigger than total bits"<<std::endl;
//            return false;
//        }
        char buf;
        memcpy(&buf, nullbuffer + bitnum/8, sizeof (char));
        bool result = (buf >> (7 - (bitnum % 8))) & 1;
        return result;
    }

    unsigned short RecordBasedFileManager::getRecordOffset(const char *pagebuffer) {
        unsigned short recordnum = getRecordNum(pagebuffer);
        if(recordnum == 0)return 0;

        char buf[2];
        memcpy(buf, pagebuffer + PAGE_SIZE - 4*(recordnum + 1), sizeof (short));
        unsigned short offset = *((short *)buf);
        memcpy(buf, pagebuffer + PAGE_SIZE - 4*(recordnum + 1) + 2, sizeof (short));
        unsigned short len = *((short *)buf);
        return offset + len;
    }

    unsigned short RecordBasedFileManager::writeSlotInfo(char *pagedata, unsigned short offset, unsigned short len) {
        char buffer[4];
        unsigned short recordnum = getRecordNum(pagedata);
        unsigned short freespace = getFreeSpace(pagedata);

        recordnum++;
        freespace -= len + 4;

        memcpy(buffer, &offset, sizeof (short));
        memcpy(buffer + 2, &len, sizeof (short));

        //write slot
        memcpy(pagedata + PAGE_SIZE - 4*(recordnum + 1), buffer, 4);
        //write recordnum and freespace
        memcpy(pagedata + PAGE_SIZE - 4, &recordnum, 2);
        memcpy(pagedata + PAGE_SIZE - 2, &freespace, 2);

        return recordnum;
    }

    //todo: before call this function, allocate mem for recordbuffer.
    RC RecordBasedFileManager::constructRecord(const std::vector<Attribute> &recordDescriptor, const char* data,  char*& recordbuffer, unsigned short &len){

        unsigned short nullibyte = recordDescriptor.size()/8 + (recordDescriptor.size()%8 == 0 ? 0: 1);
        char *nullbuffer = new char[nullibyte];
        unsigned short datapointer = nullibyte*sizeof (char);

        unsigned short bufpointer = 0;
        unsigned short fieldnum = recordDescriptor.size();

        memcpy(nullbuffer, data, nullibyte*sizeof (char));

        memcpy(recordbuffer, &fieldnum, sizeof(short));
//        unsigned short testnum = 0;
//        char testbuf[2];
//        memcpy(&testbuf, recordbuffer, sizeof (short));
//        testnum = *((unsigned short*)testbuf);

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
                datapointer += sizeof (int);

                //change offset
                memcpy(recordbuffer + (i+1)*sizeof (short), &bufpointer, sizeof (short));

            }else if(recordDescriptor[i].type == AttrType::TypeVarChar){

                //get length of field
                char integer[4];
                memcpy(integer, (char *)data + datapointer, sizeof (int));
                unsigned short fieldlen = (unsigned short) *((unsigned short*)integer);
                datapointer += sizeof (int);

                //write data to record buffer
                memcpy(recordbuffer + bufpointer, (char *)data + datapointer, fieldlen);
                //change pointer
                bufpointer += fieldlen;
                datapointer += fieldlen;

                //change offset
                memcpy(recordbuffer + (i + 1)*sizeof (short), &bufpointer, sizeof (short));
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
            return RC::OOUT_SLOT;
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
        std::vector<char> nullp(fieldnum / 8 + (fieldnum % 8 == 0 ? 0 : 1), 0);
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
                unsigned bytelen = loffset - datapointer;
                if(recordDescriptor[i].type == TypeVarChar){
                    memcpy(record + recordpointer, &bytelen, sizeof (int));
                    recordpointer += sizeof (int);
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