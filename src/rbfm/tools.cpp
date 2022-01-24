//
// Created by 晓斌 on 1/9/22.
//

#include "src/include/rbfm.h"

namespace PeterDB {

    RC RecordBasedFileManager::insertNewRecordPage(FileHandle &fileHandle) {
        char pagebuffer[PAGE_SIZE];
        memset(pagebuffer, 0, PAGE_SIZE);
        unsigned short free = 4092;
        unsigned short reco = 0;

        //todo: does this directly read work?
        memcpy(pagebuffer + 4092 * sizeof(char), &reco, 2 * sizeof(char));
        memcpy(pagebuffer + 4094 * sizeof(char), &free, 2 * sizeof(char));

        fileHandle.freeSpaceList.push_back(4092);
        return fileHandle.appendPage(pagebuffer);
    }

    unsigned short RecordBasedFileManager::getFreeSpace(const char *pagebuffer) {
        char freespace[2];
        memcpy(freespace, pagebuffer + 4094, 2 * sizeof(char));
        unsigned short result = *((unsigned short *) freespace);
        return result;
    }

    unsigned short RecordBasedFileManager::getRecordNum(const char *pagebuffer) {
        char recordnumbuf[2];
        memcpy(recordnumbuf, pagebuffer + 4092, 2 * sizeof(char));
        unsigned short result = *((unsigned short *) recordnumbuf);
        return result;
    }

    //bitnum start from 0
    bool RecordBasedFileManager::checkNull(char *nullbuffer, unsigned short bitnum, unsigned short totalbit) {

        char buf;
        memcpy(&buf, nullbuffer + bitnum / 8, sizeof(char));
        bool result = buf & (char) (1 << (7 - (bitnum % 8)));
        return result;
    }

    unsigned short RecordBasedFileManager::getRecordOffset(const char *pagebuffer) {
        unsigned short recordnum = getRecordNum(pagebuffer);
        if (recordnum == 0)return 0;

        char buf[2];
        unsigned short maxoffset = 0, len = 0;
        for (int i = 1; i <= recordnum; i++) {
            memcpy(buf, pagebuffer + PAGE_SIZE - 4 * (i + 1), sizeof(short));
            unsigned short offset = *((short *) buf);
            if (offset >= maxoffset) {
                maxoffset = offset;
                memcpy(buf, pagebuffer + PAGE_SIZE - 4 * (i + 1) + 2, sizeof(short));
                len = *((short *) buf);
            }
        }
//        memcpy(buf, pagebuffer + PAGE_SIZE - 4 * (recordnum + 1), sizeof(short));
//        unsigned short offset = *((short *) buf);
//        memcpy(buf, pagebuffer + PAGE_SIZE - 4 * (recordnum + 1) + 2, sizeof(short));
//        unsigned short len = *((short *) buf);
        return maxoffset + len;
    }

    //append slot info, slot number start from 1
    unsigned short RecordBasedFileManager::writeSlotInfo(char *pageData, unsigned short offset, unsigned short len,
                                                         unsigned short &freeSpace) {
        char buffer[4];
        unsigned short recordNum = getRecordNum(pageData);

        unsigned short slotNum = 1;
        for (slotNum = 1; slotNum <= recordNum; slotNum++) {
            //temporally construct rid, pageNum is not used
            RID r{0, slotNum};
            if (checkRecordDeleted(pageData, r)) {
                break;
            }
        }
        recordNum++;
        if (slotNum == recordNum)freeSpace -= len + 4;
        else freeSpace -= len;

        memcpy(buffer, &offset, sizeof(short));
        memcpy(buffer + 2, &len, sizeof(short));

        //write slot
        memcpy(pageData + PAGE_SIZE - 4 * (slotNum + 1), buffer, 4);
        //write recordnum and freespace
        memcpy(pageData + PAGE_SIZE - 4, &recordNum, 2);
        memcpy(pageData + PAGE_SIZE - 2, &freeSpace, 2);

        return slotNum;
    }


    RC RecordBasedFileManager::readSlotInfo(const char *pageData, const RID &rid, unsigned short &offset,
                                            unsigned short &len) {
        char buf[2];
        if (getRecordNum(pageData) < rid.slotNum) {
            return RC::OOUT_SLOT;
        }
        memcpy(buf, pageData + PAGE_SIZE - 4 * (rid.slotNum + 1), sizeof(short));
        offset = *((unsigned short *) buf);

        memcpy(buf, pageData + PAGE_SIZE - 4 * (rid.slotNum + 1) + 2, sizeof(short));
        len = *((unsigned short *) buf);
//        if (len >= 32768) {
//            len -= 32768;
//        }
        return RC::ok;
    }

    RC RecordBasedFileManager::updateSlotInfo(const char *pageData, const RID &rid, unsigned short offset,
                                              unsigned short len) {

        if (getRecordNum(pageData) < rid.slotNum) {
            return RC::OOUT_SLOT;
        }
        memcpy((void *) (pageData + PAGE_SIZE - 4 * (rid.slotNum + 1)), (void *) &offset, sizeof(short));

        memcpy((void *) (pageData + PAGE_SIZE - 4 * (rid.slotNum + 1) + 2), (void *) &len, sizeof(short));

        return RC::ok;
    }

    //todo: before call this function, allocate mem for recordbuffer.
    RC RecordBasedFileManager::constructRecord(const std::vector<Attribute> &recordDescriptor, const char *data,
                                               char *&recordbuffer, unsigned short &len) {

        unsigned short nullibyte = recordDescriptor.size() / 8 + (recordDescriptor.size() % 8 == 0 ? 0 : 1);
        char *nullbuffer = new char[nullibyte];
        unsigned short datapointer = nullibyte * sizeof(char);

        unsigned short bufpointer = 0;
        unsigned short fieldnum = recordDescriptor.size();

        memcpy(nullbuffer, data, nullibyte * sizeof(char));

        memcpy(recordbuffer, &fieldnum, sizeof(short));
        bufpointer += (fieldnum + 1) * sizeof(short);

        for (int i = 0; i < recordDescriptor.size(); ++i) {
            if (checkNull(nullbuffer, i, fieldnum)) {
                memset(recordbuffer + (i + 1) * sizeof(short), 0, sizeof(short));
                continue;
            }

            if (recordDescriptor[i].type == AttrType::TypeInt || recordDescriptor[i].type == AttrType::TypeReal) {
                //write data to record buffer
                memcpy(recordbuffer + bufpointer, (char *) data + datapointer, sizeof(int));

                bufpointer += sizeof(int);
                datapointer += sizeof(int);

                //change offset
                memcpy(recordbuffer + (i + 1) * sizeof(short), &bufpointer, sizeof(short));

            } else if (recordDescriptor[i].type == AttrType::TypeVarChar) {

                //get length of field
                char integer[4];
                memcpy(integer, (char *) data + datapointer, sizeof(int));
                unsigned short fieldlen = (unsigned short) *((unsigned short *) integer);
                datapointer += sizeof(int);

                //write data to record buffer
                memcpy(recordbuffer + bufpointer, (char *) data + datapointer, fieldlen);
                //change pointer
                bufpointer += fieldlen;
                datapointer += fieldlen;

                //change offset
                memcpy(recordbuffer + (i + 1) * sizeof(short), &bufpointer, sizeof(short));
            }
        }
        //todo: re-allocate buffer with len?
        len = bufpointer;
        delete[]nullbuffer;
        return RC::ok;
    }


    RC RecordBasedFileManager::deconstructRecord(const std::vector<Attribute> &recordDescriptor, const char *data,
                                                 char *&record) {
        char buf[2];
        memcpy(buf, data, sizeof(short));
        //total field number
        unsigned short fieldnum = *((unsigned short *) buf);
        //field num convert to null indicator
        std::vector<char> nullp(fieldnum / 8 + (fieldnum % 8 == 0 ? 0 : 1), 0);
        //pointer of current data
        unsigned short datapointer = (fieldnum + 1) * 2;
        //pointer of current record
        unsigned short recordpointer = nullp.size();

        for (int i = 0; i < fieldnum; i++) {
            memcpy(buf, data + (i + 1) * sizeof(short), sizeof(short));
            unsigned short loffset = *((unsigned short *) buf);
            if (loffset == 0) {
                //null field
                nullp[i / 8] = nullp[i / 8] | (1 << (7 - (i % 8)));

            } else {
                unsigned bytelen = loffset - datapointer;
                if (recordDescriptor[i].type == TypeVarChar) {
                    memcpy(record + recordpointer, &bytelen, sizeof(int));
                    recordpointer += sizeof(int);
                }
                memcpy(record + recordpointer, data + datapointer, bytelen);
                recordpointer += bytelen;
                datapointer += bytelen;
            }
        }

        for (int i = 0; i < nullp.size(); ++i) {
            memcpy(record + i, &nullp[i], sizeof(char));
        }

        return RC::ok;
    }

    RC RecordBasedFileManager::markDeleteRecord(char *pageData, const RID &rid) {

        unsigned short offset = 0, len = 0;
        readSlotInfo(pageData, rid, offset, len);
        if (len >= 32768) {
            len -= 32768;
        }
        unsigned short coffset = offset, clen = PAGE_SIZE + 1;
        updateSlotInfo(pageData, rid, coffset, clen);

        unsigned short recordNum = getRecordNum(pageData);
        unsigned short freeSpace = getFreeSpace(pageData);

        recordNum--;
        freeSpace += len;

        //write recordnum and freespace
        memcpy(pageData + PAGE_SIZE - 4, &recordNum, 2);
        memcpy(pageData + PAGE_SIZE - 2, &freeSpace, 2);

        return RC::ok;
    }

    // todo: check if length in slot larger than PAGE_SIZE and smaller than 32768 (10000000 00000000)
    bool RecordBasedFileManager::checkRecordDeleted(const char *pageData, const RID &rid) {
        unsigned short offset = 0, len = 0;
        readSlotInfo(pageData, rid, offset, len);
        if (len < 32768 && len >= PAGE_SIZE) {
            return true;
        }
        return false;
    }

    //check if first byte of record is 128(10000000), if yes, it indicates this record is a tombstone. First byte is field number, it will not be that big(field number has 2 bytes)
    bool RecordBasedFileManager::checkTombstone(const char *pageData, const RID &rid) {
        char buf[2];
        memcpy(buf, pageData + PAGE_SIZE - 4 * (rid.slotNum + 1), sizeof(short));
        unsigned short offset = *((unsigned short *) buf);
        char singlebuf;
        memcpy(&singlebuf, pageData + offset, sizeof(char));
        unsigned short flag = *((unsigned short *) &singlebuf);
        if (flag == 128)return true;
        return false;
    }

    RC RecordBasedFileManager::readTombStone(const char *pageData, const RID &rid, RID &targetRid) {
        unsigned short offset = 0, len = 0;
        readSlotInfo(pageData, rid, offset, len);
        if (len >= 32768) {//10000000 00000000
            len -= 32768;
        }
        unsigned pageNum = 0;
        unsigned short slotNum = 0;
        memcpy(&pageNum, pageData + offset + 1, sizeof(int));
        memcpy(&slotNum, pageData + offset + sizeof(int) + 1, sizeof(short));

        targetRid.pageNum = pageNum;
        targetRid.slotNum = slotNum;
        return RC::ok;
    }

    //reach where real data stores(across tombstones)
    RC RecordBasedFileManager::accessRealRecord(FileHandle &fileHandle, const RID &oriRid, RID &realRid) {
        char pageData[PAGE_SIZE];
        fileHandle.readPage(oriRid.pageNum, pageData);
        if (checkTombstone(pageData, oriRid)) {

            readTombStone(pageData, oriRid, realRid);
            RID r = {realRid.pageNum, realRid.slotNum};
            accessRealRecord(fileHandle, r, realRid);
        }
        return RC::ok;
    }

    //insert records that have been constructed, used by updateRecords
    RC RecordBasedFileManager::insertConstructedRecord(FileHandle &fileHandle, const char *recordbuf,
                                                       const unsigned int &buflen, RID &rid) {
        //padding record to 6 bytes, at least 6 bytes to store a tombstone, otherwise manager need to shift right when
        //record change to tombstone

        unsigned short totalpage = fileHandle.getNumberOfPages();
        if (totalpage == 0) {
            insertNewRecordPage(fileHandle);
            totalpage++;
        }

        char pagebuffer[PAGE_SIZE];
        //check which page have enough free space, start from last page
        if (buflen <= fileHandle.freeSpaceList.at(totalpage - 1) - 4) {

            fileHandle.readPage(totalpage - 1, pagebuffer);
            unsigned short offset = getRecordOffset(pagebuffer);
            memcpy(pagebuffer + offset, recordbuf, buflen);

            rid.pageNum = totalpage - 1;

            unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen, fileHandle.freeSpaceList[rid.pageNum]);

            rid.slotNum = slotnum;

        } else {

            bool inserted = false;
            for (int i = 0; i < fileHandle.totalPage - 1; i++) {
                if (buflen <= fileHandle.freeSpaceList[i] - 4) {

                    fileHandle.readPage(i, pagebuffer);
                    unsigned short offset = getRecordOffset(pagebuffer);
                    memcpy(pagebuffer + offset, recordbuf, buflen);

                    rid.pageNum = i;
                    unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen,
                                                           fileHandle.freeSpaceList[rid.pageNum]);
                    inserted = true;
                    rid.slotNum = slotnum;
                }
            }

            if (!inserted) {
                insertNewRecordPage(fileHandle);
                totalpage++;
                fileHandle.readPage(totalpage - 1, pagebuffer);
                unsigned short offset = getRecordOffset(pagebuffer);
                memcpy(pagebuffer + offset, recordbuf, buflen);
                rid.pageNum = totalpage - 1;
                unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen,
                                                       fileHandle.freeSpaceList[rid.pageNum]);
                rid.slotNum = slotnum;
            }
        }

        fileHandle.writePage(rid.pageNum, pagebuffer);
        return RC::ok;
    }
}