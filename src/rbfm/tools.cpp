//
// Created by 晓斌 on 1/9/22.
//

#include "src/include/rbfm.h"
#include "unordered_map"
#include "algorithm"

namespace PeterDB {

    RC RecordBasedFileManager::insertNewRecordPage(FileHandle &fileHandle) {
        char pagebuffer[PAGE_SIZE];
        memset(pagebuffer, 0, PAGE_SIZE);
        unsigned short free = 4092;
        unsigned short reco = 0;

        memcpy(pagebuffer + 4092 * sizeof(char), &reco, sizeof(short));
        memcpy(pagebuffer + 4094 * sizeof(char), &free, sizeof(short));

        fileHandle.freeSpaceList.push_back(4092);
        return fileHandle.appendPage(pagebuffer);
    }

    unsigned short RecordBasedFileManager::getFreeSpace(FileHandle &fileHandle, unsigned pageNum) {
//        char freespace[2];
//        memcpy(freespace, pagebuffer + 4094, 2 * sizeof(char));
//        unsigned short result = *((unsigned short *) freespace);
        return fileHandle.freeSpaceList[pageNum];
    }

    unsigned short RecordBasedFileManager::getMaxSlotNum(const char *pageData, unsigned short recordNum) {
        unsigned short slotCounter = 0;
        for (unsigned i = 1; i <= recordNum; i++) {
            slotCounter++;
            if (checkRecordDeleted(pageData, {0, slotCounter})) {
                i--;
            }
        }
        return slotCounter;
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
        unsigned short maxSlot = getMaxSlotNum(pagebuffer, recordnum);
        for (int i = 1; i <= maxSlot; i++) {
            memcpy(buf, pagebuffer + PAGE_SIZE - 4 * (i + 1), sizeof(short));
            unsigned short offset = *((short *) buf);
            if (offset >= maxoffset) {
                maxoffset = offset;
                memcpy(buf, pagebuffer + PAGE_SIZE - 4 * (i + 1) + 2, sizeof(short));
                len = *((short *) buf);
                if (len >= 32768) {
                    len -= 32768;
                }
            }
        }

        return maxoffset + len;
    }

    //append slot info, slot number start from 1
    unsigned short RecordBasedFileManager::writeSlotInfo(char *pageData, unsigned short offset, unsigned short len,
                                                         short &freeSpace) {
        char buffer[4];
        unsigned short recordNum = getRecordNum(pageData);

        unsigned short slotNum = 1;
        bool usedSlot = false;
        for (slotNum = 1; slotNum <= recordNum; slotNum++) {
            //temporally construct rid, pageNum is not used
            RID r{0, slotNum};
            if (checkRecordDeleted(pageData, r)) {
                usedSlot = true;
                break;
            }
        }

        if (!usedSlot)freeSpace -= len + 4;
        else freeSpace -= len;

        memcpy(buffer, &offset, sizeof(short));
        memcpy(buffer + sizeof(short), &len, sizeof(short));
        recordNum++;

        //write slot
        memcpy(pageData + PAGE_SIZE - 2 * sizeof(short) * (slotNum + 1), buffer, 2 * sizeof(short));
        //write recordnum and freespace
        memcpy(pageData + PAGE_SIZE - 4, &recordNum, sizeof(short));
        memcpy(pageData + PAGE_SIZE - 2, &freeSpace, sizeof(short));

        return slotNum;
    }


    RC RecordBasedFileManager::readSlotInfo(const char *pageData, const RID &rid, unsigned short &offset,
                                            unsigned short &len) {
        char buf[2];
//        if (getRecordNum(pageData) < rid.slotNum) {
//            return RC::OOUT_SLOT;
//        }
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

//        if (getRecordNum(pageData) < rid.slotNum) {
//            return RC::OOUT_SLOT;
//        }
        memcpy((void *) (pageData + PAGE_SIZE - 4 * (rid.slotNum + 1)), (void *) &offset, sizeof(short));

        memcpy((void *) (pageData + PAGE_SIZE - 4 * (rid.slotNum + 1) + 2), (void *) &len, sizeof(short));

        return RC::ok;
    }

    RC RecordBasedFileManager::changeFreeSpace(FileHandle &fileHandle, const RID &rid, char *pageData,
                                               const unsigned short freeSpace) {
        fileHandle.freeSpaceList[rid.pageNum] = freeSpace;
        memcpy(pageData + PAGE_SIZE - 2, &freeSpace, sizeof(short));
        return RC::ok;
    }

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

    RC RecordBasedFileManager::markDeleteRecord(FileHandle &fileHandle, char *pageData, const RID &rid) {

        unsigned short offset = 0, len = 0;
        readSlotInfo(pageData, rid, offset, len);
        if (len >= 32768) {
            len -= 32768;
        }
        unsigned short coffset = offset, clen = PAGE_SIZE + 1;
        updateSlotInfo(pageData, rid, coffset, clen);

        unsigned short recordNum = getRecordNum(pageData);
        unsigned short freeSpace = getFreeSpace(fileHandle, rid.pageNum);

        recordNum--;
        freeSpace += len;
        fileHandle.freeSpaceList[rid.pageNum] += len;

        //write recordnum and freespace
        memcpy(pageData + PAGE_SIZE - 4, &recordNum, 2);
        memcpy(pageData + PAGE_SIZE - 2, &freeSpace, 2);

        return RC::ok;
    }

    // check if length in slot larger than PAGE_SIZE and smaller than 32768 (10000000 00000000)
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
        if (singlebuf == 128)return true;
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

    //insert records that have been constructed, used by update Records
    RC RecordBasedFileManager::insertConstructedRecord(FileHandle &fileHandle, const char *recordbuf,
                                                       const int &buflen, RID &rid) {
        //padding record to 6 bytes, at least 6 bytes to store a tombstone, otherwise manager need to shift right when
        //record change to tombstone

        unsigned short totalpage = fileHandle.getNumberOfPages();
        if (totalpage == 0) {
            insertNewRecordPage(fileHandle);
            totalpage++;
        }

        char pagebuffer[PAGE_SIZE];
        //check which page have enough free space, start from last page
        if (buflen <= (short) (fileHandle.freeSpaceList[totalpage - 1] - 4)) {

            fileHandle.readPage(totalpage - 1, pagebuffer);
            unsigned short offset = getRecordOffset(pagebuffer);
            memcpy(pagebuffer + offset, recordbuf, buflen);

            rid.pageNum = totalpage - 1;
            unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen, fileHandle.freeSpaceList[rid.pageNum]);
            rid.slotNum = slotnum;

            markInternalRid(pagebuffer, rid);

        } else {

            bool inserted = false;
            for (int i = 0; i < fileHandle.totalPage - 1; i++) {
                if (buflen <= (short) (fileHandle.freeSpaceList[i] - 4)) {

                    fileHandle.readPage(i, pagebuffer);
                    unsigned short offset = getRecordOffset(pagebuffer);
                    memcpy(pagebuffer + offset, recordbuf, buflen);

                    rid.pageNum = i;
                    unsigned short slotnum = writeSlotInfo(pagebuffer, offset, buflen,
                                                           fileHandle.freeSpaceList[rid.pageNum]);
                    inserted = true;
                    rid.slotNum = slotnum;

                    markInternalRid(pagebuffer, rid);
                    break;
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

                markInternalRid(pagebuffer, rid);
            }
        }

        fileHandle.writePage(rid.pageNum, pagebuffer);
        return RC::ok;
    }

    //ensure that rid is not deleted or is internal
    RC RecordBasedFileManager::readProjAttr(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid, const std::vector<std::string> &projAttr, void *data,
                                            unsigned &rlen) {
        unsigned dataFieldNum = projAttr.size();
        //field num convert to null indicator
        std::vector<char> nullIndic(dataFieldNum / 8 + (dataFieldNum % 8 == 0 ? 0 : 1), 0);
        //pointer of current record
        unsigned short dataPointer = nullIndic.size();

        std::vector<unsigned> loc;
        std::vector<AttrType> type;

        for (int p = 0; p < projAttr.size(); p++) {
            for (int i = 0; i < recordDescriptor.size(); i++) {
                if (recordDescriptor[i].name == projAttr[p]) {
                    loc.push_back(i);
                    type.push_back(recordDescriptor[i].type);
                    break;
                }
            }
        }

        //read record
        char *pagebuffer = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, pagebuffer);
//        if (checkRecordDeleted(pagebuffer, rid)) {
//            delete[]pagebuffer;
//            return RC::RECORD_HAS_DEL;
//        }
        RID realRid = {rid.pageNum, rid.slotNum};
        if (checkTombstone(pagebuffer, rid)) {
            accessRealRecord(fileHandle, rid, realRid);
        }
        if (realRid.pageNum != rid.pageNum) {
            fileHandle.readPage(realRid.pageNum, pagebuffer);

            if (checkRecordDeleted(pagebuffer, realRid)) {
                delete[]pagebuffer;
                return RC::RECORD_HAS_DEL;
            }
        }

        unsigned short offset = 0, len = 0;
        readSlotInfo(pagebuffer, realRid, offset, len);
        if (len >= 32768) {
            len -= 32768;
        }
        char *recordbuf = new char[len];
        memcpy(recordbuf, pagebuffer + offset, len);

        //fetch the end offset
        for (int p = 0; p < projAttr.size(); p++) {
            unsigned short endOff = 0;
            memcpy(&endOff, recordbuf + 2 * (loc[p] + 1), sizeof(short));
            if (endOff == 0) {
                //null field
                nullIndic[p / 8] = nullIndic[p / 8] | (1 << (7 - (p % 8)));
            } else {
                //find start offset: check whether field before is 0(indicate null), if yes, check the former one again
                unsigned startOff = 2 * (recordDescriptor.size() + 1);
                for (int i = loc[p] - 1; i >= 0; i--) {
                    unsigned off = 0;
                    memcpy(&off, recordbuf + 2 * (i + 1), sizeof(short));
                    if (off != 0) {
                        startOff = off;
                        break;
                    }
                }

                if (type[p] == TypeVarChar) {
                    unsigned totalLen = endOff - startOff;
                    memcpy((char *) data + dataPointer, &totalLen, sizeof(int));
                    dataPointer += sizeof(int);
                    memcpy((char *) data + dataPointer, recordbuf + startOff, totalLen);
                    dataPointer += totalLen;
                } else {
                    memcpy((char *) data + dataPointer, recordbuf + startOff, endOff - startOff);
                    dataPointer += sizeof(int);
                }
            }
        }

        for (int i = 0; i < nullIndic.size(); ++i) {
            memcpy((char *) data + i, &nullIndic[i], sizeof(char));
        }
        rlen = dataPointer;

        delete[]pagebuffer;
        delete[]recordbuf;
        return RC::ok;
    }

    // finish string comparison
    bool RBFM_ScanIterator::checkCondSatisfy(char *data) {
        char nullp = 0;
        memcpy(&nullp, data, sizeof(char));
        unsigned len = 0, recordP = 1;
        bool isNull = false;
        bool result = false;

        //without null pointer
        char *recordVal;

        if (nullp != '\0') {
            isNull = true;
            len = 1;
            recordVal = nullptr;
        } else {
            if (valType == TypeVarChar) {
                memcpy(&len, data + recordP, sizeof(int));
                recordP += sizeof(int);
                recordVal = new char[len];
                memcpy(recordVal, data + recordP, len);
            } else {
                len = sizeof(int);
                recordVal = new char[len];
                memcpy(recordVal, data + recordP, sizeof(int));
            }
        }

        if (compData == nullptr) {
            isNull = true;
            len = 1;
        }

        //todo efficiency change compare method
        switch (op) {
            case EQ_OP:
                if (compData == nullptr && recordVal == nullptr) {
                    result = true;
                } else if (compData == nullptr || recordVal == nullptr) {
                    result = false;
                } else if (memcmp(recordVal, compData, len) == 0) {
                    result = true;
                } else {
                    result = false;
                }
                break;

            case LT_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (valType == TypeInt) {
                    if (*((int *) recordVal) < *((int *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeReal) {
                    if (*((float *) recordVal) < *((float *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeVarChar) {
                    std::string recordStr(recordVal, len);
                    std::string compStr((char *) compData, valLen);
                    result = recordStr < compStr;
                }
                break;
            case LE_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (valType == TypeInt) {
                    if (*((int *) recordVal) <= *((int *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeReal) {
                    if (*((float *) recordVal) <= *((float *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeVarChar) {
                    std::string recordStr(recordVal, len);
                    std::string compStr((char *) compData, valLen);
                    result = recordStr <= compStr;
                }
                break;
            case GT_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (valType == TypeInt) {
                    if (*((int *) recordVal) > *((int *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeReal) {
                    if (*((float *) recordVal) > *((float *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeVarChar) {
                    std::string recordStr(recordVal, len);
                    std::string compStr((char *) compData, valLen);
                    result = recordStr > compStr;
                }
                break;
            case GE_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (valType == TypeInt) {
                    if (*((int *) recordVal) >= *((int *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeReal) {
                    if (*((float *) recordVal) >= *((float *) compData)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (valType == TypeVarChar) {
                    std::string recordStr(recordVal, len);
                    std::string compStr((char *) compData, valLen);
                    result = recordStr >= compStr;
                }
                break;
            case NE_OP:
                if (compData == nullptr && recordVal == nullptr) {
                    result = false;
                } else if (compData == nullptr || recordVal == nullptr) {
                    result = true;
                } else if (memcmp(recordVal, compData, len) != 0) {
                    result = true;
                } else {
                    result = false;
                }
                break;
            case NO_OP:
                result = true;
                break;
        }

        delete[]recordVal;
        if (result)return true;
        return false;
    }

    RC RecordBasedFileManager::getFollowedSlots(const char *pageData, const RID &curRid,
                                                std::vector<unsigned short> &followRids) {
        unsigned recordNum = getRecordNum(pageData);
        std::vector<std::pair<unsigned, unsigned >> pair_vec;
        if (recordNum == 0)return RC::ok;

        unsigned curOff = 0;
        memcpy(&curOff, pageData + PAGE_SIZE - 2 * sizeof(short) * (curRid.slotNum + 1), sizeof(short));

        unsigned short maxSlot = getMaxSlotNum(pageData, recordNum);
        for (unsigned short i = 1; i <= maxSlot; i++) {
            unsigned offset = 0;
            memcpy(&offset, pageData + PAGE_SIZE - 2 * sizeof(short) * (i + 1), sizeof(short));
            pair_vec.emplace_back(offset, i);
        }
        std::sort(pair_vec.begin(), pair_vec.end(),
                  [](const std::pair<unsigned, unsigned> &p1, const std::pair<unsigned, unsigned> &p2) {
                      return p1.first < p2.first;
                  });
        auto iter = std::lower_bound(pair_vec.begin(), pair_vec.end(), std::pair<unsigned, unsigned>{curOff, 0},
                                     [](const std::pair<unsigned, unsigned> &p1,
                                        const std::pair<unsigned, unsigned> &p2) {
                                         return p1.first < p2.first;
                                     });
        iter++;
        while (iter != pair_vec.end()) {
            followRids.push_back(iter->second);
            iter++;
        }
        return RC::ok;
    }

    bool RecordBasedFileManager::checkInternalRid(char *pageData, const RID &rid) {
        unsigned short offset = 0, len = 0;
        readSlotInfo(pageData, rid, offset, len);
        if (len >= 32768) {
            return true;
        }
        return false;
    }

    RC RecordBasedFileManager::markInternalRid(char *pageData, const RID &rid) {
        unsigned short offset = 0, len = 0;
        readSlotInfo(pageData, rid, offset, len);
        if (len >= 32768) {
            len -= 32768;
        }
        updateSlotInfo(pageData, rid, offset, len + 32768);
        return RC::ok;
    }
}