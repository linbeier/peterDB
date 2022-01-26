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

    //done: records inserting should longer than 6bytes
    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {

        //construct record
        char *recordbuf = new char[PAGE_SIZE];
        unsigned short buflen = 0;
        unsigned short result = constructRecord(recordDescriptor, static_cast<const char *>(data), recordbuf, buflen);
        if (result > PAGE_SIZE - 2) {
            delete[]recordbuf;
            return RC::RECORD_TOO_BIG;
        }
        //padding record to 6 bytes, at least 6 bytes to store a tombstone, otherwise manager need to shift right when
        //record change to tombstone
        if (buflen < TOMBSTONE_SIZE) {
            buflen = TOMBSTONE_SIZE;
        }

        unsigned short totalpage = fileHandle.getNumberOfPages();
        if (totalpage == 0) {
            result = insertNewRecordPage(fileHandle);
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
                result = insertNewRecordPage(fileHandle);
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
        delete[]recordbuf;
        return RC::ok;
    }

    //First bit of a record indicates if this is a tombstone done: across tombstones
    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        //fetch page data
        char *pageData = new char[PAGE_SIZE];
        unsigned short recordOffset = 0;
        unsigned short recordLen = 0;
        char *recordData = new char[PAGE_SIZE];

        RC result = fileHandle.readPage(rid.pageNum, pageData);
        if (checkRecordDeleted(pageData, rid)) {
            delete[]pageData;
            delete[]recordData;
            return RC::RECORD_HAS_DEL;
        }
        RID realRid = {rid.pageNum, rid.slotNum};
        if (checkTombstone(pageData, rid)) {
            accessRealRecord(fileHandle, rid, realRid);
        }

        result = fileHandle.readPage(realRid.pageNum, pageData);
        if (result != RC::ok) {
            delete[]pageData;
            delete[]recordData;
            return result;
        }


        //fetch slot data
        readSlotInfo(pageData, realRid, recordOffset, recordLen);
        memcpy(recordData, pageData + recordOffset, recordLen);

        //construct record
        char *datap = static_cast<char *>(data);
        deconstructRecord(recordDescriptor, recordData, datap);

        delete[]pageData;
        delete[]recordData;
        return RC::ok;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {

        char *pageData = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, pageData);

        //first check if it has been deleted
        if (checkRecordDeleted(pageData, rid)) {
            delete[]pageData;
            return RC::RECORD_HAS_DEL;
        }

        if (checkTombstone(pageData, rid)) {
            RID r = {};
            readTombStone(pageData, rid, r);
            deleteRecord(fileHandle, recordDescriptor, r);
        }

        unsigned recordNum = getRecordNum(pageData);
        //Figure out where and length to move
        unsigned short recordOffset = 0, recordLen = 0;
        unsigned short moveOffset = 0, moveLen = 0;
        readSlotInfo(pageData, rid, recordOffset, recordLen);
        if (recordLen >= 32768) {
            recordLen -= 32768;
        }
        moveOffset = recordOffset + recordLen;

        //no records after current record
        if (rid.slotNum >= recordNum) {
            markDeleteRecord(pageData, rid);
            fileHandle.writePage(rid.pageNum, pageData);

            delete[]pageData;
            return RC::ok;
        }

        for (unsigned short i = rid.slotNum + 1; i <= recordNum; ++i) {
            RID r = {rid.pageNum, i};

            if (!checkRecordDeleted(pageData, r)) {
                unsigned short offset = 0, len = 0;
                readSlotInfo(pageData, r, offset, len);
                if (len >= 32768) {
                    len -= 32768;
                }
                offset -= recordLen;
                moveLen += len;
                updateSlotInfo(pageData, r, offset, len);

            } else {
                continue;
            }
        }

        markDeleteRecord(pageData, rid);
        memcpy(pageData + recordOffset, pageData + moveOffset, moveLen);
        fileHandle.writePage(rid.pageNum, pageData);

        delete[]pageData;
        return RC::ok;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        unsigned short fieldnum = recordDescriptor.size();
        unsigned short fieldbyte = recordDescriptor.size() / 8 + (recordDescriptor.size() % 8 == 0 ? 0 : 1);
        unsigned short datapointer = fieldbyte;

        char *nullbuffer = new char[fieldbyte];
        memcpy(nullbuffer, data, fieldbyte);

        for (int i = 0; i < fieldnum; ++i) {

            if (checkNull(nullbuffer, i, fieldnum)) {

                if (i != fieldnum - 1) {
                    out << recordDescriptor[i].name << ": " << "NULL" << ", ";
                } else {
                    out << recordDescriptor[i].name << ": " << "NULL" << "\n";
                }

            } else if (recordDescriptor[i].type == AttrType::TypeVarChar) {

                char bytelen[4];
                memcpy(bytelen, (char *) data + datapointer, 4);
                unsigned short len = *((unsigned short *) bytelen);
                datapointer += sizeof(int);

                char *value = new char[len];
                memcpy(value, (char *) data + datapointer, len);
                datapointer += len;
                if (i != fieldnum - 1) {
                    out << recordDescriptor[i].name << ": ";
                    out.write(value, len);
                    out << ", ";
                } else {
                    out << recordDescriptor[i].name << ": ";
                    out.write(value, len);
                    out << "\n";
                }
                delete[]value;

            } else if (recordDescriptor[i].type == AttrType::TypeInt) {
                char value[4];
                memcpy(value, (char *) data + datapointer, sizeof(int));
                datapointer += sizeof(int);
                int valuenum = *((int *) value);
                if (i != fieldnum - 1) {
                    out << recordDescriptor[i].name << ": " << valuenum << ", ";
                } else {
                    out << recordDescriptor[i].name << ": " << valuenum << "\n";
                }
            } else if (recordDescriptor[i].type == AttrType::TypeReal) {
                char value[4];
                memcpy(value, (char *) data + datapointer, sizeof(float));
                datapointer += sizeof(float);
                float valuenum = *((float *) value);
                if (i != fieldnum - 1) {
                    out << recordDescriptor[i].name << ": " << valuenum << ", ";
                } else {
                    out << recordDescriptor[i].name << ": " << valuenum << "\n";
                }
            }
        }
        delete[]nullbuffer;
        return RC::ok;
    }

    //First bit of length in slot indicates whether this is a tombstone
    // (not first bit of offset because we may change offset when we shift right/left)
    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        //jump across tombstones
        RID realRid{rid.pageNum, rid.slotNum};
        accessRealRecord(fileHandle, rid, realRid);

        char *pageData = new char[PAGE_SIZE];
        fileHandle.readPage(realRid.pageNum, pageData);

        unsigned short recordLen = 0;
        char *recordBuf = new char[PAGE_SIZE];
        constructRecord(recordDescriptor, static_cast<const char *>(data), recordBuf, recordLen);
        if (recordLen < TOMBSTONE_SIZE) {
            recordLen = TOMBSTONE_SIZE;
        }
        unsigned freeSpace = getFreeSpace(pageData);
        unsigned recordNum = getRecordNum(pageData);

        unsigned short oriRecordOff = 0, oriRecordLen = 0;
        readSlotInfo(pageData, realRid, oriRecordOff, oriRecordLen);
        unsigned short followedLen = getRecordOffset(pageData) - oriRecordOff - oriRecordLen;

        //followed records shift left
        if (recordLen < oriRecordLen) {

            unsigned short diff = oriRecordLen - recordLen;

            memcpy(pageData + oriRecordOff, recordBuf, recordLen);
            //shift left
            memcpy(pageData + oriRecordOff + recordLen, pageData + oriRecordOff + oriRecordLen, followedLen);

            unsigned short offset = oriRecordOff, len = recordLen;
            updateSlotInfo(pageData, {realRid.pageNum, realRid.slotNum}, offset, len);
            for (unsigned short i = realRid.slotNum + 1; i <= recordNum; i++) {

                readSlotInfo(pageData, {realRid.pageNum, i}, offset, len);
                offset -= diff;
                updateSlotInfo(pageData, {realRid.pageNum, i}, offset, len);

            }

        } else if (recordLen == oriRecordLen) {

            memcpy(pageData + oriRecordOff, recordBuf, recordLen);

        } else {

            unsigned short diff = recordLen - oriRecordLen;
            //no enough free space
            if (diff > freeSpace) {
                RID insertR = {};
                //change record to tombstone, all records followed shift left
                insertConstructedRecord(fileHandle, recordBuf, recordLen, insertR);

                //write tombstone
                char *tombstone = new char[TOMBSTONE_SIZE];
                unsigned short flag = 128;
                memcpy(tombstone, &flag, sizeof(char));
                memcpy(tombstone + 1, &insertR.pageNum, sizeof(int));
                memcpy(tombstone + sizeof(int) + 1, &insertR.slotNum, sizeof(short));
                memcpy(pageData + oriRecordOff, tombstone, TOMBSTONE_SIZE);
                delete[]tombstone;

                //shift left
                memcpy(pageData + oriRecordOff + TOMBSTONE_SIZE, pageData + oriRecordOff + oriRecordLen, followedLen);

                if (rid.pageNum == realRid.pageNum && rid.slotNum == realRid.slotNum) {
                    updateSlotInfo(pageData, realRid, oriRecordOff, TOMBSTONE_SIZE);
                } else {
                    //indicates this is an internal rid
                    updateSlotInfo(pageData, realRid, oriRecordOff, 32768 + TOMBSTONE_SIZE);
                }

                for (unsigned short i = realRid.slotNum + 1; i <= recordNum; i++) {
                    unsigned short offset = 0, len = 0;
                    readSlotInfo(pageData, {realRid.pageNum, i}, offset, len);
                    offset -= oriRecordLen - TOMBSTONE_SIZE;
                    updateSlotInfo(pageData, {realRid.pageNum, i}, offset, len);
                }

            } else {
                //shift right
                char *buffer = new char[followedLen];
                memcpy(buffer, pageData + oriRecordOff + oriRecordLen, followedLen);

                memcpy(pageData + oriRecordOff, recordBuf, recordLen);
                memcpy(pageData + oriRecordOff + recordLen, buffer, followedLen);

                updateSlotInfo(pageData, realRid, oriRecordOff, recordLen);
                for (unsigned short i = realRid.slotNum + 1; i <= recordNum; i++) {
                    unsigned short offset = 0, len = 0;
                    readSlotInfo(pageData, {realRid.pageNum, i}, offset, len);
                    offset += diff;
                    updateSlotInfo(pageData, {realRid.pageNum, i}, offset, len);
                }

                delete[]buffer;
            }
        }

        fileHandle.writePage(realRid.pageNum, pageData);

        delete[]recordBuf;
        delete[]pageData;
        return RC::ok;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        unsigned fieldNum = recordDescriptor.size();
        //find location of attribute
        unsigned loc = 0;
        AttrType type = TypeInt;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == attributeName) {
                loc = i;
                type = recordDescriptor[i].type;
                break;
            }
        }
        //read record
        char *pagebuffer = new char[PAGE_SIZE];
        fileHandle.readPage(rid.pageNum, pagebuffer);
        if (checkRecordDeleted(pagebuffer, rid)) {
            delete[]pagebuffer;
            return RC::RECORD_HAS_DEL;
        }
        RID realRid = {rid.pageNum, rid.slotNum};
        if (checkTombstone(pagebuffer, rid)) {
            accessRealRecord(fileHandle, rid, realRid);
        }
        if (realRid.pageNum != rid.pageNum) {
            fileHandle.readPage(realRid.pageNum, pagebuffer);
        }
        unsigned short offset = 0, len = 0;
        readSlotInfo(pagebuffer, rid, offset, len);
        if (len >= 32768) {
            len -= 32768;
        }
        char *recordbuf = new char[len];
        memcpy(recordbuf, pagebuffer + offset, len);

        //fetch the end offset
        unsigned short endOff = 0;
        memcpy(&endOff, recordbuf + 2 * (loc + 1), sizeof(short));
        if (endOff == 0) {
            char nullp = 0;
            memcpy(data, &nullp, sizeof(char));
        } else {
            //find start offset: check whether field before is 0(indicate null), if yes, check the former one again
            unsigned startOff = 2 * (fieldNum + 1);
            for (unsigned i = loc - 1; i >= 0; i--) {
                unsigned off = 0;
                memcpy(&off, recordbuf + 2 * (i + 1), sizeof(short));
                if (off != 0) {
                    startOff = off;
                    break;
                }
            }
            char nullp = 1;
            memcpy(data, &nullp, sizeof(char));
            if (type == TypeVarChar) {
                unsigned rlen = endOff - startOff;
                memcpy((char *) data + sizeof(char), &rlen, sizeof(int));
                memcpy((char *) data + sizeof(char) + sizeof(int), recordbuf + startOff, rlen);
            } else {
                memcpy((char *) data + sizeof(char), recordbuf + startOff, endOff - startOff);
            }
        }

        delete[]pagebuffer;
        delete[]recordbuf;
        return RC::ok;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {

        rbfm_ScanIterator.fd = fileHandle;
        rbfm_ScanIterator.op = compOp;
        rbfm_ScanIterator.projAttrs = attributeNames;
        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
        //start rid
        rbfm_ScanIterator.currentRid = RID{0, 1};
        AttrType compType = TypeInt;
        for (int i = 0; i < recordDescriptor.size(); i++) {
            if (recordDescriptor[i].name == conditionAttribute) {
                rbfm_ScanIterator.compAttr = conditionAttribute;
                compType = recordDescriptor[i].type;
                rbfm_ScanIterator.valType = compType;
                break;
            }
        }
//        for (int i = 0; i < attributeNames.size(); i++) {
//            for (int j = 0; j < recordDescriptor.size(); j++) {
//                if (attributeNames[i] == recordDescriptor[j].name) {
//                    rbfm_ScanIterator.projAttrs.push_back(j);
//                    break;
//                }
//            }
//        }
        if (compType == TypeReal || compType == TypeInt) {
            rbfm_ScanIterator.valLen = sizeof(int);
            rbfm_ScanIterator.compData = new int;
            memcpy(rbfm_ScanIterator.compData, value, sizeof(int));
        } else if (compType == TypeVarChar) {
            unsigned len = 0;
            memcpy(&len, value, sizeof(int));
            rbfm_ScanIterator.valLen = len;
            rbfm_ScanIterator.compData = new char[len];
            memcpy(rbfm_ScanIterator.compData, (char *) value + sizeof(int), len);

        }

        return RC::ok;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        //check if it meets the end
        if (currentRid.pageNum > fd.totalPage) {
            return static_cast<RC>RBFM_EOF;
        }
        char *pageBuf = new char[PAGE_SIZE];

        for (unsigned p = currentRid.pageNum; p < fd.totalPage; p++) {
            currentRid.pageNum = p;
            fd.readPage(currentRid.pageNum, pageBuf);
            unsigned short recordNum = 0;
            recordNum = RecordBasedFileManager::getRecordNum(pageBuf);

            for (unsigned short s = currentRid.slotNum; s <= recordNum; s++) {
                char *buffer = new char[PAGE_SIZE];
                currentRid.slotNum = s;
                RecordBasedFileManager::instance().readAttribute(fd, recordDescriptor, currentRid, compAttr, buffer);
                char nullp = 0;
                memcpy(&nullp, buffer, sizeof(char));
                if (nullp == 0) {
                    //not null
                    if (checkCondSatisfy(buffer + 1)) {
                        rid = {p, s};
                        //construct data from projAttr
                        unsigned resultLen = 0;
                        RecordBasedFileManager::instance().readProjAttr(fd, recordDescriptor, currentRid, projAttrs,
                                                                        data, resultLen);

                    }
                } else {
                    //null value
                    return static_cast<RC>RBFM_EOF;
                }
            }
        }
        return static_cast<RC>RBFM_EOF;
    }

    RC RBFM_ScanIterator::close() {

    }


} // namespace PeterDB

