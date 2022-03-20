//
// Created by 晓斌 on 3/10/22.
//
#include "src/include/qe.h"
//#include "src/ix/tools.cpp"

namespace PeterDB {
    std::string getTableName(std::string &lhsAttr) {
        std::string delimiter = ".";
        std::string tableName = lhsAttr.substr(0, lhsAttr.find(delimiter));
        return tableName;
    }

    std::string getAttrName(std::string &lhsAttr) {
        std::string delimiter = ".";
        std::string AttrName = lhsAttr.substr(lhsAttr.find(delimiter) + 1, lhsAttr.length());
        return AttrName;
    }

    unsigned getRecordLen(std::vector<Attribute> &attrs, char *data) {
        unsigned short fieldNum = attrs.size();
        unsigned short nullByte = attrs.size() / 8 + (attrs.size() % 8 == 0 ? 0 : 1);
        unsigned dataPointer = nullByte;

        for (int i = 0; i < attrs.size(); i++) {
            if (RecordBasedFileManager::checkNull(data, i, 0)) {
                continue;
            }
            if (attrs[i].type == TypeVarChar) {
                int len = 0;
                memcpy(&len, data + dataPointer, sizeof(int));
                dataPointer += len + sizeof(int);
            } else {
                dataPointer += sizeof(int);
            }
        }
        return dataPointer;
    }

    bool Filter::check_condition(void *data, unsigned &dataLen) {

        unsigned rhsLen = 0;
        unsigned lhsLen = 0;

        return check_operator_value(static_cast<char *>(data), cond.rhsValue.type, lhsLen,
                                    static_cast<char *>(cond.rhsValue.data), cond.rhsValue.type, rhsLen);

    }

    void Filter::extract_data(AttrType type, const char *data, char *&record, unsigned &len) {
        if (type == TypeVarChar) {
            memcpy(&len, data + 1, sizeof(int));
            record = new char[len];
            memcpy(record, data + 1 + 4, len);
        } else {
            len = sizeof(int);
            record = new char[len];
            memcpy(record, data + 1, sizeof(int));
        }
    }

    void Filter::rhs_extract_data(AttrType type, const char *data, char *&record, unsigned &len) {
        if (type == TypeVarChar) {
            memcpy(&len, data, sizeof(int));
            record = new char[len];
            memcpy(record, data + 4, len);
        } else {
            len = sizeof(int);
            record = new char[len];
            memcpy(record, data, sizeof(int));
        }
    }

    //lhs: null + data;  rhs: only data
    bool
    Filter::check_operator_value(char *lhsData, AttrType lhsType, unsigned &lhsLen, char *rhsData, AttrType rhsType,
                                 unsigned &rhsLen) {
        char lhsNullp = '\0';
        memcpy(&lhsNullp, lhsData, sizeof(char));
//        memcpy(&rhsNullp, rhsData, sizeof(char));
        lhsLen = 0;
        rhsLen = 0;
        bool isNull = false;
        bool result = false;

        //without null pointer
        char *lhsVal = nullptr;
        char *rhsVal = nullptr;

        if (lhsNullp != '\0') {
            isNull = true;
            lhsLen = 1;
            lhsVal = nullptr;
        } else {
            extract_data(lhsType, lhsData, lhsVal, lhsLen);
        }

        rhs_extract_data(rhsType, rhsData, rhsVal, rhsLen);


//        if (rhsNullp != '\0') {
//            isNull = true;
//            rhsLen = 1;
//            rhsVal = nullptr;
//        } else {
//        extract_data(rhsType, rhsData, rhsVal, rhsLen);
//        }
//
//        if (cond.rhsValue.data == nullptr) {
//            isNull = true;
//            leftLen = 1;
//        }

        switch (cond.op) {
            case EQ_OP:
                if (rhsVal == nullptr && lhsVal == nullptr) {
                    result = true;
                } else if (rhsVal == nullptr || lhsVal == nullptr) {
                    result = false;
                } else if (lhsLen != rhsLen) {
                    result = false;
                } else if (memcmp(lhsVal, rhsVal, lhsLen) == 0) {
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
                if (cond.rhsValue.type == TypeInt) {
                    if (*((int *) lhsVal) < *((int *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeReal) {
                    if (*((float *) lhsVal) < *((float *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeVarChar) {
                    std::string recordStr(lhsVal, lhsLen);
                    std::string compStr(rhsVal, rhsLen);
                    result = recordStr < compStr;
                }
                break;
            case LE_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (cond.rhsValue.type == TypeInt) {
                    if (*((int *) lhsVal) <= *((int *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeReal) {
                    if (*((float *) lhsVal) <= *((float *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeVarChar) {
                    std::string recordStr(lhsVal, lhsLen);
                    std::string compStr(rhsVal, rhsLen);
                    result = recordStr <= compStr;
                }
                break;
            case GT_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (cond.rhsValue.type == TypeInt) {
                    if (*((int *) lhsVal) > *((int *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeReal) {
                    if (*((float *) lhsVal) > *((float *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeVarChar) {
                    std::string recordStr(lhsVal, lhsLen);
                    std::string compStr(rhsVal, rhsLen);
                    result = recordStr > compStr;
                }
                break;
            case GE_OP:
                if (isNull) {
                    result = false;
                    break;
                }
                if (cond.rhsValue.type == TypeInt) {
                    if (*((int *) lhsVal) >= *((int *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeReal) {
                    if (*((float *) lhsVal) >= *((float *) rhsVal)) {
                        result = true;
                    } else {
                        result = false;
                    }
                } else if (cond.rhsValue.type == TypeVarChar) {
                    std::string recordStr(lhsVal, lhsLen);
                    std::string compStr(rhsVal, rhsLen);
                    result = recordStr >= compStr;
                }
                break;
            case NE_OP:
                if (rhsVal == nullptr && lhsVal == nullptr) {
                    result = false;
                } else if (rhsVal == nullptr || lhsVal == nullptr) {
                    result = true;
                } else if (rhsLen != lhsLen) {
                    result = true;
                } else if (memcmp(lhsVal, rhsVal, lhsLen) != 0) {
                    result = true;
                } else {
                    result = false;
                }
                break;
            case NO_OP:
                result = true;
                break;
        }

        delete[]lhsVal;
        delete[]rhsVal;
        if (result)return true;
        return false;
    }

    RC BNLJoin::insertIntMap(void *key, char *data, unsigned dataLen) {
        int intkey = *(int *) key;
        char *record = new char[dataLen];
        memcpy(record, data, dataLen);
        intKeyMap[intkey].push_back(record);
        return RC::ok;
    }

    RC BNLJoin::insertFloatMap(void *key, char *data, unsigned dataLen) {
        float floatkey = *(float *) key;
        char *record = new char[dataLen];
        memcpy(record, data, dataLen);
        floatKeyMap[floatkey].push_back(record);
        return RC::ok;
    }

    RC BNLJoin::insertStringMap(void *key, char *data, unsigned dataLen) {
        int len = 0;
        memcpy(&len, key, sizeof(int));
        std::string str((char *) key + 4, len);
        char *record = new char[dataLen];
        memcpy(record, data, dataLen);
        stringKeyMap[str].push_back(record);
        return RC::ok;
    }

    bool BNLJoin::searchIntMap(void *key) {
        int intkey = *(int *) key;
        if (intKeyMap.find(intkey) != intKeyMap.end()) {
            for (auto records: intKeyMap[intkey]) {
                left_results.push(records);
            }
            return true;
        }
        return false;
    }

    bool BNLJoin::searchFloatMap(void *key) {
        float floatkey = *(float *) key;
        if (floatKeyMap.find(floatkey) != floatKeyMap.end()) {
            for (auto records: floatKeyMap[floatkey]) {
                left_results.push(records);
            }
            return true;
        }
        return false;
    }

    bool BNLJoin::searchStringMap(void *key) {
        int len = 0;
        memcpy(&len, key, sizeof(int));
        std::string str((char *) key + 4, len);
        if (stringKeyMap.find(str) != stringKeyMap.end()) {
            for (auto records: stringKeyMap[str]) {
                left_results.push(records);
            }
            return true;
        }
        return false;
    }

    RC BNLJoin::loadMap() {
        unsigned maxTotalLen = pageLimit * PAGE_SIZE;
        char recordBuf[PAGE_SIZE];
        std::vector<Attribute> attrs;
        left_input->getAttributes(attrs);
        while (left_input->getNextTuple(recordBuf) == RC::ok) {
            //deal with record
            std::vector<void *> vals;
            rm.formVector(attrs, vals, recordBuf);
            unsigned recordLen = getRecordLen(attrs, recordBuf);

            int count = 0;
            for (int i = 0; i < attrs.size(); ++i) {
                if (attrs[i].name == cond.lhsAttr) {
                    count = i;
                    break;
                }
            }

            //add to corresponding map
            if (keyType == TypeInt) {
                insertIntMap(vals[count], recordBuf, recordLen);
            } else if (keyType == TypeReal) {
                insertFloatMap(vals[count], recordBuf, recordLen);
            } else {
                insertStringMap(vals[count], recordBuf, recordLen);
            }

            maxTotalLen -= recordLen;
            if (maxTotalLen <= 0) {
                needLoad = false;
                return RC::ok;
            }
        }
        return RC(-1);
    }

    RC BNLJoin::combineResult(void *data) {
        std::vector<Attribute> rightAttrs;
        right_input->getAttributes(rightAttrs);
        std::vector<Attribute> leftAttrs;
        left_input->getAttributes(leftAttrs);

        char *temp = left_results.top();
        int len = getRecordLen(rightAttrs, temp);
        std::vector<Attribute> allAttrs;
        getAttributes(allAttrs);
        std::vector<void *> left_vals;
        std::vector<void *> right_vals;
        rm.formVector(leftAttrs, left_vals, temp);
        rm.formVector(rightAttrs, right_vals, right_result);
        for (auto val: right_vals) {
            left_vals.push_back(val);
        }
        rm.formData(allAttrs, left_vals, data);
        left_results.pop();
        return RC::ok;
    }

    RC attrProjection(void *data, std::vector<Attribute> &attrs, std::vector<Attribute> &projAttrs, char *projData) {
        unsigned projDataFieldNum = projAttrs.size();
        //field num convert to null indicator
        std::vector<char> projNullIndic(projDataFieldNum / 8 + (projDataFieldNum % 8 == 0 ? 0 : 1), 0);
        //pointer of current record
        unsigned short projDataPointer = projNullIndic.size();

        unsigned dataFieldNum = attrs.size();
        //field num convert to null indicator
        std::vector<char> nullIndic(dataFieldNum / 8 + (dataFieldNum % 8 == 0 ? 0 : 1), 0);
        //pointer of current record
        unsigned short dataPointer = nullIndic.size();

        std::vector<bool> loc(attrs.size(), false);
//        std::vector<AttrType> type(attrs.size(), TypeInt);

        for (int p = 0; p < projAttrs.size(); p++) {
            for (int i = 0; i < attrs.size(); i++) {
                if (attrs[i].name == projAttrs[p].name) {
                    loc[i] = true;
//                    type[i] = attrs[i].type;
                    break;
                }
            }
        }

        projData = new char[PAGE_SIZE];
        int projCount = 0;

        for (int i = 0; i < attrs.size(); ++i) {
            if (RecordBasedFileManager::checkNull((char *) data, i, 0)) {

            }
            if (attrs[i].type == TypeVarChar) {
                int len = 0;
                memcpy(&len, (char *) data + dataPointer, sizeof(int));
                if (loc[i]) {
                    memcpy(projData + projDataPointer, (char *) data + dataPointer, len + sizeof(int));
                    projDataPointer += len + sizeof(int);
                    projCount++;
                }
                dataPointer += len + sizeof(int);
            } else {

            }
        }
    }
}
