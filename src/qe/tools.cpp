//
// Created by 晓斌 on 3/10/22.
//
#include "src/include/qe.h"

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
