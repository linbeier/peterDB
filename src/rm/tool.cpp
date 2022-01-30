//
// Created by 晓斌 on 1/26/22.
//

#include "src/include/rm.h"

namespace PeterDB {

    //Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
    RC RelationManager::formTableAttr(std::vector<Attribute> &descriptor) {
        Attribute attr{"table-id", TypeInt, 4};
        descriptor.push_back(attr);
        attr = {"table-name", TypeVarChar, 50};
        descriptor.push_back(attr);
        attr = {"file-name", TypeVarChar, 50};
        descriptor.push_back(attr);

        return RC::ok;
    }

    //Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
    RC RelationManager::formColumnsAttr(std::vector<Attribute> &descriptor) {
        Attribute attr{"table-id", TypeInt, 4};
        descriptor.push_back(attr);
        attr = {"column-name", TypeVarChar, 50};
        descriptor.push_back(attr);
        attr = {"column-type", TypeInt, 4};
        descriptor.push_back(attr);
        attr = {"column-length", TypeInt, 4};
        descriptor.push_back(attr);
        attr = {"column-position", TypeInt, 4};
        descriptor.push_back(attr);

        return RC::ok;
    }

    //[n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field]
    RC RelationManager::constructTableRecord(std::vector<Attribute> &descriptor, unsigned int *table_id,
                                             const char *table_name, const char *file_name, char *data,
                                             unsigned &dataLen) {
        unsigned fieldNum = descriptor.size();
        //field num convert to null indicator
        char nullIndic = 0;
        //pointer of current record
        unsigned short recordPointer = 1;
        if (table_id == nullptr) {
            nullIndic = nullIndic | (1 << 7);
        } else {
            memcpy(data + recordPointer, table_id, sizeof(int));
            recordPointer += sizeof(int);
        }
        if (table_name == nullptr) {
            nullIndic = nullIndic | (1 << 6);
        } else {
            unsigned len = strlen(table_name);
            memcpy(data + recordPointer, &len, sizeof(int));
            recordPointer += sizeof(int);
            memcpy(data + recordPointer, table_name, len);
            recordPointer += len;
        }
        if (file_name == nullptr) {
            nullIndic = nullIndic | (1 << 5);
        } else {
            unsigned len = strlen(file_name);
            memcpy(data + recordPointer, &len, sizeof(int));
            recordPointer += sizeof(int);
            memcpy(data + recordPointer, file_name, len);
            recordPointer += len;
        }
        memcpy(data, &nullIndic, sizeof(char));
        dataLen = recordPointer;
        return RC::ok;
    }

    RC RelationManager::constructColumnsRecord(std::vector<Attribute> &descriptor, unsigned int *table_id,
                                               const char *column_name, unsigned int *column_type,
                                               unsigned int *column_length, unsigned int *column_position, char *data) {
        unsigned fieldNum = descriptor.size();
        //field num convert to null indicator
        char nullIndic = '\0';
        //pointer of current record
        unsigned short recordPointer = 1;
        if (table_id == nullptr) {
            nullIndic = nullIndic | (1 << 7);
        } else {
            memcpy(data + recordPointer, table_id, sizeof(int));
            recordPointer += sizeof(int);
        }
        if (column_name == nullptr) {
            nullIndic = nullIndic | (1 << 6);
        } else {
            unsigned len = strlen(column_name);
            memcpy(data + recordPointer, &len, sizeof(int));
            recordPointer += sizeof(int);
            memcpy(data + recordPointer, column_name, len);
            recordPointer += len;
        }
        if (column_type == nullptr) {
            nullIndic = nullIndic | (1 << 5);
        } else {
            memcpy(data + recordPointer, column_type, sizeof(int));
            recordPointer += sizeof(int);
        }
        if (column_length == nullptr) {
            nullIndic = nullIndic | (1 << 4);
        } else {
            memcpy(data + recordPointer, column_length, sizeof(int));
            recordPointer += sizeof(int);
        }
        if (column_position == nullptr) {
            nullIndic = nullIndic | (1 << 3);
        } else {
            memcpy(data + recordPointer, column_position, sizeof(int));
            recordPointer += sizeof(int);
        }
        memcpy(data, &nullIndic, sizeof(char));

        return RC::ok;
    }

    unsigned RelationManager::getLastTableId(std::vector<Attribute> &descriptor, FileHandle &fd) {

        int value = 0;
        RBFM_ScanIterator iter;
        rbfm->scan(fd, descriptor, "table-id", NO_OP,
                   &value, {"table-id"}, iter);

        RID rid;
        unsigned max_table_id = 0;
        unsigned table_id = 0;
        char null = '\0';
        char *data = new char[5];
        while (iter.getNextRecord(rid, data) == RC::ok) {
            memcpy(&null, data, sizeof(char));
            if (null == '\0') {
                memcpy(&table_id, data + 1, sizeof(int));
                max_table_id = table_id > max_table_id ? table_id : max_table_id;
            }
        }
        return max_table_id;
    }


}
