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
        delete[]data;
        return max_table_id;
    }

    RC RelationManager::formRecordValue(const char *data, char *recordVal, AttrType type, unsigned len) {

        if (data == nullptr) {
            recordVal = nullptr;
        } else {

            if (type == TypeVarChar) {
                memcpy(recordVal, &len, sizeof(int));
                memcpy(recordVal + sizeof(int), data, len);
            } else {
                memcpy(recordVal, data, sizeof(int));
            }
        }
        return RC::ok;
    }

    //todo efficiency maintain fd_table, fd_col in rm class
    RC RelationManager::getTableId(const std::string &tableName,
                                   unsigned int &tableId) {
        FileHandle fd_table;
        RC re = rbfm->openFile(tableCatalog, fd_table);
        if (re != RC::ok) {
            return re;
        }
        std::vector<Attribute> tableAttr;
        formTableAttr(tableAttr);
        char *record = new char[tableName.length() + 10];
        formRecordValue(tableName.c_str(), record, TypeVarChar, tableName.length());
        RBFM_ScanIterator iter;
        rbfm->scan(fd_table, tableAttr, "table-name", EQ_OP,
                   record, {"table-id"}, iter);

        RID rid;
        char *data = new char[10];
        if (iter.getNextRecord(rid, data) == RC::ok) {
            memcpy(&tableId, data + 1, sizeof(int));
        } else {
            rbfm->closeFile(fd_table);
            delete[]record;
            delete[]data;
            return (RC) RM_EOF;
        }
        rbfm->closeFile(fd_table);
        delete[]record;
        delete[]data;
        return RC::ok;
    }

    RC
    RelationManager::getTableFile(const std::string &tableName, std::string &fileName) {
        FileHandle fd_table;
        RC re = rbfm->openFile(tableCatalog, fd_table);
        if (re != RC::ok) {
            return re;
        }

        std::vector<Attribute> tableAttr;
        formTableAttr(tableAttr);
        char *record = new char[tableName.length() + 10];
        formRecordValue(tableName.c_str(), record, TypeVarChar, tableName.length());
        RBFM_ScanIterator iter;
        rbfm->scan(fd_table, tableAttr, "table-name", EQ_OP,
                   record, {"file-name"}, iter);

        RID rid;
        char *data = new char[60];
        char *file = new char[51];
        unsigned len = 0;
        if (iter.getNextRecord(rid, data) == RC::ok) {
            memcpy(&len, data + 1, sizeof(int));
            memcpy(file, data + 1 + sizeof(int), len);
        } else {
            rbfm->closeFile(fd_table);

            delete[]data;
            delete[]file;
            delete[]record;
            return (RC) RM_EOF;
        }
        file[len] = '\0';
        fileName.assign(file);
        rbfm->closeFile(fd_table);

        delete[]data;
        delete[]file;
        delete[]record;
        return RC::ok;
    }

    RC RelationManager::getOneAttribute(const std::string &tableName, const std::string &attributeName,
                                        Attribute &attr) {
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        for (int i = 0; i < attrs.size(); ++i) {
            if (attrs[i].name == attributeName) {
                attr.name = attrs[i].name;
                attr.type = attrs[i].type;
                attr.length = attrs[i].length;
            }
        }
        return RC::ok;
    }

    RC RelationManager::formIndexAttr(std::vector<Attribute> &descriptor) {
        Attribute attr{"tableName", TypeVarChar, 50};
        descriptor.push_back(attr);
        attr = {"indexAttr", TypeVarChar, 50};
        descriptor.push_back(attr);
        attr = {"indexName", TypeVarChar, 50};
        descriptor.push_back(attr);
        attr = {"fileName", TypeVarChar, 50};
        descriptor.push_back(attr);
        return RC::ok;
    }

    RC
    RelationManager::formData(const std::vector<Attribute> &descriptor, std::vector<const void *> &values,
                              char *&data) {
        data = new char[PAGE_SIZE];
        unsigned dataFieldNum = descriptor.size();
        //field num convert to null indicator
        std::vector<char> nullIndic(dataFieldNum / 8 + (dataFieldNum % 8 == 0 ? 0 : 1), 0);
        //pointer of current record
        unsigned short dataPointer = nullIndic.size();

        for (int i = 0; i < values.size(); i++) {
            if (values[i] == nullptr) {
                //null field
                nullIndic[i / 8] = nullIndic[i / 8] | (1 << (7 - (i % 8)));
            } else {
                if (descriptor[i].type == TypeVarChar) {
                    //value type : const char*
                    int len = 0;
                    memcpy(&len, values[i], sizeof(int));
                    memcpy(data + dataPointer, values[i], sizeof(int) + len);
                    dataPointer += len + sizeof(int);
                } else {
                    memcpy(data + dataPointer, values[i], sizeof(int));
                    dataPointer += sizeof(int);
                }
            }
        }

        for (int i = 0; i < nullIndic.size(); ++i) {
            memcpy(data + i, &nullIndic[i], sizeof(char));
        }
        return RC::ok;
    }

    RC RelationManager::formVector(const std::vector<Attribute> &descriptor, std::vector<void *> &values,
                                   const char *data) {
        unsigned dataFieldNum = descriptor.size();
        //field num convert to null indicator
        std::vector<char> nullIndic(dataFieldNum / 8 + (dataFieldNum % 8 == 0 ? 0 : 1), 0);
        //pointer of current record
        unsigned short dataPointer = nullIndic.size();

        for (int i = 0; i < descriptor.size(); i++) {
            bool isNull = rbfm->checkNull(data, i, 0);
            if (isNull) {
                values.push_back(nullptr);
            } else {
                if (descriptor[i].type == TypeVarChar) {
                    int len = 0;
                    memcpy(&len, data + dataPointer, sizeof(int));
                    dataPointer += sizeof(int);
                    std::string str(data + dataPointer, len);
                    values.push_back(&str);
                    dataPointer += len;
                } else {
                    char *val = new char[sizeof(int)];
                    memcpy(val, data + dataPointer, sizeof(int));
                    values.push_back(val);
                    dataPointer += sizeof(int);
                }
            }
        }
        return RC::ok;
    }
}
