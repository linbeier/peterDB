#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() : rbfm(nullptr), tableCatalog("Tables"),
                                         columnsCatalog("Columns"), idx(nullptr), pfm(nullptr),
                                         indexTable("index_table") {
        rbfm = &RecordBasedFileManager::instance();
        idx = &IndexManager::instance();
        pfm = &PagedFileManager::instance();
    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        FileHandle fd_table;
        FileHandle fd_col;

        unsigned table_id = 1;
        std::string table_name = "Tables";
        std::string file_name = "Tables";

        rbfm->createFile(table_name);
        rbfm->openFile(table_name, fd_table);
        std::vector<Attribute> tableDescriptor;
        formTableAttr(tableDescriptor);

        char *tableRecord = new char[106];
        unsigned recordLen = 0;
        constructTableRecord(tableDescriptor, &table_id, table_name.c_str(), file_name.c_str(), tableRecord, recordLen);
        RID rid;
        rbfm->insertRecord(fd_table, tableDescriptor, tableRecord, rid);

        table_id++;
        table_name = "Columns";
        file_name = "Columns";
        constructTableRecord(tableDescriptor, &table_id, table_name.c_str(), file_name.c_str(),
                             tableRecord, recordLen);
        rbfm->insertRecord(fd_table, tableDescriptor, tableRecord, rid);

        //create columns table
        //Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
        table_id = 1;
        std::string column_name = "table-id";
        unsigned column_type = AttrType::TypeInt;
        unsigned column_length = 4;
        unsigned column_position = 1;
//        FileHandle fd_col;

        rbfm->createFile("Columns");
        rbfm->openFile("Columns", fd_col);
        std::vector<Attribute> columnDescriptor;
        formColumnsAttr(columnDescriptor);

        char *colRecord = new char[80];
        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 1;
        column_name = "table-name";
        column_type = AttrType::TypeVarChar;
        column_length = 50;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 1;
        column_name = "file-name";
        column_type = AttrType::TypeVarChar;
        column_length = 50;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "table-id";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position = 1;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-name";
        column_type = AttrType::TypeVarChar;
        column_length = 50;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-type";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-length";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-position";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(fd_col, columnDescriptor, colRecord, rid);

        //close files
        rbfm->closeFile(fd_table);
        rbfm->closeFile(fd_col);

        delete[]colRecord;
        delete[]tableRecord;
        return RC::ok;
    }

    RC RelationManager::deleteCatalog() {

        if (rbfm->destroyFile(tableCatalog) != RC::ok || rbfm->destroyFile(columnsCatalog) != RC::ok ||
            rbfm->destroyFile(indexTable) != RC::ok) {
            return RC::REMV_FILE_FAIL;
        }

        return RC::ok;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if (tableName == tableCatalog || tableName == columnsCatalog) {
            return RC::CREA_FILE_FAIL;
        }
        FileHandle fd_table;
        FileHandle fd_col;
        RC re = rbfm->openFile(tableCatalog, fd_table);
        if (re != RC::ok) {
            return re;
        }
        re = rbfm->openFile(columnsCatalog, fd_col);
        if (re != RC::ok) {
            return re;
        }

        unsigned lastTableId = 0;
        std::vector<Attribute> tableDesc;
        formTableAttr(tableDesc);
        lastTableId = getLastTableId(tableDesc, fd_table);

        unsigned table_id = lastTableId + 1;
        std::string table_name = tableName;
        std::string file_name = tableName;
        char *record = new char[150];
        unsigned recordLen = 0;
        constructTableRecord(tableDesc, &table_id, table_name.c_str(), file_name.c_str(),
                             record, recordLen);

        RID rid;
        rbfm->insertRecord(fd_table, tableDesc, record, rid);

        //insert attributes into columns
        //Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
        std::vector<Attribute> colDesc;
        formColumnsAttr(colDesc);
        std::string colName = "Columns";
        unsigned column_position = 0;
        for (auto v: attrs) {
            column_position++;
            unsigned column_type = (unsigned) v.type;
            constructColumnsRecord(colDesc, &table_id, v.name.c_str(), &column_type,
                                   &v.length, &column_position, record);
            rbfm->insertRecord(fd_col, colDesc, record, rid);
        }

        //create table file
        rbfm->createFile(tableName);

        //close catalog
        rbfm->closeFile(fd_table);
        rbfm->closeFile(fd_col);
        delete[]record;
        return RC::ok;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if (tableName == tableCatalog || tableName == columnsCatalog) {
            return RC::REMV_FILE_FAIL;
        }
        FileHandle fd_table;
        FileHandle fd_col;
        RC re = rbfm->openFile(tableCatalog, fd_table);
        if (re != RC::ok) {
            return re;
        }
        re = rbfm->openFile(columnsCatalog, fd_col);
        if (re != RC::ok) {
            return re;
        }
        RM_ScanIterator rm_iter;
        char *tableNameBuf = new char[tableName.length() + sizeof(int)];
        formRecordValue(tableName.c_str(), tableNameBuf, TypeVarChar, tableName.length());
        this->scan(this->indexTable, "tableName", EQ_OP, tableNameBuf, {"indexAttr"}, rm_iter);
        RID tempRid;
        char *data = new char[PAGE_SIZE];
        std::vector<std::string> attrs;
        while (rm_iter.getNextTuple(tempRid, data) == RC::ok) {
            //ignore null indicator
            int len = 0;
            memcpy(&len, data + 1, sizeof(int));
            std::string indexAttr(data + 5, len);
            attrs.push_back(indexAttr);
        }
        for (const auto &attr: attrs) {
            this->destroyIndex(tableName, attr);
        }

        if (rbfm->destroyFile(tableName) != RC::ok) {
            return RC::REMV_FILE_FAIL;
        }
        std::vector<Attribute> tabDesc;
        formTableAttr(tabDesc);
        RBFM_ScanIterator iter;

        //construct data
        char *value = new char[tableName.length() + sizeof(int)];
        formRecordValue(tableName.c_str(), value, TypeVarChar, tableName.length());
        rbfm->scan(fd_table, tabDesc, "table-name", EQ_OP,
                   value, {"table-id"}, iter);

        RID rid;
        char *record = new char[150];
        unsigned table_id = 0;
        while (iter.getNextRecord(rid, record) == RC::ok) {
            rbfm->deleteRecord(fd_table, tabDesc, rid);
            if (*record == '\0') {
                memcpy(&table_id, record + 1, sizeof(int));
            }
        }

        std::vector<Attribute> colDesc;
        formColumnsAttr(colDesc);

        rbfm->scan(fd_col, colDesc, "table-id", EQ_OP,
                   &table_id, {"table-id"}, iter);

        while (iter.getNextRecord(rid, record) == RC::ok) {
            rbfm->deleteRecord(fd_col, colDesc, rid);
        }

        rbfm->closeFile(fd_table);
        rbfm->closeFile(fd_col);
        delete[]value;
        delete[]record;
        return RC::ok;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        FileHandle fd_col;
        RC re = rbfm->openFile(columnsCatalog, fd_col);
        if (re != RC::ok) {
            return re;
        }

        unsigned tableId = 0;
        if (getTableId(tableName, tableId) != RC::ok) {
            rbfm->closeFile(fd_col);
            return (RC) RM_EOF;
        }

        std::vector<Attribute> colAttr;
        formColumnsAttr(colAttr);

        RBFM_ScanIterator iter;
        rbfm->scan(fd_col, colAttr, {"table-id"}, EQ_OP,
                   &tableId, {"column-name", "column-type", "column-length"}, iter);
        RID rid;
        char *record = new char[PAGE_SIZE];
        while (iter.getNextRecord(rid, record) == RC::ok) {
            Attribute tempAttr;
            unsigned recordP = 1;
            unsigned len = 0;

            memcpy(&len, record + recordP, sizeof(int));
            recordP += sizeof(int);
            char *name = new char[len + 1];
            memcpy(name, record + recordP, len);
            name[len] = '\0';
            tempAttr.name.assign(name);
            recordP += len;

            unsigned type = 0;
            memcpy(&type, record + recordP, sizeof(int));
            tempAttr.type = static_cast<AttrType>(type);
            recordP += sizeof(int);

            len = 0;
            memcpy(&len, record + recordP, sizeof(int));
            tempAttr.length = len;

            attrs.push_back(tempAttr);
            delete[]name;
        }

        rbfm->closeFile(fd_col);
        delete[]record;
        return RC::ok;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (tableName == tableCatalog || tableName == columnsCatalog) {
            return RC::REMV_FILE_FAIL;
        }
        std::vector<Attribute> attr;
        std::string fileName;
        FileHandle fd;

        getAttributes(tableName, attr);
        getTableFile(tableName, fileName);
        RC re = rbfm->openFile(fileName, fd);
        if (re != RC::ok) {
            return re;
        }
        re = rbfm->insertRecord(fd, attr, data, rid);
        if (re != RC::ok) {
            rbfm->closeFile(fd);
            return re;
        }
        rbfm->closeFile(fd);
        return RC::ok;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (tableName == tableCatalog || tableName == columnsCatalog) {
            return RC::REMV_FILE_FAIL;
        }
        std::vector<Attribute> attr;
        std::string fileName;
        FileHandle fd;

        getAttributes(tableName, attr);
        getTableFile(tableName, fileName);
        RC re = rbfm->openFile(fileName, fd);
        if (re != RC::ok) {
            return re;
        }
        re = rbfm->deleteRecord(fd, attr, rid);
        if (re != RC::ok) {
            rbfm->closeFile(fd);
            return re;
        }
        rbfm->closeFile(fd);
        return RC::ok;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (tableName == tableCatalog || tableName == columnsCatalog) {
            return RC::REMV_FILE_FAIL;
        }
        std::vector<Attribute> attr;
        std::string fileName;
        FileHandle fd;

        getAttributes(tableName, attr);
        getTableFile(tableName, fileName);
        RC re = rbfm->openFile(fileName, fd);
        if (re != RC::ok) {
            return re;
        }
        re = rbfm->updateRecord(fd, attr, data, rid);
        if (re != RC::ok) {
            rbfm->closeFile(fd);
            return re;
        }
        rbfm->closeFile(fd);
        return RC::ok;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        std::vector<Attribute> attr;
        std::string fileName;
        FileHandle fd;

        getAttributes(tableName, attr);
        getTableFile(tableName, fileName);
        RC re = rbfm->openFile(fileName, fd);
        if (re != RC::ok) {
            return re;
        }
        re = rbfm->readRecord(fd, attr, rid, data);
        if (re != RC::ok) {
            rbfm->closeFile(fd);
            return re;
        }
        rbfm->closeFile(fd);
        return RC::ok;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        RC re = rbfm->printRecord(attrs, data, out);
        if (re != RC::ok) {
            return re;
        }
        return RC::ok;
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        std::vector<Attribute> attr;
        std::string fileName;
        FileHandle fd;

        getAttributes(tableName, attr);
        getTableFile(tableName, fileName);
        rbfm->openFile(fileName, fd);
        RC re = rbfm->readAttribute(fd, attr, rid, attributeName, data);
        if (re != RC::ok) {
            rbfm->closeFile(fd);
            return re;
        }
        rbfm->closeFile(fd);
        return RC::ok;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        std::vector<Attribute> attr;
        std::string fileName;
        FileHandle fd;

        getAttributes(tableName, attr);
        getTableFile(tableName, fileName);
        RC re = rbfm->openFile(fileName, fd);
        if (re != RC::ok) {
            return re;
        }
        rm_ScanIterator.iter = new RBFM_ScanIterator();
        re = rbfm->scan(fd, attr, conditionAttribute, compOp,
                        value, attributeNames, *rm_ScanIterator.iter);
        if (re != RC::ok) {
            rbfm->closeFile(fd);
            return re;
        }
//        rbfm->closeFile(fd);
        return RC::ok;
    }

    RM_ScanIterator::RM_ScanIterator() : iter(nullptr) {

    }

    RM_ScanIterator::~RM_ScanIterator() {
        if (iter != nullptr) {
            delete iter;
            iter = nullptr;
        }
    }

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        RC re = iter->getNextRecord(rid, data);
        if (re != RC::ok) {
            return re;
        }
        return RC::ok;
    }

    RC RM_ScanIterator::close() {
        if (iter != nullptr) {
            RC re = iter->close();
            if (re != RC::ok) {
                return re;
            }
        }
        return RC::ok;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return static_cast<RC>(RM_EOF);
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return static_cast<RC>(RM_EOF);
    }

} // namespace PeterDB