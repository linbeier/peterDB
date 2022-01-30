#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() : rbfm(nullptr), fd_table(nullptr), fd_col(nullptr) {
        rbfm = &RecordBasedFileManager::instance();

    }

    RelationManager::~RelationManager() = default;

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RC RelationManager::createCatalog() {
        fd_table = new FileHandle();
        fd_col = new FileHandle();

        unsigned table_id = 1;
        std::string table_name = "Tables";
        std::string file_name = "Tables";

        rbfm->createFile(table_name);
        rbfm->openFile(table_name, *fd_table);
        std::vector<Attribute> tableDescriptor;
        formTableAttr(tableDescriptor);

        char *tableRecord = new char[106];
        unsigned recordLen = 0;
        constructTableRecord(tableDescriptor, &table_id, table_name.c_str(), file_name.c_str(), tableRecord, recordLen);
        RID rid;
        rbfm->insertRecord(*fd_table, tableDescriptor, tableRecord, rid);

        table_id++;
        table_name = "Columns";
        file_name = "Columns";
        constructTableRecord(tableDescriptor, &table_id, table_name.c_str(), file_name.c_str(),
                             tableRecord, recordLen);
        rbfm->insertRecord(*fd_table, tableDescriptor, tableRecord, rid);

        //create columns table
        //Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)
        table_id = 1;
        std::string column_name = "table-id";
        unsigned column_type = AttrType::TypeInt;
        unsigned column_length = 4;
        unsigned column_position = 1;
//        FileHandle fd_col;

        rbfm->createFile("Columns");
        rbfm->openFile("Columns", *fd_col);
        std::vector<Attribute> columnDescriptor;
        formColumnsAttr(columnDescriptor);

        char *colRecord = new char[80];
        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 1;
        column_name = "table-name";
        column_type = AttrType::TypeVarChar;
        column_length = 50;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 1;
        column_name = "file-name";
        column_type = AttrType::TypeVarChar;
        column_length = 50;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "table-id";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position = 1;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-name";
        column_type = AttrType::TypeVarChar;
        column_length = 50;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-type";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-length";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        table_id = 2;
        column_name = "column-position";
        column_type = AttrType::TypeInt;
        column_length = 4;
        column_position++;

        constructColumnsRecord(columnDescriptor, &table_id, column_name.c_str(),
                               &column_type, &column_length, &column_position, colRecord);
        rbfm->insertRecord(*fd_col, columnDescriptor, colRecord, rid);

        delete[]colRecord;
        delete[]tableRecord;
        return RC::ok;
    }

    RC RelationManager::deleteCatalog() {

        if (rbfm->destroyFile("Tables") != RC::ok || rbfm->destroyFile("Columns") != RC::ok) {
            return RC::REMV_FILE_FAIL;
        }

        return RC::ok;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if (fd_table == nullptr || fd_col == nullptr) {
            return RC::FD_FAIL;
        }
        unsigned lastTableId = 0;
        std::vector<Attribute> tableDesc;
        formTableAttr(tableDesc);
        std::string tabName = "Tables";
        lastTableId = getLastTableId(tableDesc, *fd_table);

        unsigned table_id = lastTableId + 1;
        std::string table_name = tableName;
        std::string file_name = tableName;
        char *record = new char[150];
        unsigned recordLen = 0;
        constructTableRecord(tableDesc, &table_id, table_name.c_str(), file_name.c_str(),
                             record, recordLen);

        RID rid;
        rbfm->insertRecord(*fd_table, tableDesc, record, rid);

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
            rbfm->insertRecord(*fd_col, colDesc, record, rid);
        }

        //create table file
        rbfm->createFile(tableName);

        delete[]record;
        return RC::ok;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {

        if (rbfm->destroyFile(tableName) != RC::ok) {
            return RC::REMV_FILE_FAIL;
        }
        std::vector<Attribute> tabDesc;
        formTableAttr(tabDesc);
        RBFM_ScanIterator iter;

        //construct data
        char nullp = '\0';
        char *value = new char[tableName.length() + 10];
        memcpy(value, &nullp, sizeof(char));
        unsigned len = tableName.length();
        memcpy(value + 1, &len, sizeof(int));
        memcpy(value + 1 + sizeof(int), tableName.c_str(), len);
        rbfm->scan(*fd_table, tabDesc, "table-name", EQ_OP,
                   value, {"table-id"}, iter);

        RID rid;
        char *record = new char[100];
        unsigned table_id = 0;
        while (iter.getNextRecord(rid, record) == RC::ok) {
            rbfm->deleteRecord(*fd_table, tabDesc, rid);
            if (*record == '\0') {
                memcpy(&table_id, record + 1, sizeof(int));
            }
        }

        std::vector<Attribute> colDesc;
        formColumnsAttr(colDesc);
        memcpy(value, &nullp, 1);
        memcpy(value + 1, &table_id, sizeof(int));
        rbfm->scan(*fd_col, colDesc, "table-id", EQ_OP,
                   value, {"table-id"}, iter);

        while (iter.getNextRecord(rid, record) == RC::ok) {
            rbfm->deleteRecord(*fd_table, colDesc, rid);
        }
        return RC::ok;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        return static_cast<RC>(RM_EOF);
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        return static_cast<RC>(RM_EOF);
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) { return static_cast<RC>(RM_EOF); }

    RC RM_ScanIterator::close() { return static_cast<RC>(RM_EOF); }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return static_cast<RC>(RM_EOF);
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return static_cast<RC>(RM_EOF);
    }

} // namespace PeterDB