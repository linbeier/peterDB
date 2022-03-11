#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "src/include/rbfm.h"
#include "src/include/ix.h"

namespace PeterDB {
#define RM_EOF (-1)  // end of a scan operator

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RBFM_ScanIterator *iter;

        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        IX_ScanIterator *idx;

        RM_IndexScanIterator() {
            idx = nullptr;
        };    // Constructor
        ~RM_IndexScanIterator() {

        };    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key) {
            return idx->getNextEntry(rid, key);
        };    // Get next matching entry
        RC close() {
            return idx->close();
        };                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    public:
        RecordBasedFileManager *rbfm;

        std::string tableCatalog;
        std::string columnsCatalog;

        IndexManager *idx;

        static RelationManager &instance();

        RC createCatalog();

        RC deleteCatalog();

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);


        //tool functions

        RC formTableAttr(std::vector<Attribute> &descriptor);

        RC formColumnsAttr(std::vector<Attribute> &descriptor);

        RC constructTableRecord(std::vector<Attribute> &descriptor, unsigned *table_id, const char *table_name,
                                const char *file_name, char *data, unsigned &dataLen);

        RC constructColumnsRecord(std::vector<Attribute> &descriptor, unsigned *table_id, const char *column_name,
                                  unsigned *column_type, unsigned *column_length, unsigned *column_position,
                                  char *data);

        unsigned getLastTableId(std::vector<Attribute> &descriptor, FileHandle &fd);

        RC
        getTableId(const std::string &tableName, unsigned &tableId);

        RC formRecordValue(const char *data, char *recordVal, AttrType type, unsigned len);

        RC getTableFile(const std::string &tableName, std::string &fileName);

        RC getOneAttribute(const std::string &tableName, const std::string &attributeName, Attribute &attr);

        // QE IX related
        RC createIndex(const std::string &tableName, const std::string &attributeName) {
            std::string fileName = tableName + "." + attributeName + ".idx";
            return idx->createFile(fileName);
        };

        RC destroyIndex(const std::string &tableName, const std::string &attributeName) {
            std::string fileName = tableName + "." + attributeName + ".idx";
            return idx->destroyFile(fileName);
        };

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator) {
            std::string fileName = tableName + "." + attributeName + ".idx";
            IXFileHandle fh;
            idx->openFile(fileName, fh);
            Attribute attr;
            getOneAttribute(tableName, attributeName, attr);
            return idx->scan(fh, attr, lowKey, highKey, lowKeyInclusive, highKeyInclusive,
                             *rm_IndexScanIterator.idx);
        };

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_