#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <unordered_map>
#include <queue>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual RC fetchTableName(std::string &tableName) = 0;

        virtual ~Iterator() = default;
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr: attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute: attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        RC fetchTableName(std::string &tableName) override {
            tableName = this->tableName;
            return RC::ok;
        }

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = NULL) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;


            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute: attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
        };

        RC fetchTableName(std::string &tableName) override {
            tableName = this->tableName;
            return RC::ok;
        }

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        // Filter operator
        std::string lhsTableName;
        std::string lhsAttrName;
        std::string rhsTableName;
        std::string rhsAttrName;
        Condition cond;

        Iterator *input;
        RelationManager &rm;
        IndexManager &idx;
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        bool check_condition(void *data, unsigned &dataLen);

        bool check_operator_value(char *lhsData, AttrType lhsType, unsigned &lhsLen, char *rhsData, AttrType rhsType,
                                  unsigned &rhsLen);

        void extract_data(AttrType type, const char *data, char *&record, unsigned &len);

        void rhs_extract_data(AttrType type, const char *data, char *&record, unsigned &len);

//        unsigned getRecordLen(std::vector<Attribute> &attrs, char *data);

        RC fetchTableName(std::string &tableName) override { return (RC) -1; };
    };

    class Project : public Iterator {
        // Projection operator
        RelationManager &rm;
        std::vector<Attribute> attrs;
        std::vector<Attribute> attrAll;
        std::string tableName;
        Iterator *input;
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        RC fetchTableName(std::string &tableName) override { return (RC) -1; };

    };

    class BNLJoin : public Iterator {
        // Block nested-loop join operator
        unsigned pageLimit;
        std::string lhsTableName;
        std::string lhsAttrName;
        std::string rhsTableName;
        std::string rhsAttrName;

        Condition cond;
        AttrType keyType;
        bool needLoad;
        bool cannotLoad;

        std::unordered_map<int, std::vector<char *>> intKeyMap;
        std::unordered_map<float, std::vector<char *>> floatKeyMap;
        std::unordered_map<std::string, std::vector<char *>> stringKeyMap;

        std::priority_queue<char *> left_results;
        char *right_result;

        Iterator *left_input;
        TableScan *right_input;
        RelationManager &rm;
//        IndexManager &idx;
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        RC fetchTableName(std::string &tableName) override { return (RC) -1; };

        RC insertIntMap(void *key, char *data, unsigned dataLen);

        RC insertFloatMap(void *key, char *data, unsigned dataLen);

        RC insertStringMap(void *key, char *data, unsigned dataLen);

        bool searchIntMap(void *key);

        bool searchFloatMap(void *key);

        bool searchStringMap(void *key);

        RC loadMap();

        RC combineResult(void *data);
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
        std::string lhsTableName;
        std::string lhsAttrName;
        std::string rhsTableName;
        std::string rhsAttrName;

        Condition cond;
        AttrType keyType;

        void *right_key;
        char *left_result;

        Iterator *left_input;
        IndexScan *right_input;
        RelationManager &rm;
        IndexManager &idx;
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        RC fetchTableName(std::string &tableName) override { return (RC) -1; };

        bool checkIntEqual(void *key);

        bool checkFloatEqual(void *key);

        bool checkStringEqual(void *key);

        RC combineResult(char *right_result, char *left_result, void *data);

    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        RC fetchTableName(std::string &tableName) override { return (RC) -1; };
    };

    class Aggregate : public Iterator {
        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;

        RC fetchTableName(std::string &tableName) override { return (RC) -1; };
    };
} // namespace PeterDB

#endif // _qe_h_