#include "src/include/qe.h"
#include "src/qe/tools.cpp"

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) : rm(RelationManager::instance()),
                                                                  idx(IndexManager::instance()) {
        this->input = input;
        this->cond = condition;
        lhsTableName = getTableName(cond.lhsAttr);
        lhsAttrName = getAttrName(cond.lhsAttr);
//        if (cond.bRhsIsAttr) {
//            rhsTableName = getTableName(cond.rhsAttr);
//            rhsAttrName = getAttrName(cond.rhsAttr);
//            rm.scan(rhsTableName, rhsAttrName, NO_OP, NULL, {rhsAttrName}, rightIter);
//        }
    }

    Filter::~Filter() {

    }

    RC Filter::getNextTuple(void *data) {
        char recordBuffer[PAGE_SIZE];
        std::vector<Attribute> attrs;
        rm.getAttributes(lhsTableName, attrs);

        while (input->getNextTuple(recordBuffer) == RC::ok) {
            std::vector<void *> vals;
            this->rm.formVector(attrs, vals, recordBuffer);
            int count = 0;
            for (int i = 0; i < attrs.size(); i++) {
                if (attrs[i].name == lhsAttrName) {
                    count = i;
                    break;
                }
            }
            unsigned dataLen = 0;
            void *oneEntry = nullptr;
            std::vector<Attribute> oneAttr = {attrs[count]};
            std::vector<const void *> oneData = {vals[count]};
            rm.formData(oneAttr, oneData, oneEntry);
            if (check_condition(oneEntry, dataLen)) {
                unsigned len = getRecordLen(attrs, recordBuffer);
                memcpy(data, recordBuffer, len);
                return RC::ok;
            }
        }

        return (RC) -1;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        input->getAttributes(attrs);
        return RC::ok;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) : rm(RelationManager::instance()) {

        input->getTableName(tableName);
        rm.getAttributes(tableName, attrAll);
        this->input = input;

        for (int i = 0; i < attrAll.size(); i++) {
            for (int j = 0; j < attrNames.size(); j++) {
                if (tableName + "." + attrAll[i].name == attrNames[j]) {
                    attrs.push_back(attrAll[i]);
                    break;
                }
            }
        }
    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        std::vector<void *> vals;
        char recordBuffer[PAGE_SIZE];

        RC re = input->getNextTuple(recordBuffer);
        if (re != RC::ok) {
            return re;
        }
        rm.formVector(attrAll, vals, recordBuffer);

        std::vector<const void *> dealedVals;
        for (int i = 0; i < attrAll.size(); ++i) {
            for (int j = 0; j < attrs.size(); ++j) {
                if (attrAll[i].name == attrs[j].name) {
                    dealedVals.push_back(vals[i]);
                    break;
                }
            }
        }

        rm.formData(attrs, dealedVals, data);
        return RC::ok;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->attrs;
        for (int i = 0; i < attrs.size(); ++i) {
            attrs[i].name = tableName + "." + attrs[i].name;
        }
        return RC::ok;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) {

    }

    BNLJoin::~BNLJoin() {

    }

    RC BNLJoin::getNextTuple(void *data) {
        return (RC) -1;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return (RC) -1;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return (RC) -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return (RC) -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return (RC) -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return (RC) -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return (RC) -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return (RC) -1;
    }
} // namespace PeterDB
