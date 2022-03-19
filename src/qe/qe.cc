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
        input->getAttributes(attrs);
        while (input->getNextTuple(recordBuffer) == RC::ok) {
//            std::vector<void *> vals;
//            this->rm.formVector(attrs, vals, recordBuffer);
            unsigned dataLen = 0;
            if (check_condition(recordBuffer, dataLen)) {
                memcpy((char *) data, recordBuffer, dataLen);
                return RC::ok;
            }
        }

        return (RC) -1;
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        input->getAttributes(attrs);
        return RC::ok;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {

    }

    Project::~Project() {

    }

    RC Project::getNextTuple(void *data) {
        return (RC) -1;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        return (RC) -1;
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
