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
            std::vector<void *> vals;
            this->rm.formVector(attrs, vals, recordBuffer);
            int count = 0;
            for (int i = 0; i < attrs.size(); i++) {
                if (attrs[i].name == cond.lhsAttr) {
                    count = i;
                    break;
                }
            }
            unsigned dataLen = 0;
            void *oneEntry = new char[PAGE_SIZE];
            std::vector<Attribute> oneAttr = {attrs[count]};
            std::vector<void *> oneData = {vals[count]};
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

        input->fetchTableName(tableName);
        input->getAttributes(attrAll);
        this->input = input;

        for (int i = 0; i < attrNames.size(); i++) {
            for (int j = 0; j < attrAll.size(); j++) {
                if (attrAll[j].name == attrNames[i]) {
                    attrs.push_back(attrAll[j]);
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

        std::vector<void *> dealedVals;
        for (int i = 0; i < attrs.size(); ++i) {
            for (int j = 0; j < attrAll.size(); ++j) {
                if (attrAll[j].name == attrs[i].name) {
                    dealedVals.push_back(vals[j]);
                    break;
                }
            }
        }

        rm.formData(attrs, dealedVals, data);
        return RC::ok;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs = this->attrs;
//        for (int i = 0; i < attrs.size(); ++i) {
//            attrs[i].name = tableName + "." + attrs[i].name;
//        }
        return RC::ok;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages)
            : rm(RelationManager::instance()), right_result(nullptr) {
        pageLimit = numPages;
        left_input = leftIn;
        right_input = rightIn;
        cond = condition;

        keyType = condition.rhsValue.type;
        needLoad = true;
        cannotLoad = false;

        lhsTableName = getTableName(cond.lhsAttr);
        lhsAttrName = getAttrName(cond.lhsAttr);
        if (cond.bRhsIsAttr) {
            rhsTableName = getTableName(cond.rhsAttr);
            rhsAttrName = getAttrName(cond.rhsAttr);
        }
        if (needLoad) {
            RC re = loadMap();
            needLoad = false;
            if (re != RC::ok) {
                cannotLoad = true;
            }
        }
    }

    BNLJoin::~BNLJoin() {
        delete[]right_result;
    }

    //TODO:all attribute should be tableName.Attribute
    RC BNLJoin::getNextTuple(void *data) {

        std::vector<Attribute> rightAttrs;
        right_input->getAttributes(rightAttrs);
        std::vector<Attribute> leftAttrs;
        left_input->getAttributes(leftAttrs);

        if (!left_results.empty()) {
            combineResult(data);
            return RC::ok;
        }
        lp:
        char tarBuf[PAGE_SIZE];
        while (right_input->getNextTuple(tarBuf) == RC::ok) {
            std::vector<void *> vals;
            rm.formVector(rightAttrs, vals, tarBuf);
            unsigned tarLen = getRecordLen(rightAttrs, tarBuf);

            int count = 0;
            for (int i = 0; i < rightAttrs.size(); ++i) {
                if (rightAttrs[i].name == cond.rhsAttr) {
                    count = i;
                    break;
                }
            }

            bool satisfy = false;
            if (keyType == TypeInt) {
                satisfy = searchIntMap(vals[count]);
            } else if (keyType == TypeReal) {
                satisfy = searchFloatMap(vals[count]);
            } else {
                satisfy = searchStringMap(vals[count]);
            }

            if (satisfy) {
                delete[]right_result;
                right_result = new char[tarLen];
                memcpy(right_result, tarBuf, tarLen);
                combineResult(data);
                return RC::ok;
            }
        }
        needLoad = true;
        if (needLoad && !cannotLoad) {
            RC re = loadMap();
            needLoad = false;
            right_input->setIterator();
            if (re != RC::ok) {
                cannotLoad = true;
            }
            goto lp;
        }
        return (RC) -1;
    }


    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        std::vector<Attribute> rightAttrs;
        right_input->getAttributes(rightAttrs);
        std::vector<Attribute> leftAttrs;
        left_input->getAttributes(leftAttrs);

        attrs = leftAttrs;

        for (auto attr: rightAttrs) {
            attrs.push_back(attr);
        }
        return (RC) ok;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) : rm(
            RelationManager::instance()), idx(IndexManager::instance()) {
        left_input = leftIn;
        right_input = rightIn;
        cond = condition;

        hasSetCond = false;
        keyType = condition.rhsValue.type;
        left_result = new char[PAGE_SIZE];

        lhsTableName = getTableName(cond.lhsAttr);
        lhsAttrName = getAttrName(cond.lhsAttr);
        if (cond.bRhsIsAttr) {
            rhsTableName = getTableName(cond.rhsAttr);
            rhsAttrName = getAttrName(cond.rhsAttr);
        }
    }

    INLJoin::~INLJoin() {
        delete[]left_result;
    }

    RC INLJoin::getNextTuple(void *data) {
        std::vector<Attribute> rightAttrs;
        right_input->getAttributes(rightAttrs);
        std::vector<Attribute> leftAttrs;
        left_input->getAttributes(leftAttrs);

        char right_result[PAGE_SIZE];

        if (hasSetCond) {
            while (right_input->getNextTuple(right_result) == RC::ok) {
                combineResult(right_result, left_result, data);
                return RC::ok;
            }
        }

        while (left_input->getNextTuple(left_result) == RC::ok) {
            std::vector<void *> vals;
            rm.formVector(leftAttrs, vals, left_result);
            unsigned tarLen = getRecordLen(leftAttrs, left_result);

            int count = 0;
            for (int i = 0; i < leftAttrs.size(); ++i) {
                if (leftAttrs[i].name == cond.lhsAttr) {
                    count = i;
                    break;
                }
            }

            right_input->setIterator(vals[count], vals[count], true, true);

            while (right_input->getNextTuple(right_result) == RC::ok) {
                combineResult(right_result, left_result, data);
                hasSetCond = true;
                return RC::ok;
            }
//            bool satisfy = false;
//            if (keyType == TypeInt) {
//                satisfy = checkIntEqual(vals[count]);
//            } else if (keyType == TypeReal) {
//                satisfy = checkFloatEqual(vals[count]);
//            } else {
//                satisfy = checkStringEqual(vals[count]);
//            }
//
//            if (satisfy) {
//
//            }
        }


        return (RC) -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        std::vector<Attribute> leftAttrs;
        left_input->getAttributes(leftAttrs);
        std::vector<Attribute> rightAttrs;
        right_input->getAttributes(rightAttrs);

        attrs = leftAttrs;

        for (auto attr: rightAttrs) {
            attrs.push_back(attr);
        }
        return (RC) ok;
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

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) : rm(RelationManager::instance()) {
        min = std::numeric_limits<float>::max();
        max = 0;
        count = 0;
        sum = 0;
        avg = 0;
        this->input = input;
        this->aggAttr = aggAttr;
        this->op = op;
        hasCal = false;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) : rm(
            RelationManager::instance()) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        if (hasCal) {
            return (RC) -1;
        }
        std::vector<Attribute> attrs;
        input->getAttributes(attrs);
        int tar = 0;
        for (int i = 0; i < attrs.size(); ++i) {
            if (attrs[i].name == aggAttr.name) {
                tar = i;
                break;
            }
        }

        char recordBuf[PAGE_SIZE];
        while (input->getNextTuple(recordBuf) == RC::ok) {
            std::vector<void *> vals;
            rm.formVector(attrs, vals, recordBuf);
            if (aggAttr.type == TypeInt) {
                int key = *(int *) vals[tar];
                min = key < min ? key : min;
                max = key > max ? key : max;
                sum += key;
                count++;
                avg = sum / count;
            } else {
                float key = *(float *) vals[tar];
                min = key < min ? key : min;
                max = key > max ? key : max;
                sum += key;
                count++;
                avg = sum / count;
            }
        }

        char nullInd = '\0';
        switch (op) {
            case MIN:
                memcpy(data, &nullInd, 1);
                memcpy((char *) data + 1, &min, sizeof(float));
                break;
            case MAX:
                memcpy(data, &nullInd, 1);
                memcpy((char *) data + 1, &max, sizeof(float));
                break;
            case COUNT:
                memcpy(data, &nullInd, 1);
                memcpy((char *) data + 1, &count, sizeof(float));
                break;
            case SUM:
                memcpy(data, &nullInd, 1);
                memcpy((char *) data + 1, &sum, sizeof(float));
                break;
            case AVG:
                memcpy(data, &nullInd, 1);
                memcpy((char *) data + 1, &avg, sizeof(float));
                break;
        }
        hasCal = true;
        return (RC) ok;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        Attribute attr;
        std::string opName;
        switch (op) {
            case MIN:
                opName = "MIN";
                break;
            case MAX:
                opName = "MAX";
                break;
            case COUNT:
                opName = "COUNT";
                break;
            case SUM:
                opName = "SUM";
                break;
            case AVG:
                opName = "AVG";
                break;
        }
        attr.name = opName + "(" + aggAttr.name + ")";
        attr.type = TypeReal;
        attr.length = 4;
        attrs = {attr};
        return RC::ok;
    }
} // namespace PeterDB
