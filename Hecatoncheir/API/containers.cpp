#include "containers.h"

#include "def.h"
#include "utils.h"
#include "../include/containers.h"

namespace hec
{
    SpatialQueries spatialQueries;
    SupportedQueryResultTypes queryResultTypes;

    JoinQuery::JoinQuery(DatasetID Rid, DatasetID Sid, int id, std::string predicateStr) {
        QueryType queryType = mapping::queryTypeStrToInt(predicateStr);
        if (queryType != Q_NONE) {
            this->Rid = Rid;
            this->Sid = Sid;
            this->queryID = id;
            this->queryType = queryType;
            this->resultType = QR_COUNT;
            return;
        }
        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid predicate for a join query:", predicateStr);
    }

    JoinQuery::JoinQuery(DatasetID Rid, DatasetID Sid, int id, std::string predicateStr, std::string resultTypeStr) {
        QueryType queryType = mapping::queryTypeStrToInt(predicateStr);
        hec::QueryResultType queryResultTypes = mapping::queryResultTypeStrToInt(resultTypeStr);
        if (queryType != Q_NONE) {
            this->Rid = Rid;
            this->Sid = Sid;
            this->queryID = id;
            this->queryType = queryType;
            this->resultType = queryResultTypes;
            return;
        }
        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid predicate for a join query:", predicateStr);
    }

    JoinQuery::JoinQuery(DatasetID Rid, DatasetID Sid, int id, QueryType predicate, QueryResultType resultType) {
        if (predicate != Q_NONE && predicate != Q_RANGE) {
            this->Rid = Rid;
            this->Sid = Sid;
            this->queryID = id;
            this->queryType = predicate;
            this->resultType = resultType;
            return;
        }
        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid predicate for a join query:", predicate);
    }

    RangeQuery::RangeQuery(DatasetID datasetID, int id, std::string queryWKT) {
        if (queryWKT == "") {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid (empty) wkt for range query.");
            return;
        }
        this->datasetID = datasetID;
        this->queryID = id;
        this->queryType = Q_RANGE;
        this->resultType = QR_COUNT;
        this->wktText = queryWKT;
    }

    RangeQuery::RangeQuery(DatasetID datasetID, int id, std::string queryWKT, std::string resultTypeStr) {
        if (queryWKT == "") {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid (empty) wkt for range query.");
            return;
        }
        this->datasetID = datasetID;
        this->queryID = id;
        this->queryType = Q_RANGE;
        this->wktText = queryWKT;
        // Shape window;
        // DB_STATUS ret = window.setFromWKT(queryWKT);
        // if (ret != DBERR_OK) {
        //     logger::log_error(ret, "Invalid wkt geometry for window:", queryWKT);
        //     return;
        // }
        // window.setMBR();
        // this->coords = coords;
        // this->xMin = std::numeric_limits<int>::max();
        // this->yMin = std::numeric_limits<int>::max();
        // this->xMax = -std::numeric_limits<int>::max();
        // this->yMax = -std::numeric_limits<int>::max();

        // for (int i=0; i<coords.size(); i+=2) {
        //     this->xMin = std::min(this->xMin, coords[i]);
        //     this->yMin = std::min(this->yMin, coords[i+1]);
        //     this->xMax = std::max(this->xMax, coords[i]);
        //     this->yMax = std::max(this->yMax, coords[i+1]);
        // }
        QueryResultType resultType = mapping::queryResultTypeStrToInt(resultTypeStr);
        if (resultType != QR_COUNT && resultType != QR_COLLECT) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid result type parameter:", resultType);
            return;
        }
        this->resultType = resultType;
    }

    RangeQuery::RangeQuery(DatasetID datasetID, int id, std::string queryWKT, QueryResultType resultType) {
        if (queryWKT == "") {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid (empty) wkt for range query.");
            return;
        }
        this->datasetID = datasetID;
        this->queryID = id;
        this->queryType = Q_RANGE;
        this->wktText = queryWKT;
        // Shape window;
        // DB_STATUS ret = window.setFromWKT(queryWKT);
        // if (ret != DBERR_OK) {
        //     logger::log_error(ret, "Invalid wkt geometry for window:", queryWKT);
        //     return;
        // }
        // window.setMBR();
        // this->coords = coords;
        // this->xMin = std::numeric_limits<int>::max();
        // this->yMin = std::numeric_limits<int>::max();
        // this->xMax = -std::numeric_limits<int>::max();
        // this->yMax = -std::numeric_limits<int>::max();

        // for (int i=0; i<coords.size(); i+=2) {
        //     this->xMin = std::min(this->xMin, coords[i]);
        //     this->yMin = std::min(this->yMin, coords[i+1]);
        //     this->xMax = std::max(this->xMax, coords[i]);
        //     this->yMax = std::max(this->yMax, coords[i+1]);
        // }
        if (resultType != QR_COUNT && resultType != QR_COLLECT) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid result type parameter:", resultType);
            return;
        }
        this->resultType = resultType;
    }

    std::string RangeQuery::getWKT() {
        return this->wktText;
    }

    // Q RESULT BASE

    int QResultBase::getQueryID() {
        return this->queryID;
    }

    QueryType QResultBase::getQueryType() {
        return this->queryType;
    }

    QueryResultType QResultBase::getResultType() {
        return this->queryResultType;
    }

    QueryType QResultBase::getQueryTypeFromSerializedBuffer(const char *buffer, int bufferSize) {
        int position = sizeof(int); // move position accordingly
        QueryType res;
        // query type
        memcpy(&res, buffer + position, sizeof(QueryType));
        return res;
    }

    QueryResultType QResultBase::getResultTypeFromSerializedBuffer(const char *buffer, int bufferSize) {
        int position = sizeof(int) + sizeof(QueryType); // move position accordingly
        QueryResultType res;
        // result type
        memcpy(&res, buffer + position, sizeof(QueryResultType));
        return res;
    }

    // RESULT COUNT

    QResultCount::QResultCount(int queryID, QueryType queryType, QueryResultType queryResultType) {
        this->queryID = queryID;
        this->queryType = queryType;
        this->queryResultType = queryResultType;
        this->resultCount = 0;
    }

    void QResultCount::addResult(size_t id) {
        this->resultCount++;
    }

    void QResultCount::addResult(size_t idR, size_t idS) {
        this->resultCount++;
        // printf("%ld,%ld\n", idR, idS);
    }

    void QResultCount::addResult(int relation, size_t idR, size_t idS) {
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QResultCount::addResult(int relation, size_t idR, size_t idS).");
    };


    int QResultCount::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        size += sizeof(size_t);                                             // result count
        return size;
    }

    void QResultCount::serialize(char **buffer, int &bufferSize) {
        // calculate size
        int bufferSizeRet = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QResultCount object.");
            return;
        }
        char* localBuffer = *buffer;
        // add query id
        *reinterpret_cast<int*>(localBuffer) = this->queryID;
        localBuffer += sizeof(int);
        // add query type
        *reinterpret_cast<hec::QueryType*>(localBuffer) = this->queryType;
        localBuffer += sizeof(hec::QueryType);
        // add query result type
        *reinterpret_cast<QueryResultType*>(localBuffer) = this->queryResultType;
        localBuffer += sizeof(QueryResultType);
        // result count
        *reinterpret_cast<size_t*>(localBuffer) = this->resultCount;
        localBuffer += sizeof(size_t);
        // set final size
        bufferSize = bufferSizeRet;
    }
    
    void QResultCount::deserialize(const char *buffer, int bufferSize) {
        int position = 0;
        // query id
        memcpy(&this->queryID, buffer + position, sizeof(int));
        position += sizeof(int);
        // query type
        memcpy(&this->queryType, buffer + position, sizeof(QueryType));
        position += sizeof(QueryType);
        // query result type
        memcpy(&this->queryResultType, buffer + position, sizeof(QueryResultType));
        position += sizeof(QueryResultType);
        // result count
        memcpy(&this->resultCount, buffer + position, sizeof(size_t));
        position += sizeof(size_t);

        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Deserialization of QResultCount object finished, but buffer contains unchecked elements.");
        }
    }

    void QResultCount::mergeResults(QResultBase* other) {
        // cast object pointer from base to derived
        auto otherCasted = dynamic_cast<QResultCount*>(other);
        this->resultCount += otherCasted->resultCount;
    }

    size_t QResultCount::getResultCount() {
        return this->resultCount;
    }

    std::vector<size_t> QResultCount::getResultList() {
        std::vector<size_t> empty;
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QResultCount::getResultList().");
        return empty;
    }

    std::vector<std::vector<size_t>> QResultCount::getTopologyResultList() {
        std::vector<std::vector<size_t>> empty;
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QResultCount::getTopologyResultList().");
        return empty;
    }

    void QResultCount::setResult(size_t resultCount) {
        this->resultCount = resultCount;
    }

    void QResultCount::deepCopy(QResultBase* other) {
        this->queryID = other->getQueryID();
        this->queryType = other->getQueryType();
        this->queryResultType = other->getResultType();
        this->resultCount = other->getResultCount();
    }

    QResultBase* QResultCount::cloneEmpty() {
        QResultBase* newPtr = new hec::QResultCount(queryID, queryType, queryResultType);
        return newPtr;
    }


    // SINGLE ID RESULT COLLECT

    QResultCollect::QResultCollect(int queryID, QueryType queryType, QueryResultType queryResultType) {
        this->queryID = queryID;
        this->queryType = queryType;
        this->queryResultType = queryResultType;
        this->resultList.clear();
    }

    void QResultCollect::addResult(size_t id) {
        this->resultList.emplace_back(id);
    }
    void QResultCollect::addResult(size_t idR, size_t idS) {
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QResultCollect::addResult(size_t idR, size_t idS).");
    }
    
    void QResultCollect::addResult(int relation, size_t idR, size_t idS) {
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QResultCollect::addResult(int relation, size_t idR, size_t idS).");
    }
       
    int QResultCollect::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        size += sizeof(this->resultList.size());                             // result ids count
        size += this->resultList.size() * sizeof(size_t);                   // result ids
        return size;
    }

    void QResultCollect::serialize(char **buffer, int &bufferSize) {
        // calculate size
        int bufferSizeRet = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QResultCollect object.");
            return;
        }
        char* localBuffer = *buffer;
        // add query id
        *reinterpret_cast<int*>(localBuffer) = this->queryID;
        localBuffer += sizeof(int);
        // add query type
        *reinterpret_cast<hec::QueryType*>(localBuffer) = this->queryType;
        localBuffer += sizeof(hec::QueryType);
        // add query result type
        *reinterpret_cast<QueryResultType*>(localBuffer) = this->queryResultType;
        localBuffer += sizeof(QueryResultType);
        // ids list size
        size_t idsListSize = this->resultList.size();
        *reinterpret_cast<size_t*>(localBuffer) = idsListSize;
        localBuffer += sizeof(size_t);
        // ids
        for (auto &id : this->resultList) {
            *reinterpret_cast<size_t*>(localBuffer) = id;
            localBuffer += sizeof(size_t);
        }
        // set final size
        bufferSize = bufferSizeRet;
    }
    
    void QResultCollect::deserialize(const char *buffer, int bufferSize) {
        int position = 0;
        // query id
        memcpy(&this->queryID, buffer + position, sizeof(int));
        position += sizeof(int);
        // query type
        memcpy(&this->queryType, buffer + position, sizeof(QueryType));
        position += sizeof(QueryType);
        // query result type
        memcpy(&this->queryResultType, buffer + position, sizeof(QueryResultType));
        position += sizeof(QueryResultType);
        // result ids list size
        size_t idsListSize;
        memcpy(&idsListSize, buffer + position, sizeof(size_t));
        position += sizeof(size_t);
        this->resultList.resize(idsListSize);
        // result ids
        for (int i=0; i<idsListSize; i++) {
            size_t id;
            memcpy(&id, buffer + position, sizeof(size_t));
            position += sizeof(size_t);
            this->resultList[i] = id;
        }
        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Deserialization of QResultCollect object finished, but buffer contains unchecked elements.");
            return;
        }
    }
    
    void QResultCollect::mergeResults(QResultBase* other) {
        // cast object pointer from base to derived
        auto otherCasted = dynamic_cast<QResultCollect*>(other);
        this->resultList.reserve(this->resultList.size() + otherCasted->resultList.size());
        this->resultList.insert(this->resultList.end(), otherCasted->resultList.begin(), otherCasted->resultList.end());
    }

    size_t QResultCollect::getResultCount() {
        return this->resultList.size();
    }

    std::vector<size_t> QResultCollect::getResultList() {
        return this->resultList;
    }

    std::vector<std::vector<size_t>> QResultCollect::getTopologyResultList() {
        std::vector<std::vector<size_t>> empty;
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QResultCollect::getTopologyResultList().");
        return empty;
    }

    void QResultCollect::setResult(std::vector<size_t> &resultList) {
        this->resultList = resultList;
    }

    void QResultCollect::deepCopy(QResultBase* other) {
        auto castedPtr = dynamic_cast<QResultCollect*>(other);
        if (castedPtr) {
            this->queryID = castedPtr->getQueryID();
            this->queryType = castedPtr->getQueryType();
            this->queryResultType = castedPtr->getResultType();
            this->resultList = castedPtr->getResultList();
        }
    }

    QResultBase* QResultCollect::cloneEmpty() {
        QResultBase* newPtr = new hec::QPairResultCollect(queryID, queryType, queryResultType);
        return newPtr;
    }

    // PAIR IDS RESULT COLLECT

    QPairResultCollect::QPairResultCollect(int queryID, QueryType queryType, QueryResultType queryResultType) {
        this->queryID = queryID;
        this->queryType = queryType;
        this->queryResultType = queryResultType;
        this->resultList.clear();
    }

    void QPairResultCollect::addResult(size_t idR, size_t idS) {
        this->resultList.emplace_back(idR);
        this->resultList.emplace_back(idS);
    }

    int QPairResultCollect::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        size += sizeof(this->resultList.size());                             // result ids count
        size += this->resultList.size() * sizeof(size_t);                   // result ids
        return size;
    }

    void QPairResultCollect::serialize(char **buffer, int &bufferSize) {
        // calculate size
        int bufferSizeRet = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QPairResultCollect object.");
            return;
        }
        char* localBuffer = *buffer;
        // add query id
        *reinterpret_cast<int*>(localBuffer) = this->queryID;
        localBuffer += sizeof(int);
        // add query type
        *reinterpret_cast<hec::QueryType*>(localBuffer) = this->queryType;
        localBuffer += sizeof(hec::QueryType);
        // add query result type
        *reinterpret_cast<QueryResultType*>(localBuffer) = this->queryResultType;
        localBuffer += sizeof(QueryResultType);
        // ids list size
        size_t idsListSize = this->resultList.size();
        *reinterpret_cast<size_t*>(localBuffer) = idsListSize;
        localBuffer += sizeof(size_t);
        // ids
        for (auto &it : this->resultList) {
            *reinterpret_cast<size_t*>(localBuffer) = it;
            localBuffer += sizeof(size_t);
        }
        // set final size
        bufferSize = bufferSizeRet;
    }
    
    void QPairResultCollect::deserialize(const char *buffer, int bufferSize) {
        int position = 0;
        // query id
        memcpy(&this->queryID, buffer + position, sizeof(int));
        position += sizeof(int);
        // query type
        memcpy(&this->queryType, buffer + position, sizeof(QueryType));
        position += sizeof(QueryType);
        // query result type
        memcpy(&this->queryResultType, buffer + position, sizeof(QueryResultType));
        position += sizeof(QueryResultType);
        // result ids list size
        size_t idsListSize;
        memcpy(&idsListSize, buffer + position, sizeof(size_t));
        position += sizeof(size_t);
        this->resultList.resize(idsListSize);
        // result ids
        for (int i=0; i<idsListSize; i++) {
            size_t id;
            memcpy(&id, buffer + position, sizeof(size_t));
            position += sizeof(size_t);
            this->resultList[i] = id;
        }
        // check correctness
        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Deserialization of QPairResultCollect object finished, but buffer contains unchecked elements.");
            return;
        }
    }
    
    void QPairResultCollect::mergeResults(QResultBase* other) {
        // cast object pointer from base to derived
        auto otherCasted = dynamic_cast<QPairResultCollect*>(other);
        this->resultList.reserve(this->resultList.size() + otherCasted->resultList.size());
        this->resultList.insert(this->resultList.end(), otherCasted->resultList.begin(), otherCasted->resultList.end());
    }

    size_t QPairResultCollect::getResultCount() {
        // return size / 2 because every two ids in the list represent a pair
        return this->resultList.size() / 2;
    }

    std::vector<size_t> QPairResultCollect::getResultList() {
        return this->resultList;
    }

    std::vector<std::vector<size_t>> QPairResultCollect::getTopologyResultList() {
        std::vector<std::vector<size_t>> empty;
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QPairResultCollect::getTopologyResultList().");
        return empty;
    }

    void QPairResultCollect::setResult(std::vector<size_t> &resultList) {
        this->resultList = resultList;
    }

    void QPairResultCollect::deepCopy(QResultBase* other) {
        auto castedPtr = dynamic_cast<QPairResultCollect*>(other);
        if (castedPtr) {
            this->queryID = castedPtr->getQueryID();
            this->queryType = castedPtr->getQueryType();
            this->queryResultType = castedPtr->getResultType();
            this->resultList = castedPtr->getResultList();
        }
    }

    QResultBase* QPairResultCollect::cloneEmpty() {
        QResultBase* newPtr = new hec::QPairResultCollect(queryID, queryType, queryResultType);
        return newPtr;
    }

    // TOPOLOGY RESULT COUNT

    QTopologyResultCount::QTopologyResultCount(int queryID, QueryType queryType, QueryResultType queryResultType) {
        this->queryID = queryID;
        this->queryType = queryType;
        this->queryResultType = queryResultType;
        this->resultList.resize(this->TOTAL_RELATIONS);
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            this->resultList[i] = 0;
        }
    }

    void QTopologyResultCount::addResult(int relation, size_t idR, size_t idS) {
        if (relation < 0 || relation > 7) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Count Result: Invalid relation with code", relation,". Must be 0-7.");
            return;
        }
        this->resultList[relation]++;
    }

    int QTopologyResultCount::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        size += this->TOTAL_RELATIONS * sizeof(size_t);                           // per relation result
        return size;
    }

    void QTopologyResultCount::serialize(char **buffer, int &bufferSize) {
        // calculate size
        int bufferSizeRet = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QTopologyResultCount object.");
            return;
        }
        char* localBuffer = *buffer;
        // add query id
        *reinterpret_cast<int*>(localBuffer) = this->queryID;
        localBuffer += sizeof(int);
        // add query type
        *reinterpret_cast<hec::QueryType*>(localBuffer) = this->queryType;
        localBuffer += sizeof(hec::QueryType);
        // add query result type
        *reinterpret_cast<QueryResultType*>(localBuffer) = this->queryResultType;
        localBuffer += sizeof(QueryResultType);
        // per relation results
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            size_t relationResult = this->resultList[i];
            *reinterpret_cast<size_t*>(localBuffer) = relationResult;
            localBuffer += sizeof(size_t);
        }
        // set final size
        bufferSize = bufferSizeRet;
    }
    
    void QTopologyResultCount::deserialize(const char *buffer, int bufferSize) {
        int position = 0;
        // query id
        memcpy(&this->queryID, buffer + position, sizeof(int));
        position += sizeof(int);
        // query type
        memcpy(&this->queryType, buffer + position, sizeof(QueryType));
        position += sizeof(QueryType);
        // query result type
        memcpy(&this->queryResultType, buffer + position, sizeof(QueryResultType));
        position += sizeof(QueryResultType);
        // per relation results
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            size_t relationResult;
            memcpy(&relationResult, buffer + position, sizeof(size_t));
            position += sizeof(size_t);
            this->resultList[i] = relationResult;
        }
        // check correctness
        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Deserialization of QTopologyResultCount object finished, but buffer contains unchecked elements.");
            return;
        }
    }

    void QTopologyResultCount::mergeResults(QResultBase* other) {
        // cast object pointer from base to derived
        auto otherCasted = dynamic_cast<QTopologyResultCount*>(other);
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            this->resultList[i] += otherCasted->resultList[i];
        }
    }

    size_t QTopologyResultCount::getResultCount() {
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QTopologyResultCount::getResultCount().");
        return 0;
    }

    std::vector<size_t> QTopologyResultCount::getResultList() {
        return this->resultList;
    }
    
    std::vector<std::vector<size_t>> QTopologyResultCount::getTopologyResultList() {
        std::vector<std::vector<size_t>> empty;
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QTopologyResultCount::getTopologyResultList().");
        return empty;
    }

    void QTopologyResultCount::setResult(int relation, size_t resultCount) {
        if (relation < 0 || relation > 7) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Get Result: Invalid relation with code", relation,". Must be 0-7.");
            return;
        }
        this->resultList[relation] = resultCount;
    }

    void QTopologyResultCount::deepCopy(QResultBase* other) {
        auto castedPtr = dynamic_cast<QTopologyResultCount*>(other);
        if (castedPtr) {
            this->queryID = castedPtr->getQueryID();
            this->queryType = castedPtr->getQueryType();
            this->queryResultType = castedPtr->getResultType();
            this->resultList = castedPtr->getResultList();
        }
    }

    QResultBase* QTopologyResultCount::cloneEmpty() {
        QResultBase* newPtr = new hec::QTopologyResultCount(queryID, queryType, queryResultType);
        return newPtr;
    }

    // TOPOLOGY RESULT COLLECT

    QTopologyResultCollect::QTopologyResultCollect(int queryID, QueryType queryType, QueryResultType queryResultType) {
        this->queryID = queryID;
        this->queryType = queryType;
        this->queryResultType = queryResultType;
        this->resultList.resize(this->TOTAL_RELATIONS);
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            this->resultList[i].clear();
        }
    }

    void QTopologyResultCollect::addResult(int relation, size_t idR, size_t idS) {
        if (relation < 0 || relation > 7) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Count Result: Invalid relation with code", relation,". Must be 0-7.");
            return;
        }
        this->resultList[relation].emplace_back(idR);
        this->resultList[relation].emplace_back(idS);
    }

    int QTopologyResultCollect::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        for (auto &relation: this->resultList) {
            size += sizeof(relation.size());                            // total pairs for this relation
            size += relation.size() * sizeof(size_t);               // the pair ids for this relation
        }
        return size;
    }

    void QTopologyResultCollect::serialize(char **buffer, int &bufferSize) {
        // calculate size
        int bufferSizeRet = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QTopologyResultCollect object.");
            return;
        }
        char* localBuffer = *buffer;
        // add query id
        *reinterpret_cast<int*>(localBuffer) = this->queryID;
        localBuffer += sizeof(int);
        // add query type
        *reinterpret_cast<hec::QueryType*>(localBuffer) = this->queryType;
        localBuffer += sizeof(hec::QueryType);
        // add query result type
        *reinterpret_cast<QueryResultType*>(localBuffer) = this->queryResultType;
        localBuffer += sizeof(QueryResultType);
        // per relation ids
        for (auto &it : this->resultList) {
            // ids list size
            size_t idsListSize = it.size();
            *reinterpret_cast<size_t*>(localBuffer) = idsListSize;
            localBuffer += sizeof(size_t);
            // ids
            for (auto &id : it) {
                *reinterpret_cast<size_t*>(localBuffer) = id;
                localBuffer += sizeof(size_t);
            }
        }
        // set final size
        bufferSize = bufferSizeRet;
    }
    
    void QTopologyResultCollect::deserialize(const char *buffer, int bufferSize) {
        int position = 0;
        // query id
        memcpy(&this->queryID, buffer + position, sizeof(int));
        position += sizeof(int);
        // query type
        memcpy(&this->queryType, buffer + position, sizeof(QueryType));
        position += sizeof(QueryType);
        // query result type
        memcpy(&this->queryResultType, buffer + position, sizeof(QueryResultType));
        position += sizeof(QueryResultType);
        // per relation ids
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            // ids list size
            size_t idsListSize;
            memcpy(&idsListSize, buffer + position, sizeof(size_t));
            position += sizeof(size_t);
            this->resultList[i].resize(idsListSize);
            // ids
            size_t id;
            for (int j=0; j<idsListSize; j++) {
                memcpy(&id, buffer + position, sizeof(size_t));
                position += sizeof(size_t);
                this->resultList[i][j] = id;
            }
        }
        // check correctness
        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Deserialization of QTopologyResultCollect object finished, but buffer contains unchecked elements.");
            return;
        }
    }
    
    void QTopologyResultCollect::mergeResults(QResultBase* other) {
        // cast object pointer from base to derived
        auto otherCasted = dynamic_cast<QTopologyResultCollect*>(other);
        for (int i=0; i<this->TOTAL_RELATIONS; i++) {
            this->resultList[i].reserve(this->resultList[i].size() + otherCasted->resultList[i].size());
            this->resultList[i].insert(this->resultList[i].end(), otherCasted->resultList[i].begin(), otherCasted->resultList[i].end());
        }
    }

    size_t QTopologyResultCollect::getResultCount() {
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QTopologyResultCollect::getResultCount().");
        return 0;
    }

    std::vector<size_t> QTopologyResultCollect::getResultList() {
        std::vector<size_t> empty;
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QTopologyResultCollect::getResultCount().");
        return empty;
    }

    std::vector<size_t> QTopologyResultCollect::getResultList(int relation) {
        if (relation < 0 || relation > 7) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Count Result: Invalid relation with code", relation,". Must be 0-7.");
            std::vector<size_t> empty;
            return empty;
        }
        return this->resultList[relation];
    }

    std::vector<std::vector<size_t>> QTopologyResultCollect::getTopologyResultList() {
        return this->resultList;
    }


    void QTopologyResultCollect::setResult(int relation, std::vector<size_t> &ids) {
        if (relation < 0 || relation > 7) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Get Result: Invalid relation with code", relation,". Must be 0-7.");
            return;
        }
        this->resultList[relation] = ids;
    }

    void QTopologyResultCollect::deepCopy(QResultBase* other) {
        auto castedPtr = dynamic_cast<QTopologyResultCollect*>(other);
        if (castedPtr) {
            this->queryID = castedPtr->getQueryID();
            this->queryType = castedPtr->getQueryType();
            this->queryResultType = castedPtr->getResultType();
            for (int i=0; i<this->TOTAL_RELATIONS; i++) {
                this->resultList[i] = castedPtr->getResultList(i);
            }
        }
    }

    QResultBase* QTopologyResultCollect::cloneEmpty() {
        QResultBase* newPtr = new hec::QTopologyResultCollect(queryID, queryType, queryResultType);
        return newPtr;
    }
}