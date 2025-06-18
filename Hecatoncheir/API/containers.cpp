#include "containers.h"

#include "def.h"
#include "utils.h"
#include "../include/containers.h"

namespace hec
{
    // globals
    SpatialQueries spatialQueries;
    SupportedQueryResultTypes queryResultTypes;

    // BASE QUERY

    Query* Query::createFromBuffer(char*& buffer) {
        char* start = buffer;
        // Read query type to determine subclass
        int queryID = *reinterpret_cast<const int*>(buffer);
        buffer += sizeof(int);

        QueryType type = *reinterpret_cast<const QueryType*>(buffer);
        buffer += sizeof(QueryType);

        // Restore buffer for the right subclass to read fully
        buffer = start;

        Query* query = nullptr;

        switch (type) {
            case Q_RANGE:
                query = new RangeQuery(); // empty constructor
                break;
            case Q_KNN:
                query = new KNNQuery();
                break;
            case Q_INTERSECTION_JOIN:
            case Q_INSIDE_JOIN:
            case Q_DISJOINT_JOIN:
            case Q_CONTAINS_JOIN:
            case Q_COVERS_JOIN:
            case Q_COVERED_BY_JOIN:
            case Q_MEET_JOIN:
            case Q_EQUAL_JOIN:
            case Q_FIND_RELATION_JOIN:
                query = new JoinQuery();
                break;
            default:
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type in message.");
                return nullptr;
        }

        // Deserialize into the new object
        if (query->deserialize(buffer) < 0) {
            delete query;
            return nullptr;
        }

        return query;
    }

    // JOIN QUERY

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

    int JoinQuery::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                       // query id
        size += sizeof(hec::QueryType);                            // query type
        size += sizeof(hec::QueryResultType);                      // result type
        size += 2 * sizeof(hec::DatasetID); 
        return size;
    }

    int JoinQuery::serialize(char **buffer, int &bufferSize) {
         // calculate size
        bufferSize = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSize * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QResultCount object.");
            return -1;
        }
        char* localBuffer = *buffer;
        *reinterpret_cast<int*>(localBuffer) = (int) this->queryID;
        localBuffer += sizeof(int);
        *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) this->queryType;
        localBuffer += sizeof(hec::QueryType);
        *reinterpret_cast<hec::QueryResultType*>(localBuffer) = this->resultType;
        localBuffer += sizeof(hec::QueryResultType);
        *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) this->Rid;
        localBuffer += sizeof(hec::DatasetID);
        *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) this->Sid;
        localBuffer += sizeof(hec::DatasetID);
        return 0;
    }

    int JoinQuery::deserialize(char*& buffer) {
        int position = 0;
        // query id
        int id = (int) *reinterpret_cast<const int*>(buffer + position);
        position += sizeof(int);
        // query type
        hec::QueryType queryType = (hec::QueryType) *reinterpret_cast<const hec::QueryType*>(buffer + position);
        position += sizeof(hec::QueryType);
        // query result type
        hec::QueryResultType queryResultType = (hec::QueryResultType) *reinterpret_cast<const hec::QueryResultType*>(buffer + position);
        position += sizeof(hec::QueryResultType);
        // unpack the rest of the info
        hec::DatasetID datasetRid = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(buffer + position);
        position += sizeof(hec::DatasetID);
        hec::DatasetID datasetSid = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(buffer + position);
        position += sizeof(hec::DatasetID); 
        
        // set object 
        this->queryID = id;
        this->queryType = queryType;
        this->resultType = queryResultType;
        this->Rid = datasetRid;
        this->Sid = datasetSid;

        // increment original buffer
        buffer += position;
        return 0;
    }

    // RANGE QUERY

    RangeQuery::RangeQuery(DatasetID datasetID, int id, std::string queryWKT) {
        if (queryWKT == "") {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid (empty) wkt for range query.");
            return;
        }
        if (queryWKT.find("POLYGON") != std::string::npos) {
            this->shapeType = DT_POLYGON;
        } else if (queryWKT.find("BOX") != std::string::npos) {
            this->shapeType = DT_BOX;
        } else {
            logger::log_error(DBERR_INVALID_DATATYPE, "Invalid WKT for range query. Use only POLYGON or BOX. WKT:", queryWKT);
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
        if (queryWKT.find("POLYGON") != std::string::npos) {
            this->shapeType = DT_POLYGON;
        } else if (queryWKT.find("BOX") != std::string::npos) {
            this->shapeType = DT_BOX;
        } else {
            logger::log_error(DBERR_INVALID_DATATYPE, "Invalid WKT for range query. Use only POLYGON or BOX. WKT:", queryWKT);
            return;
        }
        this->datasetID = datasetID;
        this->queryID = id;
        this->queryType = Q_RANGE;
        this->wktText = queryWKT;
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
        if (queryWKT.find("POLYGON") != std::string::npos) {
            this->shapeType = DT_POLYGON;
        } else if (queryWKT.find("BOX") != std::string::npos) {
            this->shapeType = DT_BOX;
        } else {
            logger::log_error(DBERR_INVALID_DATATYPE, "Invalid WKT for range query. Use only POLYGON or BOX. WKT:", queryWKT);
            return;
        }
        this->datasetID = datasetID;
        this->queryID = id;
        this->queryType = Q_RANGE;
        this->wktText = queryWKT;
        if (resultType != QR_COUNT && resultType != QR_COLLECT) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid result type parameter:", resultType);
            return;
        }
        this->resultType = resultType;
    }

    std::string RangeQuery::getWKT() {
        return this->wktText;
    }

    int RangeQuery::getShapeType() {
        return this->shapeType;
    }

    int RangeQuery::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                        // query id
        size += sizeof(hec::QueryType);                             // query type
        size += sizeof(hec::QueryResultType);                       // result type
        size += sizeof(hec::DatasetID);                             // dataset id
        size += sizeof(int);                                        // wkt text length
        size += this->wktText.length() * sizeof(char);              // wkt string
        return size;
    }

    int RangeQuery::serialize(char **buffer, int &bufferSize) {
        // calculate size
        bufferSize = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSize * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QResultCount object.");
            return -1;
        }
        char* localBuffer = *buffer;
        *reinterpret_cast<int*>(localBuffer) = (int) this->queryID;
        localBuffer += sizeof(int);
        *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) this->queryType;
        localBuffer += sizeof(hec::QueryType);
        *reinterpret_cast<hec::QueryResultType*>(localBuffer) = this->resultType;
        localBuffer += sizeof(hec::QueryResultType);
        *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) this->datasetID;
        localBuffer += sizeof(hec::DatasetID); 
        *reinterpret_cast<int*>(localBuffer) = this->wktText.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, this->wktText.data(), this->wktText.length() * sizeof(char));
        localBuffer += this->wktText.length() * sizeof(char);
        return 0;
    }

    int RangeQuery::deserialize(char*& buffer) {
        int position = 0;
        // query id
        int id = (int) *reinterpret_cast<const int*>(buffer + position);
        position += sizeof(int);
        // query type
        hec::QueryType queryType = (hec::QueryType) *reinterpret_cast<const hec::QueryType*>(buffer + position);
        position += sizeof(hec::QueryType);
        // query result type
        hec::QueryResultType queryResultType = (hec::QueryResultType) *reinterpret_cast<const hec::QueryResultType*>(buffer + position);
        position += sizeof(hec::QueryResultType);
        // unpack the rest of the info
        hec::DatasetID datasetID = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(buffer + position);
        position += sizeof(hec::DatasetID);
        // wkt text length + string
        int length;
        length = *reinterpret_cast<const int*>(buffer + position);
        position += sizeof(int);
        std::string wktText(buffer + position, buffer + position + length);
        position += length * sizeof(char);

        // set object instance
        this->queryID = id;
        this->queryType = queryType;
        this->resultType = queryResultType;
        this->datasetID = datasetID;
        this->wktText = wktText;

        // increment original buffer
        buffer += position;

        return 0;
    }

    // kNN
    KNNQuery::KNNQuery(DatasetID datasetID, int id, std::string queryWKT, int k) {
        if (queryWKT == "") {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid (empty) wkt for range query.");
            return;
        }
        if (queryWKT.find("POINT") != std::string::npos) {
            this->shapeType = DT_POINT;
        } else {
            logger::log_error(DBERR_INVALID_DATATYPE, "Invalid WKT for kNN query. Only POINT is supported currently. WKT:", queryWKT);
            return;
        }
        if (k <= 0) {
            logger::log_error(DBERR_INVALID_PARAMETER, "k value for kNN query must be larger than 0. k =", k);
            return;
        }
        this->datasetID = datasetID;
        this->queryID = id;
        this->queryType = Q_KNN;
        this->resultType = QR_KNN;
        this->wktText = queryWKT;
        this->k = k;
    }

    std::string KNNQuery::getWKT() {
        return this->wktText;
    }

    int KNNQuery::getShapeType() {
        return this->shapeType;
    }

    int KNNQuery::getK() {
        return this->k;
    }

    int KNNQuery::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                        // query id
        size += sizeof(hec::QueryType);                             // query type
        size += sizeof(hec::QueryResultType);                       // result type
        size += sizeof(hec::DatasetID);                             // dataset id
        size += sizeof(int);                                        // k
        size += sizeof(int);                                        // wkt text length
        size += this->wktText.length() * sizeof(char);              // wkt string
        return size;
    }

    int KNNQuery::serialize(char **buffer, int &bufferSize) {
        // calculate size
        bufferSize = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSize * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc failed on serialization of QResultCount object.");
            return -1;
        }
        char* localBuffer = *buffer;
        *reinterpret_cast<int*>(localBuffer) = (int) this->queryID;
        localBuffer += sizeof(int);
        *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) this->queryType;
        localBuffer += sizeof(hec::QueryType);
        *reinterpret_cast<hec::QueryResultType*>(localBuffer) = (hec::QueryResultType) this->resultType;
        localBuffer += sizeof(hec::QueryResultType);
        *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) this->datasetID;
        localBuffer += sizeof(hec::DatasetID); 
        *reinterpret_cast<int*>(localBuffer) = this->k;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = this->wktText.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, this->wktText.data(), this->wktText.length() * sizeof(char));
        localBuffer += this->wktText.length() * sizeof(char);
        return 0;
    }

    int KNNQuery::deserialize(char*& buffer) {
        int position = 0;
        // query id
        int id = (int) *reinterpret_cast<const int*>(buffer + position);
        position += sizeof(int);
        // query type
        hec::QueryType queryType = (hec::QueryType) *reinterpret_cast<const hec::QueryType*>(buffer + position);
        position += sizeof(hec::QueryType);
        // query result type
        hec::QueryResultType queryResultType = (hec::QueryResultType) *reinterpret_cast<const hec::QueryResultType*>(buffer + position);
        position += sizeof(hec::QueryResultType);
        // unpack the rest of the info
        hec::DatasetID datasetID = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(buffer + position);
        position += sizeof(hec::DatasetID);
        int k = *reinterpret_cast<const int*>(buffer + position);
        position += sizeof(int);
        // wkt text length + string
        int length;
        length = *reinterpret_cast<const int*>(buffer + position);
        position += sizeof(int);
        std::string wktText(buffer + position, buffer + position + length);
        position += length * sizeof(char);

        // set object instance
        this->queryID = id;
        this->queryType = queryType;
        this->resultType = queryResultType;
        this->datasetID = datasetID;
        this->k = k;
        this->wktText = wktText;

        // increment original buffer
        buffer += position;

        return 0;
                        
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
        if (otherCasted) {
            this->resultCount += otherCasted->resultCount;
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QResultCount::mergeResults.");
        }
    }

    size_t QResultCount::getResultCount() {
        return this->resultCount;
    }

    void QResultCount::setResult(size_t resultCount) {
        this->resultCount = resultCount;
    }

    void QResultCount::deepCopy(QResultBase* other) {
        auto castedPtr = dynamic_cast<QResultCount*>(other);
        if (castedPtr) {
            this->queryID = castedPtr->getQueryID();
            this->queryType = castedPtr->getQueryType();
            this->queryResultType = castedPtr->getResultType();
            this->resultCount = castedPtr->getResultCount();
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QResultCount::deepCopy.");
        }
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
        // it is assumed that query file will always be S, so add idR
        // @todo: verify
        this->resultList.emplace_back(idR);
    }
           
    int QResultCollect::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        size += sizeof(size_t);                             // result ids count
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
        if (otherCasted) {
            this->resultList.reserve(this->resultList.size() + otherCasted->resultList.size());
            this->resultList.insert(this->resultList.end(), otherCasted->resultList.begin(), otherCasted->resultList.end());
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QResultCollect::mergeResults.");
        }
    }

    size_t QResultCollect::getResultCount() {
        return this->resultList.size();
    }

    std::vector<size_t> QResultCollect::getResultList() {
        return this->resultList;
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
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QResultCollect::deepCopy.");
        }
    }

    QResultBase* QResultCollect::cloneEmpty() {
        QResultBase* newPtr = new hec::QPairResultCollect(queryID, queryType, queryResultType);
        return newPtr;
    }

    // KNN RESULT COLLECT
    
    QResultkNN::QResultkNN(int queryID, int k) {
        this->queryID = queryID;
        this->queryType = Q_KNN;
        this->queryResultType = QR_KNN;
        this->k = k;
        while (!maxHeap.empty()) {
            maxHeap.pop();
        }        
    }

    void QResultkNN::addResult(size_t id, double distance) {
        if (maxHeap.size() < this->k) {
            maxHeap.emplace(distance, id);
            // logger::log_success("Added point", id, "with distance", distance);
        } else if (distance < maxHeap.top().first) {
            maxHeap.pop();
            maxHeap.emplace(distance, id);
            // logger::log_success("Added point", id, "with distance", distance);
        }
    }

    bool QResultkNN::checkDistance(double distance) {
        if (maxHeap.empty()) {
            return true;
        } else if (distance < maxHeap.top().first) {
            return true;
        }
        return false;
    }

    int QResultkNN::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        size += sizeof(int);                                                // k
        size += sizeof(size_t);                                             // heap size
        size += this->maxHeap.size() * (sizeof(size_t) + sizeof(double));   // heap contents (pair<double,size_t>)
        return size;
    }

    void QResultkNN::serialize(char **buffer, int &bufferSize) {
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
        // k
        *reinterpret_cast<int*>(localBuffer) = this->k;
        localBuffer += sizeof(int);
        // ids list size
        size_t heapSize = this->maxHeap.size();
        *reinterpret_cast<size_t*>(localBuffer) = heapSize;
        localBuffer += sizeof(size_t);
        // pairs (keep a copy to preserve the original)
        auto heapCopy = this->maxHeap;
        while (!heapCopy.empty()) {
            auto pair = heapCopy.top();
            heapCopy.pop();
            *reinterpret_cast<double*>(localBuffer) = pair.first;
            localBuffer += sizeof(double);
            *reinterpret_cast<size_t*>(localBuffer) = pair.second;
            localBuffer += sizeof(size_t);
        }
        // set final size
        bufferSize = bufferSizeRet;
    }

    void QResultkNN::deserialize(const char *buffer, int bufferSize) {
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
        // k
        memcpy(&this->k, buffer + position, sizeof(int));
        position += sizeof(int);
        // heap size
        size_t heapSize;
        memcpy(&heapSize, buffer + position, sizeof(size_t));
        position += sizeof(size_t);
        // result ids
        double distance;
        size_t id;
        for (int i=0; i<heapSize; i++) {
            memcpy(&distance, buffer + position, sizeof(double));
            position += sizeof(double);
            memcpy(&id, buffer + position, sizeof(size_t));
            position += sizeof(size_t);
            this->maxHeap.emplace(distance, id);
        }
        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Deserialization of QResultCollect object finished, but buffer contains unchecked elements.");
            return;
        }
    }

    void QResultkNN::mergeResults(QResultBase* other) {
        auto otherCasted = dynamic_cast<QResultkNN*>(other);
        if (otherCasted) {
            auto otherHeapCopy = otherCasted->maxHeap;
            while (!otherHeapCopy.empty()) {
                auto pair = otherHeapCopy.top();
                otherHeapCopy.pop();
                if (this->maxHeap.size() < this->k) {
                    this->maxHeap.push(pair);
                } else if (pair.first < this->maxHeap.top().first) {
                    this->maxHeap.push(pair);
                    // remove the worst (largest) distance
                    this->maxHeap.pop(); 
                }
            }
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QResultkNN::mergeResults.");
        }
    }    

    size_t QResultkNN::getResultCount() {
        return this->maxHeap.size();
    }

    std::vector<size_t> QResultkNN::getResultList() {
        std::vector<size_t> ids;
        ids.reserve(this->maxHeap.size());
        auto heapCopy = this->maxHeap;
        while (!heapCopy.empty()) {
            auto pair = heapCopy.top();
            heapCopy.pop();
            ids.emplace_back(pair.second);
        }
        std::reverse(ids.begin(), ids.end());
        return ids;
    }

    std::vector<std::pair<double, size_t>> QResultkNN::getHeap() {
        std::vector<std::pair<double, size_t>> heapList;
        heapList.reserve(this->maxHeap.size());
        auto heapCopy = this->maxHeap;
        while (!heapCopy.empty()) {
            auto pair = heapCopy.top();
            heapCopy.pop();
            heapList.emplace_back(pair);
        }
        return heapList;
    }


    void QResultkNN::setResult(std::priority_queue<std::pair<double, size_t>> &maxHeap) {
        this->maxHeap = maxHeap;
        this->k = maxHeap.size();
    }

    void QResultkNN::deepCopy(QResultBase* other) {
        auto castedPtr = dynamic_cast<QResultkNN*>(other);
        if (castedPtr) {
            this->queryID = castedPtr->getQueryID();
            this->queryType = castedPtr->getQueryType();
            this->queryResultType = castedPtr->getResultType();
            this->k = castedPtr->k;
            this->maxHeap = castedPtr->maxHeap;
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QResultkNN::deepCopy.");
        }
    } 

    QResultBase* QResultkNN::cloneEmpty() {
        QResultBase* newPtr = new hec::QResultkNN(queryID, k);
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
        size += sizeof(size_t);                                             // result ids count
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
        if (otherCasted) {
            this->resultList.reserve(this->resultList.size() + otherCasted->resultList.size());
            this->resultList.insert(this->resultList.end(), otherCasted->resultList.begin(), otherCasted->resultList.end());
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QPairResultCollect::mergeResults.");
        }
    }

    size_t QPairResultCollect::getResultCount() {
        // return size / 2 because every two ids in the list represent a pair
        return this->resultList.size() / 2;
    }

    std::vector<size_t> QPairResultCollect::getResultList() {
        return this->resultList;
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
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QPairResultCollect::deepCopy.");
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
        if (otherCasted) {
            for (int i=0; i<this->TOTAL_RELATIONS; i++) {
                this->resultList[i] += otherCasted->resultList[i];
            }
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QTopologyResultCount::mergeResults.");
        }
    }

    std::vector<size_t> QTopologyResultCount::getResultList() {
        return this->resultList;
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
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QTopologyResultCount::deepCopy.");
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
            size += sizeof(size_t);                            // total pairs for this relation
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
        if (otherCasted) {
            for (int i=0; i<this->TOTAL_RELATIONS; i++) {
                this->resultList[i].reserve(this->resultList[i].size() + otherCasted->resultList[i].size());
                this->resultList[i].insert(this->resultList[i].end(), otherCasted->resultList[i].begin(), otherCasted->resultList[i].end());
            }
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QTopologyResultCollect::mergeResults.");
        }
    }

    size_t QTopologyResultCollect::getResultCount() {
        logger::log_error(DBERR_FORBIDDEN_METHOD_CALL, "Forbidden method call for QTopologyResultCollect::getResultCount().");
        return 0;
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
        } else {
            logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Cast failed for QTopologyResultCollect::deepCopy.");
        }
    }

    QResultBase* QTopologyResultCollect::cloneEmpty() {
        QResultBase* newPtr = new hec::QTopologyResultCollect(queryID, queryType, queryResultType);
        return newPtr;
    }
}