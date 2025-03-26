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

    QueryResult::QueryResult() {
        reset();
    }

    QueryResult::QueryResult(int queryID, QueryType queryType, QueryResultType queryResultType) {
        this->reset();
        this->queryID = queryID;
        this->queryType = queryType;
        this->queryResultType = queryResultType;
        // Prepopulate relationMap and pairMap once
        for (int rel : {TR_DISJOINT, TR_EQUAL, TR_MEET, TR_CONTAINS, TR_COVERS, TR_COVERED_BY, TR_INSIDE, TR_INTERSECT}) {
            countRelationMap[rel] = 0;
            collectRelationMap[rel] = {};
        }
    }

    void QueryResult::reset() {
        queryID = -1;
        queryType = Q_NONE;
        queryResultType = QR_COUNT;
        resultCount = 0;

        // Reset only the values
        for (int i=0; i<8; i++) {
            countRelationMap[i] = 0;
            collectRelationMap[i].clear();
        }
        pairs.clear();
    }

    void QueryResult::countResult(size_t idR, size_t idS) {
        switch (this->getResultType()) {
            case QR_COUNT:
                this->resultCount++;
                break;
            case QR_COLLECT:
                if (pairs.capacity() == pairs.size()) {
                    // Reserve in chunks to avoid too many reallocations
                    pairs.reserve(pairs.size() + 64);  
                }
                this->pairs.push_back(std::make_pair(idR, idS));
                break;
            default:
                // invalid type
                logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type during countResult.");
        }
    }

    void QueryResult::countTopologyRelationResult(int relation, size_t idR, size_t idS) {
        switch (this->getResultType()) {
            case QR_COUNT:
                this->countRelationMap[relation]++;
                break;
            case QR_COLLECT:
                this->collectRelationMap[relation].push_back(std::make_pair(idR, idS));
                break;
            default:
                // invalid type
                logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type during countResult.");
        }
    }

    void QueryResult::mergeResults(hec::QueryResult &other) {
        if (this->getResultType() == QR_COUNT) {
            switch (this->getQueryType()) {
                case Q_RANGE:
                case Q_INTERSECTION_JOIN:
                case Q_INSIDE_JOIN:
                case Q_DISJOINT_JOIN:
                case Q_EQUAL_JOIN:
                case Q_MEET_JOIN:
                case Q_CONTAINS_JOIN:
                case Q_COVERS_JOIN:
                case Q_COVERED_BY_JOIN:
                    this->resultCount += other.resultCount;
                    break;
                case Q_FIND_RELATION_JOIN:
                    for (int i=0; i<8; i++) {
                        this->countRelationMap[i] += other.countRelationMap[i];
                    }
                    break;
            }
        } else if (this->getResultType() == QR_COLLECT) {
            switch (this->getQueryType()) {
                case Q_RANGE:
                case Q_INTERSECTION_JOIN:
                case Q_INSIDE_JOIN:
                case Q_DISJOINT_JOIN:
                case Q_EQUAL_JOIN:
                case Q_MEET_JOIN:
                case Q_CONTAINS_JOIN:
                case Q_COVERS_JOIN:
                case Q_COVERED_BY_JOIN:
                    this->pairs.reserve(this->pairs.size() + other.pairs.size());
                    this->pairs.insert(this->pairs.end(), other.pairs.begin(), other.pairs.end());
                    break;
                case Q_FIND_RELATION_JOIN:
                    for (int i=0; i<8; i++) {
                        this->collectRelationMap[i].reserve(this->collectRelationMap[i].size() + other.collectRelationMap[i].size());
                        this->collectRelationMap[i].insert(this->collectRelationMap[i].end(), other.collectRelationMap[i].begin(), other.collectRelationMap[i].end());
                    }
                    logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported for merge");
                    break;
            }
        } else {
            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type during merge.");
        }
    }

    void QueryResult::mergeTopologyRelationResults(hec::QueryResult &other) {
        switch (this->getResultType()) {
            case QR_COUNT:
                this->resultCount += other.resultCount;
                for (int i=0; i<8; i++) {
                    countRelationMap[i] += other.countRelationMap[i];
                }
                break;
            case QR_COLLECT:
                
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Topology relation collect unsupported.");
                break;
            default:
                // invalid type
                logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type during countResult.");
        }
    }

    void QueryResult::setResultCount(size_t newResultCount) {
        this->resultCount = newResultCount;
    }

    size_t QueryResult::getResultCount() {
        return this->resultCount;
    }

    size_t* QueryResult::getTopologyResultCount() {
        return this->countRelationMap;
    }

    void QueryResult::setTopologyResultCount(size_t* newRelationCountMap) {
        for (int i=0; i<8; i++) {
            this->countRelationMap[i] = newRelationCountMap[i];
        }
    }

    int QueryResult::calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                                                // query ID
        size += sizeof(QueryType);                                          // query type
        size += sizeof(QueryResultType);                                    // query result type
        if (this->queryResultType == QR_COUNT) {
            // count
            size += sizeof(size_t);                                         // result count
            size += 8 * (sizeof(int) + sizeof(size_t));                     // result count map (relations)
        } else if (this->queryResultType == QR_COLLECT) {
            // collect
            size += sizeof(this->pairs.size());                             // result pairs count
            size += this->pairs.size() * 2 * sizeof(size_t);                // result pairs
            // result pair per relation
            for (int i=0; i<8; i++) {
                size += sizeof(this->collectRelationMap[i].size());                           // result pairs size for this relation
                size += this->collectRelationMap[i].size() * (sizeof(int) + sizeof(size_t));    // result pairs for this relation
            }
        } else {
            // error, unknown
            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type.");
            return -1;
        }
        return size;
    }

    void QueryResult::serialize(char **buffer, int &bufferSize) {
        // calculate size
        int bufferSizeRet = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
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

        if (this->queryResultType == QR_COUNT) {
            // result count
            *reinterpret_cast<size_t*>(localBuffer) = this->resultCount;
            localBuffer += sizeof(size_t);

            // relation map relation counts
            for (int i=0; i<8; i++) {
                *reinterpret_cast<size_t*>(localBuffer) = this->countRelationMap[i];
                localBuffer += sizeof(size_t);
            }
        } else if (this->queryResultType == QR_COLLECT) {
            // collect
            // pairs vector size
            size_t pairsSize = this->pairs.size();
            *reinterpret_cast<size_t*>(localBuffer) = pairsSize;
            localBuffer += sizeof(size_t);
            // pairs vector
            for (auto &it : this->pairs) {
                *reinterpret_cast<size_t*>(localBuffer) = it.first;
                localBuffer += sizeof(size_t);
                *reinterpret_cast<size_t*>(localBuffer) = it.second;
                localBuffer += sizeof(size_t);
            }
            // pairs map
            for (int i=0; i<8; i++) {
                // relation pairs size
                size_t relationPairsSize = collectRelationMap[i].size();
                *reinterpret_cast<size_t*>(localBuffer) = relationPairsSize;
                localBuffer += sizeof(size_t);
                // relation pairs
                for (auto &pairIT: collectRelationMap[i]) {
                    *reinterpret_cast<size_t*>(localBuffer) = pairIT.first;
                    localBuffer += sizeof(size_t);
                    *reinterpret_cast<size_t*>(localBuffer) = pairIT.second;
                    localBuffer += sizeof(size_t);
                }
            }
        } else {
            // error, unknown
            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type.");
            return;
        }
        bufferSize = bufferSizeRet;
    }

    void QueryResult::deserialize(const char *buffer, int bufferSize) {
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

        if (this->queryResultType == QR_COUNT) {
            // result count
            memcpy(&this->resultCount, buffer + position, sizeof(size_t));
            position += sizeof(size_t);

            // relation map relation counts
            size_t results;
            for (int i=0; i<8; i++) {
                memcpy(&results, buffer + position, sizeof(size_t));
                position += sizeof(size_t);
                this->countRelationMap[i] = results;
            }
        } else if (this->queryResultType == QR_COLLECT) {
            // result pair vector size
            size_t pairsVectorSize;
            memcpy(&pairsVectorSize, buffer + position, sizeof(size_t));
            position += sizeof(size_t);
            this->pairs.resize(pairsVectorSize);
            // add to vector
            for (int i=0; i<pairsVectorSize; i++) {
                size_t idR, idS;
                memcpy(&idR, buffer + position, sizeof(size_t));
                position += sizeof(size_t);
                memcpy(&idS, buffer + position, sizeof(size_t));
                position += sizeof(size_t);
                this->pairs[i] = std::make_pair(idR, idS);
            }

            // pairs map
            for (int i=0; i<8; i++) {
                // relation pairs size
                size_t relationPairsSize;
                memcpy(&relationPairsSize, buffer + position, sizeof(size_t));
                position += sizeof(size_t);

                std::vector<std::pair<size_t,size_t>> vec(relationPairsSize);

                // relation pairs
                for (int j=0; j<relationPairsSize; j++) {
                    size_t idR, idS;
                    memcpy(&idR, buffer + position, sizeof(size_t));
                    position += sizeof(size_t);
                    memcpy(&idS, buffer + position, sizeof(size_t));
                    position += sizeof(size_t);

                    vec[j] = std::make_pair(idR, idS);
                }
                this->collectRelationMap[i] = vec;
            }

        } else {
            // unknown
            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type.");
            return;
        }

    }

    void QueryResult::print() {
        if (this->queryResultType == QR_COUNT) {
            switch (this->getQueryType()) {
                case hec::Q_RANGE: 
                case hec::Q_INTERSECTION_JOIN: 
                case hec::Q_INSIDE_JOIN: 
                case hec::Q_DISJOINT_JOIN: 
                case hec::Q_EQUAL_JOIN: 
                case hec::Q_COVERED_BY_JOIN: 
                case hec::Q_COVERS_JOIN: 
                case hec::Q_CONTAINS_JOIN: 
                case hec::Q_MEET_JOIN: 
                    printf("Query %d total Results: %lu\n", this->getID(), this->resultCount);
                    break;
                case hec::Q_FIND_RELATION_JOIN: 
                    printf("Query %d: \n", this->getID());
                    for (int i=0; i<8; i++) {
                        std::string relationName = mapping::relationIntToStr(i);
                        printf("    Relation %s: %lu\n", relationName.c_str(), this->countRelationMap[i]);
                    }
                    break;
                case hec::Q_NONE: 
                    printf("Invalid query result object\n");
                    break;
            }
        } else {
            printf("Unsupported");
        }
    }

}