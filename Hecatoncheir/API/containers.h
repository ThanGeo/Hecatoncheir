#ifndef HEC_API_CONTAINERS_H
#define HEC_API_CONTAINERS_H

#include <string>
#include <vector>
#include <unordered_map>

#include "def.h"

namespace hec
{
    struct SupportedQueries {
    private:
        std::string range = "range";
        std::string intersects = "intersect";
        std::string disjoint = "disjoint";
        std::string equals = "equal";
        std::string meets = "meet";
        std::string inside = "inside";
        std::string contains = "contains";
        std::string covers = "covers";
        std::string covered_by = "covered_by";
        std::string find_relation = "find_relation";
    public:
        std::string RANGE() {return range;}
        std::string INTERSECTS() {return intersects;}
        std::string DISJOINT() {return disjoint;}
        std::string EQUALS() {return equals;}
        std::string MEETS() {return meets;}
        std::string INSIDE() {return inside;}
        std::string CONTAINS() {return contains;}
        std::string COVERS() {return covers;}
        std::string COVERED_BY() {return covered_by;}
        std::string FIND_RELATION() {return find_relation;}
    };

    extern SupportedQueries spatialQueries;

    struct SupportedQueryResultTypes {
    private:
        std::string count = "COUNT";
        std::string collect = "COLLECT";
    public:
        std::string COUNT() {return count;}
        std::string COLLECT() {return collect;}
    };

    extern SupportedQueryResultTypes queryResultTypes;

    /** @brief Query result type */
    enum QueryResultType {
        QR_COUNT,
        QR_COLLECT,
        QR_NONE = 777,
    };

    struct QueryResult {
    protected:
        int queryID;                                        // query ID
        QueryType queryType = Q_NONE;                       // query type
        QueryResultType queryResultType = QR_COUNT;         // default
        // for COUNT range and join query results
        size_t resultCount = 0;
        // for COUNT topology relations results (8 relations)
        size_t countRelationMap[8];
        // for COLLECT range and join query results
        std::vector<std::pair<size_t,size_t>> pairs;
        // for COLLECT topology relations results (8 relations)
        std::vector<std::pair<size_t,size_t>> collectRelationMap[8];
    public:
        QueryResult(int id, QueryType queryType, QueryResultType queryResultType);
        void reset();
        int getID() {return queryID;}
        QueryType getQueryType() {return queryType;}
        QueryResultType getResultType() {return queryResultType;}
        void countResult(size_t idR, size_t idS);
        void countTopologyRelationResult(int relation, size_t idR, size_t idS);
        void mergeResults(QueryResult &other);
        void mergeTopologyRelationResults(hec::QueryResult &other);
        size_t getResultCount();
        size_t* getTopologyResultCount();
        std::vector<std::pair<size_t,size_t>>* getResultPairs();
        int calculateBufferSize();
        void serialize(char **buffer, int &bufferSize);
        void deserialize(const char *buffer, int bufferSize);
        void print();
    };

    /** @brief unique identifier for each prepared/loaded dataset. */
    typedef int DatasetID;


    /** @brief Base query class. */
    struct Query {
    protected:
        int queryID = 0;
        QueryType queryType = Q_NONE;
        QueryResultType resultType = QR_COUNT;

    public:
        QueryType getQueryType() {
            return queryType;
        }

        QueryResultType getResultType() {
            return resultType;
        }

        int getQueryID() {
            return queryID;
        }

        Query() {
            queryID = 0;
            queryType = Q_NONE;
            resultType = QR_COUNT;
        }

        virtual ~Query() = default;
    };

    struct RangeQuery : public Query {
    private:
        DatasetID datasetID;
        std::vector<double> coords;
    public:
        RangeQuery(DatasetID datasetID, int queryID, std::vector<double> &coords);
        RangeQuery(DatasetID datasetID, int queryID, std::vector<double> &coords, std::string resultType);
        RangeQuery(DatasetID datasetID, int queryID, std::vector<double> &coords, QueryResultType resultType);
        std::vector<double> getCoords();
        DatasetID getDatasetID() {
            return datasetID;
        }
    };

    struct JoinQuery : public Query {
    private:
        DatasetID Rid;
        DatasetID Sid;
    public:
        JoinQuery(DatasetID Rid, DatasetID Sid, int queryID, std::string predicate);
        JoinQuery(DatasetID Rid, DatasetID Sid, int queryID, std::string predicate, std::string resultTypeStr);
        JoinQuery(DatasetID Rid, DatasetID Sid, int queryID, QueryType predicate, QueryResultType resultType);

        DatasetID getDatasetRid() {
            return Rid;
        }

        DatasetID getDatasetSid() {
            return Sid;
        }
    };
}


#endif