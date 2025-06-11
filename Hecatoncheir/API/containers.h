#ifndef HEC_API_CONTAINERS_H
#define HEC_API_CONTAINERS_H

#include <string>
#include <vector>
#include <unordered_map>

#include "def.h"

namespace hec
{
    struct SpatialQueries {
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

    extern SpatialQueries spatialQueries;

    /** @brief Index types for spatial data */
    enum IndexType {
        IT_UNIFORM_GRID,
        IT_TWO_LAYER,
    };

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

    /** @brief Base Query Result struct */
    struct QResultBase {
        int queryID;                                        // query ID
        QueryType queryType = Q_NONE;                       // query type
        QueryResultType queryResultType = QR_COUNT;         // default result type

        
        int getQueryID();
        QueryType getQueryType();
        QueryResultType getResultType();
        
        virtual void addResult(size_t id) = 0;
        virtual void addResult(size_t idR, size_t idS) = 0;
        virtual void addResult(int relation, size_t idR, size_t idS) = 0;
        
        virtual void deepCopy(QResultBase* other) = 0;

        virtual int calculateBufferSize() = 0;
        /** @todo make return type int to return error codes (cannot do DB_STATUS in the API header) */
        virtual void serialize(char **buffer, int &bufferSize) = 0;
        virtual void deserialize(const char *buffer, int bufferSize) = 0;

        QueryType getQueryTypeFromSerializedBuffer(const char *buffer, int bufferSize);
        QueryResultType getResultTypeFromSerializedBuffer(const char *buffer, int bufferSize);

        virtual void mergeResults(QResultBase* other) = 0;

        virtual size_t getResultCount() = 0;
        virtual std::vector<size_t> getResultList() = 0;
        virtual std::vector<std::vector<size_t>> getTopologyResultList() = 0;

        virtual QResultBase* cloneEmpty() = 0;

        virtual ~QResultBase() = default;
    };

    struct QResultCount : public QResultBase {
    private:
        size_t resultCount;
    public:
        QResultCount(int queryID, QueryType queryType, QueryResultType queryResultType);
        void addResult(size_t id) override;
        void addResult(size_t idR, size_t idS) override;
        void addResult(int relation, size_t idR, size_t idS) override;
        int calculateBufferSize() override;
        void serialize(char **buffer, int &bufferSize) override;
        void deserialize(const char *buffer, int bufferSize) override;
        void mergeResults(QResultBase* other);
        size_t getResultCount() override;
        std::vector<size_t> getResultList() override;
        std::vector<std::vector<size_t>> getTopologyResultList() override;
        void setResult(size_t resultCount);
        void deepCopy(QResultBase* other) override; 
        QResultBase* cloneEmpty() override;
    };

    struct QResultCollect : public QResultBase {
    private:
        std::vector<size_t> resultList;
    public:
        QResultCollect(int queryID, QueryType queryType, QueryResultType queryResultType);
        void addResult(size_t id) override;
        void addResult(size_t idR, size_t idS) override;
        void addResult(int relation, size_t idR, size_t idS) override;
        int calculateBufferSize() override;
        void serialize(char **buffer, int &bufferSize) override;
        void deserialize(const char *buffer, int bufferSize) override;
        void mergeResults(QResultBase* other);
        size_t getResultCount() override;
        std::vector<size_t> getResultList();
        std::vector<std::vector<size_t>> getTopologyResultList() override;
        void setResult(std::vector<size_t> &resultList);
        void deepCopy(QResultBase* other) override; 
        QResultBase* cloneEmpty() override;
    };

    struct QPairResultCollect : public QResultBase {
    private:
        // std::vector<std::pair<size_t,size_t>> resultList;
        std::vector<size_t> resultList;
    public:
        QPairResultCollect(int queryID, QueryType queryType, QueryResultType queryResultType);
        void addResult(size_t id) override {}
        void addResult(size_t idR, size_t idS) override;
        void addResult(int relation, size_t idR, size_t idS) override {}
        int calculateBufferSize() override;
        void serialize(char **buffer, int &bufferSize) override;
        void deserialize(const char *buffer, int bufferSize) override;
        void mergeResults(QResultBase* other);
        size_t getResultCount() override;
        std::vector<size_t> getResultList();
        std::vector<std::vector<size_t>> getTopologyResultList() override;
        void setResult(std::vector<size_t> &resultList);
        void deepCopy(QResultBase* other) override; 
        QResultBase* cloneEmpty() override;
    };

    struct QTopologyResultCount : public QResultBase {
    private:
    //positions 0-7 are the unique topology relations. 
    std::vector<size_t> resultList;
    public:
        // Always size 8. 
        const int TOTAL_RELATIONS = 8;
        QTopologyResultCount(int queryID, QueryType queryType, QueryResultType queryResultType);
        void addResult(size_t id) override {}
        void addResult(size_t idR, size_t idS) {}
        void addResult(int relation, size_t idR, size_t idS) override;
        int calculateBufferSize() override;
        void serialize(char **buffer, int &bufferSize) override;
        void deserialize(const char *buffer, int bufferSize) override;
        void mergeResults(QResultBase* other);
        size_t getResultCount() override;
        std::vector<size_t> getResultList() override;
        std::vector<std::vector<size_t>> getTopologyResultList() override;
        void setResult(int relation, size_t resultCount);
        void deepCopy(QResultBase* other) override; 
        QResultBase* cloneEmpty() override;
    };

    struct QTopologyResultCollect : public QResultBase {
    private:
        // positions 0-7 are the unique topology relations. 
        std::vector<std::vector<size_t>> resultList;
        std::vector<size_t> getResultList(int relation);
        public:
        // Always size 8. 
        const int TOTAL_RELATIONS = 8;
        QTopologyResultCollect(int queryID, QueryType queryType, QueryResultType queryResultType);
        void addResult(size_t id) override {}
        void addResult(size_t idR, size_t idS) {}
        void addResult(int relation, size_t idR, size_t idS) override;
        int calculateBufferSize() override;
        void serialize(char **buffer, int &bufferSize) override;
        void deserialize(const char *buffer, int bufferSize) override;
        void mergeResults(QResultBase* other);
        size_t getResultCount() override;
        std::vector<size_t> getResultList() override;
        std::vector<std::vector<size_t>> getTopologyResultList() override;
        void setResult(int relation, std::vector<size_t> &idPairs);
        void deepCopy(QResultBase* other) override; 
        QResultBase* cloneEmpty() override;
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
        // std::vector<double> coords;
        // double xMin, yMin, xMax, yMax;
        std::string wktText;
    public:
        RangeQuery(DatasetID datasetID, int queryID, std::string queryWKT);
        RangeQuery(DatasetID datasetID, int queryID, std::string queryWKT, std::string resultType);
        RangeQuery(DatasetID datasetID, int queryID, std::string queryWKT, QueryResultType resultType);
        DatasetID getDatasetID() {
            return datasetID;
        }
        std::string getWKT();
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