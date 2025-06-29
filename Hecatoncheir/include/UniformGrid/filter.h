#ifndef D_UNIFORM_GRID_FILTER_H
#define D_UNIFORM_GRID_FILTER_H

#include "containers.h"
#include "../API/containers.h"

/** @brief The Uniform Grid MBR filter methods. */
namespace uniform_grid
{
    /** @brief Begins the query processing specified by the query object and stores the result in the query result object. */
    DB_STATUS processQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult);

    /** @brief Alternative method to process the DISTANCE JOIN query. 
     * Stores the edge objects that need to be exchanged in the borderObjectsMap map for each node. */
    DB_STATUS processQuery(hec::Query* query, std::unordered_map<int, DJBatch> &borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult);
    
    namespace range_filter
    {
        /** @brief Evaluates a range query on the Uniform Grid Index */
        DB_STATUS evaluate(hec::RangeQuery *rangeQuery, std::unique_ptr<hec::QResultBase>& queryResult);
    }
    
    namespace knn_filter
    {        
        /** @brief Evaluates a knn query on the Uniform Grid index. */
        DB_STATUS evaluate(hec::KNNQuery *knnQuery, std::unique_ptr<hec::QResultBase>& queryResult);
    }
    
    namespace distance_filter
    {
        /** @brief Evaluates a distance join query on the Uniform Grid index. */
        DB_STATUS evaluate(hec::DistanceJoinQuery *distanceJoinQuery, std::unordered_map<int, DJBatch>& borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult);

        /** @brief Evaluates the DISTANCE JOIN query on the objects of the batch against the already stored/indexed objects. */
        DB_STATUS evaluateDJBatch(hec::DistanceJoinQuery *distanceJoinQuery, DJBatch& batch, std::unique_ptr<hec::QResultBase>& queryResult);
    }
}


#endif