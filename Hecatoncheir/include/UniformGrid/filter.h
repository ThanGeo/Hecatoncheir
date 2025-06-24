#ifndef D_UNIFORM_GRID_FILTER_H
#define D_UNIFORM_GRID_FILTER_H

#include "containers.h"
#include "../API/containers.h"
#include <omp.h>

/** @brief The Uniform Grid MBR filter methods. */
namespace uniform_grid
{
    /** @brief Begins the query processing specified by the query object and stores the result in the query result object. */
    DB_STATUS processQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult);

    namespace distance_filter
    {
        /** @brief Processes the DISTANCE JOIN query. Stores the edge objects in the borderObjectsMap map for each node. */
        DB_STATUS processQuery(hec::Query* query, std::unordered_map<int, std::vector<size_t>> &borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult);
    }
}


#endif