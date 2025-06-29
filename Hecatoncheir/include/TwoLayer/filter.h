#ifndef D_TWOLAYER_FILTER_H
#define D_TWOLAYER_FILTER_H

#include "containers.h"
#include "../API/containers.h"

/** @brief The two-layer MBR filter methods. */
namespace twolayer
{
    /** @brief Begins the query processing specified by the query object and stores the result in the query result object. */
    DB_STATUS processQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult);
    
    namespace range_filter
    {
        /** @brief MBR range query filter (non-point data) 
         * Forwards pairs to refinement (no intermediate filter). */
        DB_STATUS evaluate(hec::RangeQuery *rangeQuery, std::unique_ptr<hec::QResultBase>& queryResult);
    }
    

    namespace intersection_join_filter
    {
        /** @brief MBR intersection join filter (non-point data). 
         * Forwards pairs to either an intermediate filter (APRIL) or refinement. */
        DB_STATUS evaluate(hec::PredicateJoinQuery *joinQuery, std::unique_ptr<hec::QResultBase>& queryResult);
    }

    namespace topological_join_filter
    {
        /** @brief Specialized MBR join filter that considers MBRs in a topological manner for more-than-intersection join predicates. 
         * Forwards pairs to either an intermediate filter (APRIL) or refinement. */
        DB_STATUS evaluate(hec::PredicateJoinQuery *joinQuery, std::unique_ptr<hec::QResultBase>& queryResult);
    }

}


#endif