#ifndef D_APRIL_FILTER_H
#define D_APRIL_FILTER_H

#include "containers.h"
#include "APRIL/join.h"
#include "refinement/topology.h"

/** @brief APRIL related methods. */
namespace APRIL
{
    /** @brief Topology-related APRIL filter methods. */
    namespace topology
    {
        /**
        @brief Optimized APRIL intermediate filter for 'find topological relation' queries that evaluates the two input objects.
         * @param[in] objR The first object (left in relation).
         * @param[in] objS The second object (right in relation).
         * @param[in] mbrRelationCase Indicates how the objects' MBR relate to each other.
         * @param[out] queryOutput Where the query result is appended.
         */
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, MBRRelationCase mbrRelationCase, QueryOutput &queryOutput);
    }

    /** @brief Standard APRIL filter methods. */
    namespace standard
    {
        /**
        @brief Standard APRIL intermediate filter that filters two input objects.
         * The join predicate is set in the global configuration.
         * @param[in] objR The first object (left in relation).
         * @param[in] objS The second object (right in relation).
         * @param[out] queryOutput Where the query result is appended.
         */
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, QueryOutput &queryOutput);
    }
}





#endif