#ifndef D_APRIL_FILTER_H
#define D_APRIL_FILTER_H

#include "containers.h"
#include "APRIL/join.h"
#include "refinement/topology.h"

namespace APRIL
{
    namespace topology
    {
        /**
         * @brief Optimized APRIL intermediate filter for 'find topological relation' queries that filters two input objects
         */
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, MBRRelationCaseE mbrRelationCase, QueryOutput &queryOutput);
    }

    namespace standard
    {
        /**
         * @brief Standard APRIL intermediate filter that filters two input objects.
         * the join predicate is set in the global config variable
         */
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, QueryOutput &queryOutput);
    }
}





#endif