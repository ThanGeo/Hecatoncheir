#ifndef D_APRIL_FILTER_H
#define D_APRIL_FILTER_H

#include "def.h"
#include "APRIL/join.h"

namespace APRIL
{
    namespace standard
    {
        /**
         * @brief Standard APRIL intermediate filter that filters two input objects.
         * the join predicate is set in the global config variable
         */
        DB_STATUS IntermediateFilterEntrypoint(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS);
    }
}





#endif