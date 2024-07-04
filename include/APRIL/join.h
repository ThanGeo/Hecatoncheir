#ifndef D_APRIL_JOIN_H
#define D_APRIL_JOIN_H

#include "def.h"

namespace APRIL
{
    namespace uncompressed
    {
        /**
         * @brief joins APRIL approximations for intersection
         */
        DB_STATUS intersectionJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result);
        
        /**
         * @brief joins APRIL approximations for containment (R inside/covrered by S)
         */
        DB_STATUS insideCoveredByJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for disjoint (R in S)
         */
        DB_STATUS disjointJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for equality (R in S)
         */
        DB_STATUS equalJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for meet (R in S)
         */
        DB_STATUS meetJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for containment (R contains/covers S)
         */
        DB_STATUS containsCoversJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result);
    }
}

#endif