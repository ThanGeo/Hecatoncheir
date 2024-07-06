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


        namespace topology
        {
            /**
             * @brief joins APRIL approximations for topological relations of the 'R in S' containment type
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRRinSContainmentJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation);

            /**
             * @brief joins APRIL approximations for topological relations of the 'S in R' containment type
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRSinRContainmentJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation);

            /**
             * @brief joins APRIL approximations for topological relations when two MBRs are equal
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBREqualJoinAPRIL(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation);

            /**
             * @brief joins APRIL approximations for topological relations whose MBRs intersect generally
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRIntersectionJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation);
        }
    }
}

#endif