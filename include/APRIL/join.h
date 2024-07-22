#ifndef D_APRIL_JOIN_H
#define D_APRIL_JOIN_H

#include "def.h"
#include "refinement/topology.h"

namespace APRIL
{
    namespace uncompressed
    {
        /**
         * @brief joins APRIL approximations for intersection
         */
        DB_STATUS intersectionJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &result);
        
        /**
         * @brief joins APRIL approximations for containment (R inside/covrered by S)
         */
        DB_STATUS insideCoveredByJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for disjoint (R in S)
         */
        DB_STATUS disjointJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for equality (R in S)
         */
        DB_STATUS equalJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for meet (R in S)
         */
        DB_STATUS meetJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &result);

        /**
         * @brief joins APRIL approximations for containment (R contains/covers S)
         */
        DB_STATUS containsCoversJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &result);


        namespace topology
        {
            /**
             * @brief joins APRIL approximations for topological relations of the 'R in S' containment type
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRRinSContainmentJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &relation);

            /**
             * @brief joins APRIL approximations for topological relations of the 'S in R' containment type
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRSinRContainmentJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &relation);

            /**
             * @brief joins APRIL approximations for topological relations when two MBRs are equal
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBREqualJoinAPRIL(Shape* objR, Shape* objS, AprilDataT *aprilR, AprilDataT *aprilS, int &relation);

            /**
             * @brief joins APRIL approximations for topological relations whose MBRs intersect generally
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRIntersectionJoinAPRIL(AprilDataT *aprilR, AprilDataT *aprilS, int &relation);
        }
    }
}

#endif