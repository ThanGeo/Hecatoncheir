#ifndef D_APRIL_JOIN_H
#define D_APRIL_JOIN_H

#include "containers.h"
#include "refinement/topology.h"

/** @brief APRIL related methods. */
namespace APRIL
{
    /** @brief Uncompressed APRIL methods. */
    namespace uncompressed
    {
        /** @brief Relate APRIL filter methods for different predicates. */
        namespace standard
        {
            /**
            @brief Joins APRIL approximations for intersection.
             */
            DB_STATUS intersectionJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &result);
            
            /**
            @brief Joins APRIL approximations for containment (R inside/covrered by S).
             */
            DB_STATUS insideCoveredByJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &result);

            /**
            @brief Joins APRIL approximations for disjoint (R in S).
             */
            DB_STATUS disjointJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &result);

            /**
            @brief Joins APRIL approximations for equality (R in S).
             */
            DB_STATUS equalJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &result);

            /**
            @brief Joins APRIL approximations for meet (R in S).
             */
            DB_STATUS meetJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &result);

            /**
            @brief Joins APRIL approximations for containment (R contains/covers S).
             */
            DB_STATUS containsCoversJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &result);
        }

        /** @brief Topology-related APRIL intermediate filter methods. */
        namespace topology
        {
            /**
            @brief Joins APRIL approximations for topological relations of the 'R in S' containment type
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRRinSContainmentJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &relation);

            /**
            @brief Joins APRIL approximations for topological relations of the 'S in R' containment type
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRSinRContainmentJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &relation);

            /**
            @brief Joins APRIL approximations for topological relations when two MBRs are equal
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBREqualJoinAPRIL(Shape* objR, Shape* objS, AprilData *aprilR, AprilData *aprilS, int &relation);

            /**
            @brief Joins APRIL approximations for topological relations whose MBRs intersect generally
             * @returns DB_STATUS value, and sets the relation parameter to the identified relation (or refinement)
             */
            DB_STATUS MBRIntersectionJoinAPRIL(AprilData *aprilR, AprilData *aprilS, int &relation);
        }
    }
}

#endif