#ifndef D_REFINEMENT_TOPOLOGY_H
#define D_REFINEMENT_TOPOLOGY_H

#include "containers.h"
#include "../API/containers.h"

/** @brief Spatial refinement methods that use the objects' geometries to determine a query's predicate. */
namespace refinement 
{
    /** @brief Find exact topology relationship refinement methods. */
    namespace topology
    {
        /** @brief Entrypoint function for topological relationship refinement, for when there is NO intermediate filter. 
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
         * @param[in] mbrRelationCase How the objects' MBR intersect with each other.
         * @param[out] queryOutput Where the pair's result will be appended to.
        */
        DB_STATUS specializedRefinementEntrypoint(Shape* objR, Shape* objS, int mbrRelationCase, hec::QueryResult &queryOutput);

        /** @brief Refines for the inside and covered by relation predicates, with the intersection being guaranteed.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineInsideCoveredbyTruehitIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the disjoint, inside, covered by, meet and intersect relation predicates.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineDisjointInsideCoveredbyMeetIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the contains and covers relation predicates, with the intersection being guaranteed.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineContainsCoversTruehitIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the disjoint, contains, covers, meet and intersect relation predicates.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineDisjointContainsCoversMeetIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the disjoint, meet and intersect relation predicates (no containment).
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineDisjointMeetIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the covers and covered by relation predicates, with the intersection being guaranteed.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineCoversCoveredByTrueHitIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the equals, covers and covered by relation predicates, with the intersection being guaranteed.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineEqualCoversCoveredByTrueHitIntersect(Shape* objR, Shape* objS);

        /** @brief Refines for the covers relation predicate, with the intersection being guaranteed.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineCoversTrueHitIntersect(Shape* objR, Shape* objS);


        /** @brief Refines for the covered relation predicate, with the intersection being guaranteed.
         * @param[in] objR The first object (left in relation)
         * @param[in] objS The second object (right in relation)
        */
        int refineCoveredbyTrueHitIntersect(Shape* objR, Shape* objS);
    }

    /** @brief Refinement methods for the relate with predicate queries. */
    namespace relate
    {
        /** @brief Entrypoint function for when there is NO intermediate filter. */
        DB_STATUS refinementEntrypoint(Shape* objR, Shape* objS, hec::QueryType queryType, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for intersection. */
        void refineIntersectionJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);
        
        /** @brief Geometrically refines two objects for 'R inside S'. */
        void refineInsideJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for spatial equality. */
        void refineEqualJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for whether they are disjoint. */
        void refineDisjointJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for whether R and S meet (touch). */
        void refineMeetJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for 'R contains S'. */
        void refineContainsJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for 'R covers S'. */
        void refineCoversJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Geometrically refines two objects for 'R is covered by S'. */
        void refineCoveredByJoin(Shape* objR, Shape* objS, hec::QueryResult &queryOutput);

        /** @brief Returns true of the two objects are spatially equal. */
        bool isEqual(Shape* objR, Shape* objS);

        /** @brief Returns true of the two objects meet (touch). */
        bool isMeet(Shape* objR, Shape* objS);
    }
}

#endif