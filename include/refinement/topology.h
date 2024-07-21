#ifndef D_REFINEMENT_TOPOLOGY_H
#define D_REFINEMENT_TOPOLOGY_H

#include "def.h"


namespace refinement 
{
    namespace topology
    {

        /**
         * Entrypoint function for when there is NO intermediate filter.
         * Intermediate filters forward to refine() function and NOT this one.
        */
        void specializedRefinementEntrypoint(Shape &objR, Shape &objS, int relationCase, QueryOutputT &queryOutput);

        /**
         * @brief refines for inside/covered by but is guaranteed intersection (no disjoint)
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineInsideCoveredbyTruehitIntersect(Shape &objR, Shape &objS);


        /**
         * @brief refines for disjoint, inside, covered by, meet and intersect
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineDisjointInsideCoveredbyMeetIntersect(Shape &objR, Shape &objS);

        /**
         * @brief refines for contains/covers but is guaranteed intersection (no disjoint)
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineContainsCoversTruehitIntersect(Shape &objR, Shape &objS);

        /**
         * @brief refines for disjoint, contains, covers, meet and intersect
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineDisjointContainsCoversMeetIntersect(Shape &objR, Shape &objS);

        /**
         * @brief refines for disjoint, meet and intersect
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineDisjointMeetIntersect(Shape &objR, Shape &objS);


        /**
         * @brief refines for covers/covered by but is guaranteed intersection (no disjoint)
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineCoversCoveredByTrueHitIntersect(Shape &objR, Shape &objS);

        /**
         * @brief refines for equal, covers, covered by but is guaranteed intersection (no disjoint)
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineEqualCoversCoveredByTrueHitIntersect(Shape &objR, Shape &objS);

        /**
         * @brief refines for covers but is guaranteed intersection (no disjoint)
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineCoversTrueHitIntersect(Shape &objR, Shape &objS);


        /**
         * @brief refines for covered by but is guaranteed intersection (no disjoint)
         * 
         * @param idR 
         * @param idS 
         * @return int 
         */
        int refineCoveredbyTrueHitIntersect(Shape &objR, Shape &objS);
        }


    namespace relate
    {
        /**
         * Entrypoint function for when there is NO intermediate filter.
         * Intermediate filters forward to refine() function and NOT this one.
        */
        void refinementEntrypoint(Shape &objR, Shape &objS, QueryTypeE queryType, QueryOutputT &queryOutput);

        /**
         * loads boost geometries and refines for intersection
        */
        void refineIntersectionJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);
        
        /**
         * loads boost geometries and refines for inside
        */
        void refineInsideJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * Loads geometries and refines for topology based on setup configuration.
        */
        void refineAllRelations(Shape &objR, Shape &objS);

        /**
         * refinement for the EQUAl topology relation
        */
        void refineEqualJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * refinement for the DISJOINT topology relation
        */
        void refineDisjointJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * refinement for the MEET topology relation
        */
        void refineMeetJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * refinement for the CONTAINS topology relation
        */
        void refineContainsJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * refinement for the COVERS topology relation
        */
        void refineCoversJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * refinement for the COVERED BY topology relation
        */
        void refineCoveredByJoin(Shape &objR, Shape &objS, QueryOutputT &queryOutput);

        /**
         * refinement for the DISJOINT, MEET or INTERSECT topology relations
        */
        int refineGuaranteedNoContainment(Shape &objR, Shape &objS);


        /**
         * refinement for containment relations and intersect ONLY (Contains, Covers, Covered by, Inside, Intersect)
        */
        int refineContainmentsOnly(Shape &objR, Shape &objS);

        /**
         * refine DISJOINT, COVERS, CONTAINS, INTERSECT only
        */
        int refineContainsPlus(Shape &objR, Shape &objS);


        /**
         * refine DISJOINT, COVERED BY, INSIDE, INTERSECT only
        */
        int refineInsidePlus(Shape &objR, Shape &objS);

        /**
         * refine for EQUAl for APRIL
         */
        bool isEqual(Shape &objR, Shape &objS);

        /**
         * refine for EQUAl for APRIL
         */
        bool isMeet(Shape &objR, Shape &objS);

    }
}

#endif