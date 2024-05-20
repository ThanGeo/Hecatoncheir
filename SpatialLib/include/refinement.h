#ifndef SPATIAL_LIB_REFINEMENT_H
#define SPATIAL_LIB_REFINEMENT_H

#include "def.h"
#include "timer.h"

namespace spatial_lib 
{
    extern QueryTypeE g_queryType;

    void setupRefinement(QueryT &query);
    
    /**
     * utils
    */

    /**
     * load the geometries and return a pair of their vertex counts.
    */
    std::pair<uint,uint> getVertexCountsOfPair(uint idR, uint idS);

    /**
     * Entrypoint function for when there is NO intermediate filter.
     * Intermediate filters forward to refine() function and NOT this one.
    */
    void refinementEntrypoint(uint idR, uint idS);


    /**
     * loads boost geometries and refines for intersection
    */
    void refineIntersectionJoin(uint idR, uint idS);
    /**
     * loads boost geometries and refines for inside
    */
    void refineInsideJoin(uint idR, uint idS);

    /**
     * Loads geometries and refines for topology based on setup configuration.
    */
    void refineAllRelations(uint idR, uint idS);

    /**
     * refinement for the EQUAl topology relation
    */
    void refineEqualJoin(uint idR, uint idS);

    /**
     * refinement for the DISJOINT topology relation
    */
    void refineDisjointJoin(uint idR, uint idS);

    /**
     * refinement for the MEET topology relation
    */
    void refineMeetJoin(uint idR, uint idS);

    /**
     * refinement for the CONTAINS topology relation
    */
    void refineContainsJoin(uint idR, uint idS);

    /**
     * refinement for the COVERS topology relation
    */
    void refineCoversJoin(uint idR, uint idS);

    /**
     * refinement for the COVERED BY topology relation
    */
    void refineCoveredByJoin(uint idR, uint idS);

    /**
     * refinement for the DISJOINT, MEET or INTERSECT topology relations
    */
    int refineGuaranteedNoContainment(uint idR, uint idS);


    /**
     * refinement for containment relations and intersect ONLY (Contains, Covers, Covered by, Inside, Intersect)
    */
    int refineContainmentsOnly(uint idR, uint idS);

    /**
     * refine DISJOINT, COVERS, CONTAINS, INTERSECT only
    */
    int refineContainsPlus(uint idR, uint idS);


    /**
     * refine DISJOINT, COVERED BY, INSIDE, INTERSECT only
    */
    int refineInsidePlus(uint idR, uint idS);

    /**
     * refine for EQUAl for APRIL
     */
    bool isEqual(uint idR, uint idS);

    /**
     * refine for EQUAl for APRIL
     */
    bool isMeet(uint idR, uint idS);

    /**
     * refines for ALL relations except EQUAL
    */
    int refineAllRelationsNoEqual(uint idR, uint idS);


    /**
     * @brief refines for inside/covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineInsideCoveredbyTruehitIntersect(uint idR, uint idS);


    /**
     * @brief refines for disjoint, inside, covered by, meet and intersect
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineDisjointInsideCoveredbyMeetIntersect(uint idR, uint idS);

    /**
     * @brief refines for contains/covers but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineContainsCoversTruehitIntersect(uint idR, uint idS);

    /**
     * @brief refines for disjoint, contains, covers, meet and intersect
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineDisjointContainsCoversMeetIntersect(uint idR, uint idS);

    /**
     * @brief refines for disjoint, meet and intersect
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineDisjointMeetIntersect(uint idR, uint idS);


    /**
     * @brief refines for covers/covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineCoversCoveredByTrueHitIntersect(uint idR, uint idS);

    /**
     * @brief refines for equal, covers, covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineEqualCoversCoveredByTrueHitIntersect(uint idR, uint idS);

    /**
     * @brief refines for covers but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineCoversTrueHitIntersect(uint idR, uint idS);


    /**
     * @brief refines for covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineCoveredbyTrueHitIntersect(uint idR, uint idS);


    /**
     * @brief returns a bg polygon from disk using the offset map
     * flag R indicates which file to load from (true = R, false = S)
     * 
     * @param id 
     * @param flag 
     * @return bg_polygon 
     */
    bg_polygon loadBoostPolygonByIDandFlag(uint id, bool R);


    /**
     * Entrypoint function for when there is NO intermediate filter.
     * Intermediate filters forward to refine() function and NOT this one.
    */
    void specializedRefinementEntrypoint(uint idR, uint idS, int relationCase);
    
    namespace scalability
    {
        /**
         * Entrypoint function for when there is NO intermediate filter.
         * Intermediate filters forward to refine() function and NOT this one.
        */
        void specializedRefinementEntrypoint(uint idR, uint idS, int relationCase);
    }

}

#endif