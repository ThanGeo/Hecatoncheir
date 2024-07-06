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
    std::pair<uint,uint> getVertexCountsOfPair(PolygonT &polR, PolygonT &polS);

    /**
     * Entrypoint function for when there is NO intermediate filter.
     * Intermediate filters forward to refine() function and NOT this one.
    */
    void refinementEntrypoint(PolygonT &polR, PolygonT &polS);


    /**
     * loads boost geometries and refines for intersection
    */
    void refineIntersectionJoin(PolygonT &polR, PolygonT &polS);
    /**
     * loads boost geometries and refines for inside
    */
    void refineInsideJoin(PolygonT &polR, PolygonT &polS);

    /**
     * Loads geometries and refines for topology based on setup configuration.
    */
    void refineAllRelations(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the EQUAl topology relation
    */
    void refineEqualJoin(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the DISJOINT topology relation
    */
    void refineDisjointJoin(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the MEET topology relation
    */
    void refineMeetJoin(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the CONTAINS topology relation
    */
    void refineContainsJoin(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the COVERS topology relation
    */
    void refineCoversJoin(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the COVERED BY topology relation
    */
    void refineCoveredByJoin(PolygonT &polR, PolygonT &polS);

    /**
     * refinement for the DISJOINT, MEET or INTERSECT topology relations
    */
    int refineGuaranteedNoContainment(PolygonT &polR, PolygonT &polS);


    /**
     * refinement for containment relations and intersect ONLY (Contains, Covers, Covered by, Inside, Intersect)
    */
    int refineContainmentsOnly(PolygonT &polR, PolygonT &polS);

    /**
     * refine DISJOINT, COVERS, CONTAINS, INTERSECT only
    */
    int refineContainsPlus(PolygonT &polR, PolygonT &polS);


    /**
     * refine DISJOINT, COVERED BY, INSIDE, INTERSECT only
    */
    int refineInsidePlus(PolygonT &polR, PolygonT &polS);

    /**
     * refine for EQUAl for APRIL
     */
    bool isEqual(PolygonT &polR, PolygonT &polS);

    /**
     * refine for EQUAl for APRIL
     */
    bool isMeet(PolygonT &polR, PolygonT &polS);

    /**
     * refines for ALL relations except EQUAL
    */
    int refineAllRelationsNoEqual(PolygonT &polR, PolygonT &polS);


    /**
     * @brief refines for inside/covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineInsideCoveredbyTruehitIntersect(PolygonT &polR, PolygonT &polS);


    /**
     * @brief refines for disjoint, inside, covered by, meet and intersect
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineDisjointInsideCoveredbyMeetIntersect(PolygonT &polR, PolygonT &polS);

    /**
     * @brief refines for contains/covers but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineContainsCoversTruehitIntersect(PolygonT &polR, PolygonT &polS);

    /**
     * @brief refines for disjoint, contains, covers, meet and intersect
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineDisjointContainsCoversMeetIntersect(PolygonT &polR, PolygonT &polS);

    /**
     * @brief refines for disjoint, meet and intersect
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineDisjointMeetIntersect(PolygonT &polR, PolygonT &polS);


    /**
     * @brief refines for covers/covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineCoversCoveredByTrueHitIntersect(PolygonT &polR, PolygonT &polS);

    /**
     * @brief refines for equal, covers, covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineEqualCoversCoveredByTrueHitIntersect(PolygonT &polR, PolygonT &polS);

    /**
     * @brief refines for covers but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineCoversTrueHitIntersect(PolygonT &polR, PolygonT &polS);


    /**
     * @brief refines for covered by but is guaranteed intersection (no disjoint)
     * 
     * @param idR 
     * @param idS 
     * @return int 
     */
    int refineCoveredbyTrueHitIntersect(PolygonT &polR, PolygonT &polS);


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
    void specializedRefinementEntrypoint(PolygonT &polR, PolygonT &polS, int relationCase);
}

#endif