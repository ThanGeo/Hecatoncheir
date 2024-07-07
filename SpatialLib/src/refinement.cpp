#include "refinement.h"

namespace spatial_lib
{   
    /**
     * TOPOLOGY RELATION DE-9IM CODES
    */
    char insideCode[] = "T*F**F***";
    // char trueInsideCode[] = "T*F*FF***";
    char coveredbyCode1[] = "T*F**F***";
    char coveredbyCode2[] = "*TF**F***";
    char coveredbyCode3[] = "**FT*F***";
    char coveredbyCode4[] = "**F*TF***";
    char containsCode[] = "T*****FF*";
    char coversCode1[] = "T*****FF*";
    char coversCode2[] = "*T****FF*";
    char coversCode3[] = "***T**FF*";
    char coversCode4[] = "****T*FF*";
    char meetCode1[] = "FT*******"; 
    char meetCode2[] = "F**T*****"; 
    char meetCode3[] = "F***T****"; 
    char equalCode[] = "T*F**FFF*"; 
    char disjointCode[] = "FF*FF****";
    char intersectCode1[] = "T********";
    char intersectCode2[] = "*T*******";
    char intersectCode3[] = "***T*****";
    char intersectCode4[] = "****T****";

    //define topological masks for refinement
    // a inside b
    boost::geometry::de9im::mask insideMask(insideCode); 
    // a contains b
    boost::geometry::de9im::mask containsMask(containsCode); 
    // a covered by b
    std::vector<boost::geometry::de9im::mask> coveredByMaskList = {
                    boost::geometry::de9im::mask(coveredbyCode1),
                    boost::geometry::de9im::mask(coveredbyCode2),
                    boost::geometry::de9im::mask(coveredbyCode3),
                    boost::geometry::de9im::mask(coveredbyCode4)};
    // a covers b
    std::vector<boost::geometry::de9im::mask> coversMaskList = {
                    boost::geometry::de9im::mask(coversCode1),
                    boost::geometry::de9im::mask(coversCode2),
                    boost::geometry::de9im::mask(coversCode3),
                    boost::geometry::de9im::mask(coversCode4)};
    // a and b meet
    boost::geometry::de9im::mask meetMask1(meetCode1); 
    boost::geometry::de9im::mask meetMask2(meetCode2); 
    boost::geometry::de9im::mask meetMask3(meetCode3); 
    // a and b are equal
    boost::geometry::de9im::mask equalMask(equalCode); 
    // a and b are disjoint
    boost::geometry::de9im::mask disjointMask(disjointCode); 
    // a overlaps b
    std::vector<boost::geometry::de9im::mask> overlapMaskList = {
                    boost::geometry::de9im::mask(intersectCode1),
                    boost::geometry::de9im::mask(intersectCode2),
                    boost::geometry::de9im::mask(intersectCode3),
                    boost::geometry::de9im::mask(intersectCode4)};

    // input fstream for vector data
    std::ifstream finR;
    std::ifstream finS;
    //offset maps for binary geometries
    std::unordered_map<uint,unsigned long> offsetMapR;
    std::unordered_map<uint,unsigned long> offsetMapS;
    
    QueryTypeE g_queryType;

    /**
     * util
    */
    std::pair<uint,uint> getVertexCountsOfPair(PolygonT &polR, PolygonT &polS) {
        return std::make_pair(polR.boostPolygon.outer().size(), polS.boostPolygon.outer().size());
    }


    /**
     * Boost geometry refinement for specific relations
     */

    static int refineIntersection(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::intersects(polygonR, polygonS);
    }

    static int refineInside(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::within(polygonR, polygonS);
    }

    static int refineContains(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::within(polygonS, polygonR);
    }

    static int refineDisjoint(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::disjoint(polygonR, polygonS);
    }

    static int refineEqual(bg_polygon &polygonR, bg_polygon &polygonS){
        // dont use this bad function. 
        // Returns false for some equal polygons (maybe due to double precision errors)
        // return boost::geometry::equals(polygonR, polygonS);

        // instead, use the relate method that performs the DE-9IM matrix
        return boost::geometry::relate(polygonR, polygonS, equalMask);
    }

    static int refineMeet(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::touches(polygonR, polygonS);
    }

    static int refineCovers(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::covered_by(polygonS, polygonR);
    }

    static int refineCoveredBy(bg_polygon &polygonR, bg_polygon &polygonS){
        return boost::geometry::covered_by(polygonR, polygonS);
    }

    /**
     * Boost geometry refinement for find relation
     */


    static inline bool compareDe9imChars(char character, char char_mask) {
        if (character != 'F' && char_mask == 'T') {
            // character is 0,1,2 and char_mask is T
            return true;
        } else if (character == 'F' && char_mask == 'F'){
            // both are F
            return true;
        } else {
            // no match
            return false;
        }
    }

    static inline bool compareMasks(std::string &de9imCode, char* maskCode) {
        for(int i=0; i<9; i++) {
            if (de9imCode[i] == '*' || maskCode[i] == '*' || compareDe9imChars(de9imCode[i], maskCode[i])){
                continue;
            } else {
                return false;
            }
        }
        return true;
    }

    static inline std::string createMaskCode(bg_polygon &polygonR, bg_polygon &polygonS) {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(polygonR, polygonS);
        return matrix.str();
    }

    static int refineFindRelation(bg_polygon &polygonR, bg_polygon &polygonS, bool markedForEqual){
        // get the mask codes
        std::string code = createMaskCode(polygonR, polygonS);

        //disjoint
        if(compareMasks(code, disjointCode)){
            return TR_DISJOINT;
        }

        /**
         * it definitely intersects at this point
        */

        if (markedForEqual) {
            // check equality first because it is a subset of covers and covered by
            if(compareMasks(code, equalCode)){
                return TR_EQUAL;
            }
        }

        // covers
        if(compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || compareMasks(code, coversCode3)|| compareMasks(code, coversCode4)){
            // first check contains because it is a subset of covers
            if(compareMasks(code, containsCode)){
                return TR_CONTAINS;
            }
            return TR_COVERS;
        }

        // covered by
        if(compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || compareMasks(code, coveredbyCode3)|| compareMasks(code, coveredbyCode4)){
            // first check inside because it is a subset of covered by
            if(compareMasks(code, insideCode)){
                return TR_INSIDE;
            }
            return TR_COVERED_BY;
        }

        // meet
        if(compareMasks(code, meetCode1) || 
                    compareMasks(code, meetCode2) || 
                    compareMasks(code, meetCode3)){
            return TR_MEET;
        }

        // else return intersects
        return TR_INTERSECT;
    }

    /**
     * Refine find specific relation
     */

    void refineIntersectionJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineIntersection(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    void refineInsideJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineInside(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    void refineDisjointJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineDisjoint(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    void refineEqualJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineEqual(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    void refineMeetJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineMeet(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    void refineContainsJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineContains(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            // printf("%d,%d\n", polR.recID, polS.recID);
            queryOutput.countResult();
        }
    }

    void refineCoversJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineCovers(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    void refineCoveredByJoin(PolygonT &polR, PolygonT &polS, QueryOutputT &queryOutput) {
        // refine
        int refinementResult = refineCoveredBy(polR.boostPolygon, polS.boostPolygon);
        // count result
        if (refinementResult) {
            queryOutput.countResult();
        }
    }

    /**
     * Find Relation
     */
    void refineAllRelations(PolygonT &polR, PolygonT &polS) {
        // refine
        int refinementResult = refineFindRelation(polR.boostPolygon, polS.boostPolygon, true);
        // count result
        g_queryOutput.countTopologyRelationResult(refinementResult);
    }

    /**
     * FOR APRIL
     */

    bool isEqual(PolygonT &polR, PolygonT &polS) {
        // refine
        return refineEqual(polR.boostPolygon, polS.boostPolygon);
    }

    bool isMeet(PolygonT &polR, PolygonT &polS) {
        // refine
        return refineMeet(polR.boostPolygon, polS.boostPolygon);
    }

    int refineAllRelationsNoEqual(PolygonT &polR, PolygonT &polS) {
        // refine
        return refineFindRelation(polR.boostPolygon, polS.boostPolygon, false);
    }

    int refineGuaranteedNoContainment(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        if (compareMasks(code, disjointCode)) {
            return TR_DISJOINT;
        }
        if (compareMasks(code, meetCode1) || compareMasks(code, meetCode2) || compareMasks(code, meetCode3)) {
            return TR_MEET;
        }
        return TR_INTERSECT;
    }

    int refineContainmentsOnly(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            if (compareMasks(code, containsCode)) {
                return TR_CONTAINS;
            }
            return TR_COVERS;
        }
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            if (compareMasks(code, insideCode)) {
                return TR_INSIDE;
            }
            return TR_COVERED_BY;
        }
        return TR_INTERSECT;
    }

    int refineContainsPlus(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        if (compareMasks(code, disjointCode)) {
            return TR_DISJOINT;
        }
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            if (compareMasks(code, containsCode)) {
                return TR_CONTAINS;
            }
            return TR_COVERS;
        }
        if (compareMasks(code, meetCode1) || compareMasks(code, meetCode2) || compareMasks(code, meetCode3)) {
            return TR_MEET;
        }
        return TR_INTERSECT;
    }

    int refineInsidePlus(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        if (compareMasks(code, disjointCode)) {
            return TR_DISJOINT;
        }
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            if (compareMasks(code, insideCode)) {
                return TR_INSIDE;
            }
            return TR_COVERED_BY;
        }
        if (compareMasks(code, meetCode1) || compareMasks(code, meetCode2) || compareMasks(code, meetCode3)) {
            return TR_MEET;
        }
        return TR_INTERSECT;
    }

    // void refinementEntrypoint(PolygonT &polR, PolygonT &polS) {
    //     // time
    //     time::markRefinementFilterTimer();
    //     // count post mbr candidate
    //     g_queryOutput.postMBRFilterCandidates += 1;
    //     g_queryOutput.refinementCandidates += 1;
    //     switch(g_queryType) {
    //         case Q_INTERSECT:
    //             refineIntersectionJoin(polR, polS);
    //             break;
    //         case Q_INSIDE:
    //             refineInsideJoin(polR, polS);
    //             break;
    //         case Q_DISJOINT:
    //             refineDisjointJoin(polR, polS);
    //             break;
    //         case Q_EQUAL:
    //             refineEqualJoin(polR, polS);
    //             break;
    //         case Q_MEET:
    //             refineMeetJoin(polR, polS);
    //             break;
    //         case Q_CONTAINS:
    //             refineContainsJoin(polR, polS);
    //             break;
    //         case Q_COVERS:
    //             refineCoversJoin(polR, polS);
    //             break;
    //         case Q_COVERED_BY:
    //             refineCoveredByJoin(polR, polS);
    //             break;
    //         case Q_FIND_RELATION:
    //             refineAllRelations(polR, polS);
    //             break;
    //         default:
    //             fprintf(stderr, "Failed. No refinement support for query type.\n");
    //             exit(-1);
    //             break;
    //     }
    //     // store time
    //     g_queryOutput.refinementTime += time::getElapsedTime(time::g_timer.refTimer);
    // }


    /************************************************/
    /**
     * SPECIALIZED TOPOLOGY
    */
    /************************************************/

    int refineInsideCoveredbyTruehitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // covered by
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            // inside
            if (compareMasks(code, insideCode)) {
                return TR_INSIDE;
            }
            return TR_COVERED_BY;
        }
        // true hit intersect
        return TR_INTERSECT;
    }

    int refineDisjointInsideCoveredbyMeetIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // disjoint
        if (compareMasks(code, disjointCode)) {
            return TR_DISJOINT;
        }
        // covered by
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            // inside
            if (compareMasks(code, insideCode)) {
                return TR_INSIDE;
            }
            return TR_COVERED_BY;
        }
        // meet
        if (compareMasks(code, meetCode1) || compareMasks(code, meetCode2) || compareMasks(code, meetCode3)) {
            return TR_MEET;
        }
        // intersect
        return TR_INTERSECT;
    }

    int refineContainsCoversTruehitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // covers
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            // contains
            if (compareMasks(code, containsCode)) {
                return TR_CONTAINS;
            }
            return TR_COVERS;
        }
        // true hit intersect
        return TR_INTERSECT;
    }

    int refineDisjointContainsCoversMeetIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // disjoint
        if (compareMasks(code, disjointCode)) {
            return TR_DISJOINT;
        }
        // covers
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            // contains
            if (compareMasks(code, containsCode)) {
                return TR_CONTAINS;
            }
            return TR_COVERS;
        }
        // meet
        if (compareMasks(code, meetCode1) || compareMasks(code, meetCode2) || compareMasks(code, meetCode3)) {
            return TR_MEET;
        }
        // intersect
        return TR_INTERSECT;
    }


    int refineDisjointMeetIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);   
        // disjoint
        if (compareMasks(code, disjointCode)) {
            return TR_DISJOINT;
        }
        // meet
        if (compareMasks(code, meetCode1) || compareMasks(code, meetCode2) || compareMasks(code, meetCode3)) {
            return TR_MEET;
        }
        // intersect
        return TR_INTERSECT;
    }

    int refineCoversCoveredByTrueHitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);     
        // covers
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            // return TR_COVERS;
            // instead of covers, classify as contains for consistency with the DE-9IM
            return TR_CONTAINS;
        }
        // covered by
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            // return TR_COVERED_BY;
            // instead of covers, classify as contains for consistency with the DE-9IM
            return TR_INSIDE;
        }
        // intersect
        return TR_INTERSECT;
    }

    int refineEqualCoversCoveredByTrueHitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // equal
        if(compareMasks(code, equalCode)) {
            return TR_EQUAL;
        } 
        // covers
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            // return TR_COVERS;
            // instead of covers, classify as contains for consistency with the DE-9IM
            return TR_CONTAINS;
        }

        // covered by
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            // return TR_COVERED_BY;
            // instead of covers, classify as contains for consistency with the DE-9IM
            return TR_INSIDE;
        }
        // intersect
        return TR_INTERSECT;
    }


    int refineCoversTrueHitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // covers
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            // return TR_COVERS;
            // return contains for consistency
            return TR_CONTAINS;
        }
        // intersect
        return TR_INTERSECT;
    }


    int refineCoveredbyTrueHitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);    
        // covered by
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            // return TR_COVERED_BY;
            // return inside for consistency
            return TR_INSIDE;
        }
        // intersect
        return TR_INTERSECT;
    }

    int refineEqualCoversCoveredbyTrueHitIntersect(PolygonT &polR, PolygonT &polS) {
        // get the mask code
	    std::string code = createMaskCode(polR.boostPolygon, polS.boostPolygon);
        // check equality first because it is a subset of covers and covered by
        if(compareMasks(code, equalCode)){
            return TR_EQUAL;
        }
        // covers
        if (compareMasks(code, coversCode1) || compareMasks(code, coversCode2) || 
            compareMasks(code, coversCode3) || compareMasks(code, coversCode4)) {
            // return TR_COVERS;
            // instead of covers, classify as contains for consistency with the DE-9IM
            return TR_CONTAINS;
        }
        // covered by
        if (compareMasks(code, coveredbyCode1) || compareMasks(code, coveredbyCode2) || 
            compareMasks(code, coveredbyCode3) || compareMasks(code, coveredbyCode4)) {
            // return TR_COVERED_BY;
            // instead of covers, classify as contains for consistency with the DE-9IM
            return TR_INSIDE;
        }
        // intersect
        return TR_INTERSECT;
    }

    void specializedRefinementEntrypoint(PolygonT &polR, PolygonT &polS, int relationCase) {
        // time
        time::markRefinementFilterTimer();
        // count post mbr candidate
        g_queryOutput.postMBRFilterCandidates += 1;
        g_queryOutput.refinementCandidates += 1;
        
        int refinementResult;

        switch(relationCase) {
            case MBR_R_IN_S:
                refinementResult = refineDisjointInsideCoveredbyMeetIntersect(polR, polS);
                break;
            case MBR_S_IN_R:
                refinementResult = refineDisjointContainsCoversMeetIntersect(polR, polS);
                break;
            case MBR_EQUAL:
                refinementResult = refineEqualCoversCoveredbyTrueHitIntersect(polR, polS);
                break;
            case MBR_INTERSECT:
                refinementResult = refineDisjointMeetIntersect(polR, polS);
                break;
            default:
                fprintf(stderr, "Failed. No refinement support for this relation case.\n");
                exit(-1);
                break;
        }

        // count result
        g_queryOutput.countTopologyRelationResult(refinementResult);

        // store time
        g_queryOutput.refinementTime += time::getElapsedTime(time::g_timer.refTimer);
    }

    bg_polygon loadBoostPolygonByIDandFlag(uint id, bool R) {
        if (R) {
            return loadPolygonFromDiskBoostGeometry(id, finR, offsetMapR);
        }
        return loadPolygonFromDiskBoostGeometry(id, finS, offsetMapS);
    }


    void setupRefinement(QueryT &query){
        // open data files and keep open
        finR.open(query.R.path, std::fstream::in | std::ios_base::binary);
        finS.open(query.S.path, std::fstream::in | std::ios_base::binary);
        // load offset maps
        offsetMapR = loadOffsetMap(query.R.offsetMapPath);
        offsetMapS = loadOffsetMap(query.S.offsetMapPath);
        // setup query type
        g_queryType = query.type;
    }

}