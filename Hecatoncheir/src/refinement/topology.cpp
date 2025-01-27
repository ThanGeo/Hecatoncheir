#include "refinement/topology.h"

namespace refinement
{   
    namespace topology
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


        /************************************************/
        /**
         * SPECIALIZED TOPOLOGY
        */
        /************************************************/

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

        int refineInsideCoveredbyTruehitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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

        int refineDisjointInsideCoveredbyMeetIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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

        int refineContainsCoversTruehitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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

        int refineDisjointContainsCoversMeetIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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


        int refineDisjointMeetIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);   
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

        int refineCoversCoveredByTrueHitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);     
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

        int refineEqualCoversCoveredByTrueHitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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


        int refineCoversTrueHitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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


        int refineCoveredbyTrueHitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);    
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

        int refineEqualCoversCoveredbyTrueHitIntersect(Shape* objR, Shape* objS) {
            // get the mask code
            std::string code = objR->createMaskCode(*objS);
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


        DB_STATUS specializedRefinementEntrypoint(Shape* objR, Shape* objS, int mbrRelationCase, hec::QueryResult &queryResult) {
            int refinementResult = -1;
            // switch based on MBR intersection case
            switch(mbrRelationCase) {
                case MBR_R_IN_S:
                    refinementResult = refineDisjointInsideCoveredbyMeetIntersect(objR, objS);
                    break;
                case MBR_S_IN_R:
                    refinementResult = refineDisjointContainsCoversMeetIntersect(objR, objS);
                    break;
                case MBR_EQUAL:
                    refinementResult = refineEqualCoversCoveredbyTrueHitIntersect(objR, objS);
                    break;
                case MBR_INTERSECT:
                    refinementResult = refineDisjointMeetIntersect(objR, objS);
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "no refinement support for mbr relation case", mbrRelationCase);
                    return DBERR_QUERY_INVALID_TYPE;
            }
            // count result
            queryResult.countTopologyRelationResult(refinementResult, objR->recID, objS->recID);

            return DBERR_OK;
        }
    }

    namespace relate
    {

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
            return boost::geometry::relate(polygonR, polygonS, topology::equalMask);
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
         * Refine find specific relation
         */

        void refineIntersectionJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            // printf("Refining objects %ld and %ld\n", objR->recID, objS->recID);
            // objR->printGeometry();
            // objS->printGeometry();
            // printf("Data types: %s and %s\n", objR->getShapeType().c_str(), objS->getShapeType().c_str());
            if (objR->intersects(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
                // printf("%lu,%lu\n", objR->recID, objS->recID);
                // if (objR->recID == 101911 && objS->recID == 1691538) {
                //     logger::log_task(objR->recID, "and", objS->recID, "is result");
                // }
            }
        }

        void refineInsideJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->inside(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }

        void refineDisjointJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->disjoint(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }

        void refineEqualJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->equals(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }

        void refineMeetJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->meets(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }

        void refineContainsJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->contains(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }

        void refineCoversJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->covers(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }

        void refineCoveredByJoin(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            if (objR->coveredBy(*objS)) {
                queryResult.countResult(objR->recID, objS->recID);
            }
        }
        
        DB_STATUS refinementEntrypoint(Shape* objR, Shape* objS, hec::QueryType queryType, hec::QueryResult &queryResult) {
            // switch based on query type
            switch(queryType) {
                case hec::Q_RANGE:
                    return DBERR_FEATURE_UNSUPPORTED;
                case hec::Q_INTERSECTION_JOIN:
                    refineIntersectionJoin(objR, objS, queryResult);
                    break;
                case hec::Q_INSIDE_JOIN:
                    refineInsideJoin(objR, objS, queryResult);
                    break;
                case hec::Q_DISJOINT_JOIN:
                    refineDisjointJoin(objR, objS, queryResult);
                    break;
                case hec::Q_EQUAL_JOIN:
                    refineEqualJoin(objR, objS, queryResult);
                    break;
                case hec::Q_MEET_JOIN:
                    refineMeetJoin(objR, objS, queryResult);
                    break;
                case hec::Q_CONTAINS_JOIN:
                    refineContainsJoin(objR, objS, queryResult);
                    break;
                case hec::Q_COVERS_JOIN:
                    refineCoversJoin(objR, objS, queryResult);
                    break;
                case hec::Q_COVERED_BY_JOIN:
                    refineCoveredByJoin(objR, objS, queryResult);
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "no refinement support for query:", mapping::queryTypeIntToStr(queryType));
                    return DBERR_QUERY_INVALID_TYPE;
            }

            return DBERR_OK;
        }
        
        /**
         * FOR APRIL
         */

        bool isEqual(Shape* objR, Shape* objS) {
            return objR->equals(*objS);
        }

        bool isMeet(Shape* objR, Shape* objS) {
            return objR->meets(*objS);
        }
    }

}