#include "APRIL/join.h"

namespace APRIL
{
    /**
     * @brief returns 1 if the two interval lists have at least one overlap, otherwise zero
     * if any of them is empty, they can not overlap
     */
    static inline int intersectionJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
        // printf("%u and %u intervals\n", numintervals1, numintervals2);
        //they may not have any intervals of this type
        if(numintervals1 == 0 || numintervals2 == 0){
            return 0;
        }
        uint cur1=0;
        uint cur2=0;
        auto st1 = ar1.begin();
        auto end1 = ar1.begin() + 1;
        cur1++;
        auto st2 = ar2.begin();
        auto end2 = ar2.begin() + 1;
        cur2++;
        do {
            // printf("    (%u,%u) and (%u,%u)\n",*st1,*end1,*st2,*end2);
            if (*st1<=*st2) {
                // to check for overlap, end1>=st2 if intervals are [s,e] and end1>st2 if intervals are [s,e)
                if (*end1>*st2) {
                    //they overlap, return 1
                    return 1;
                } else {
                    st1 += 2;
                    end1 += 2;
                    cur1++;
                }
            } else {
                // st2<st1
                // overlap, end2>=st1 if intervals are [s,e] and end2>st1 if intervals are [s,e)
                if (*end2>*st1) {
                    //they overlap, return 1
                    return 1;
                } else {
                    st2 += 2;
                    end2 += 2;
                    cur2++;
                }
            }
        } while(cur1<=numintervals1 && cur2<=numintervals2);
        //no overlap, return 0
        return 0;
    }

    /**
     * @brief returns 1 if the intervals of the first list (ar1) are completely inside the intervals of the second list (ar2)
     * otherwise 0
     */
    static inline int insideJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
        //they may not have any intervals of this type
        if(numintervals1 == 0 || numintervals2 == 0){
            return 0;
        }
        // ID st1,st2,end1,end2;
        uint cur1=0;
        uint cur2=0;        
        auto st1 = ar1.begin();
        auto end1 = ar1.begin() + 1;
        cur1++;
        auto st2 = ar2.begin();
        auto end2 = ar2.begin() + 1;
        cur2++;
        bool intervalRcontained = false;
        do {
            //check if it is contained completely
            if(*st1 >= *st2 && *end1 <= *end2){
                intervalRcontained = true;
            }
            if (*end1<=*end2) {
                if(!intervalRcontained){
                    //we are skipping this interval because it was never contained, so return false (not inside)
                    return 0;
                }
                st1 += 2;
                end1 += 2;
                cur1++;
                intervalRcontained = false;
            } else {
                st2 += 2;
                end2 += 2;
                cur2++;
            }
        } while(cur1<=numintervals1 && cur2<=numintervals2);  
        //if we didnt check all of the R intervals
        if(cur1 <= numintervals1){	
            return 0;
        }
        //all intervals R were contained
        return 1;
    }

    /**
     * @brief returns 1 if the two interval lists match 100%
     * otherwise 0
     */
    static inline int joinIntervalsForMatch(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
        // if interval lists have different sizes (or both are 0) then they cannot match
        if(numintervals1 != numintervals2){
            return 0;
        }
        // if both are zero, they match
        if(numintervals1 == 0 && numintervals2 == 0) {
            return 1;
        }
        // ID st1,st2,end1,end2;
        uint cur1=0;
        uint cur2=0;
        auto st1 = ar1.begin();
        auto end1 = ar1.begin() + 1;
        cur1++;
        auto st2 = ar2.begin();
        auto end2 = ar2.begin() + 1;
        cur2++;
        do {
            //check if the current interval of R is contained completely in interval of S
            if(*st1 != *st2 || *end1 != *end2){
                return 0;
            }
            // move both lists' indexes
            st1 += 2;
            end1 += 2;
            cur1++;
            st2 += 2;
            end2 += 2;
            cur2++;
        } while(cur1<=numintervals1 && cur2<=numintervals2);     
        //if we didnt check all of the intervals, then the lists do not match
        if(cur1 <= numintervals1 || cur2 <= numintervals2){	
            return 0;
        }
        //all intervals match
        return 1;
    }

    /**
     * @brief returns a value to indicate the relationship between the two interval lists
     * could be that ar1 is inside ar2, the reverse, that they intersect or that they are disjoint
     */
    static inline int joinIntervalsHybrid(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
        bool intersect = false;
        //they may not have any intervals of this type
        if(numintervals1 == 0 || numintervals2 == 0){
            return spatial_lib::IL_DISJOINT;
        }
        uint cur1=0;
        uint cur2=0;
        auto st1 = ar1.begin();
        auto end1 = ar1.begin() + 1;
        cur1++;
        auto st2 = ar2.begin();
        auto end2 = ar2.begin() + 1;
        cur2++;
        bool intervalRcontained = false;
        do {
            //check if the current interval of R is contained completely in interval of S
            if(*st1 >= *st2 && *end1 <= *end2){
                intervalRcontained = true;
                intersect = true;
                // printf("1 contained in 2!\n");
            }else if(*st1<=*st2 && *end1>*st2){
                // there is an intersection
                intersect = true;
            }else if(*st1>*st2 && *st1<*end2){
                // there is an intersection
                intersect = true;
            }
            if (*end1<=*end2) {
                if(!intervalRcontained){
                    // found an interval that is not contained, continue looking for intersections
                    // if one was not found yet
                    if (!intersect) {
                        // get next interval
                        st1 += 2;
                        end1 += 2;
                        cur1++;
                        // and jump to continue looking for intersection
                        goto LOOK_FOR_OVERLAP;
                    }
                    // we have an intersection and non-containment, so return non-containment
                    return spatial_lib::IL_INTERSECT;
                }
                st1 += 2;
                end1 += 2;
                cur1++;
                intervalRcontained = false;
            } else {
                st2 += 2;
                end2 += 2;
                cur2++;
            }
        } while(cur1<=numintervals1 && cur2<=numintervals2);
        //if we didnt check all of the R intervals
        if(cur1 <= numintervals1/* || !intervalRcontained*/){	
            // printf("returning non-containment\n");
            if(intersect) {
                return spatial_lib::IL_INTERSECT;
            }
            return spatial_lib::IL_DISJOINT;
        }
        //all intervals R were contained
        return spatial_lib::IL_R_INSIDE_S;
    LOOK_FOR_OVERLAP:
        // after this point it is guaranteed that there is NO containment. So after an intersection is found, return
        do {
            if (*st1<=*st2) {
                // overlap, end1>=st2 if intervals are [s,e] and end1>st2 if intervals are [s,e)
                if (*end1>*st2)  {
                    //they intersect
                    return spatial_lib::IL_INTERSECT;
                } else {
                    st1 += 2;
                    end1 += 2;
                    cur1++;
                }
            // st2<st1
            } else {
                // overlap, end2>=st1 if intervals are [s,e] and end2>st1 if intervals are [s,e)
                if (*end2>*st1) {
                    //they intersect
                    return spatial_lib::IL_INTERSECT;
                } else {
                    st2 += 2;
                    end2 += 2;
                    cur2++;
                }
            }
        } while(cur1<=numintervals1 && cur2<=numintervals2);
        // guaranteed non-containment and no intersection at this point
        return spatial_lib::IL_DISJOINT;
    }

    /**
     * @brief compares two lists for whether ar1 is inside ar2, the reverse, they match, they intersect or they are disjoint
     */
    static inline int joinIntervalListsSymmetricalOptimizedTrueHitIntersect(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2) {
        // a list might be empty (disjoint)
        if(numintervals1 == 0 || numintervals2 == 0){
            return spatial_lib::IL_DISJOINT;
        }

        bool RinS = false;
        bool SinR = false;

        // is intervals R contained in intervals S?
        if (insideJoinIntervalLists(ar1, numintervals1, ar2, numintervals2)) {
            RinS = true;
        }
        // is intervals S contained in intervals R?
        if (insideJoinIntervalLists(ar2, numintervals2, ar1, numintervals1)) {
            SinR = true;
        }
        if (RinS && SinR) {
            // if both are contained, they match
            return spatial_lib::IL_MATCH;
        } else if(RinS) {
            return spatial_lib::IL_R_INSIDE_S;
        } else if(SinR) {
            return spatial_lib::IL_S_INSIDE_R;
        } else {
            return spatial_lib::IL_INTERSECT;
        }
    }

    namespace uncompressed
    {
        DB_STATUS intersectionJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result){
            DB_STATUS ret = DBERR_OK;
            // check ALL - ALL
            if(!intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            //check ALL - FULL
            if(intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                //hit
                result = spatial_lib::TRUE_HIT;
                return ret;
            }
            //check FULL - ALL
            if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //hit
                result = spatial_lib::TRUE_HIT;
                return ret;
            }
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }
        
        DB_STATUS insideCoveredByJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result){
            DB_STATUS ret = DBERR_OK;
            // check ALL - ALL
            if(!insideJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            //check ALL - FULL (todo: maybe check the opposite: find at least one interlval NOT inside, maybe its faster)
            if(insideJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                //hit
                result = spatial_lib::TRUE_HIT;
                return ret;
            }
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }

        DB_STATUS disjointJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result){
            DB_STATUS ret = DBERR_OK;
            // check ALL - ALL
            if(!intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_HIT;
                return ret;
            }
            //check ALL - FULL
            if(intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                //hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            //check FULL - ALL
            if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }

        DB_STATUS equalJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result){
            DB_STATUS ret = DBERR_OK;
            // check ALL - ALL
            if(!joinIntervalsForMatch(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            //check FULL - FULL
            if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                //hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }

        DB_STATUS meetJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result){
            DB_STATUS ret = DBERR_OK;
            // check ALL - ALL
            if(!intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            //check ALL - FULL
            if(intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                //hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            //check FULL - ALL
            if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsALL, aprilS->numIntervalsALL)){
                //hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }

        DB_STATUS containsCoversJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &result){
            DB_STATUS ret = DBERR_OK;
            // check ALL - ALL
            if(!insideJoinIntervalLists(aprilS->intervalsALL, aprilS->numIntervalsALL, aprilR->intervalsALL, aprilR->numIntervalsALL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_NEGATIVE;
                return ret;
            }

            // check FULL - ALL (todo: maybe check the opposite: find at least one interlval NOT inside, maybe its faster)
            if(insideJoinIntervalLists(aprilS->intervalsALL, aprilS->numIntervalsALL, aprilR->intervalsFULL, aprilR->numIntervalsFULL)){
                //guaranteed hit
                result = spatial_lib::TRUE_HIT;
                return ret;
            }
            
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }


        namespace topology
        {
            DB_STATUS MBRRinSContainmentJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation) {
                DB_STATUS ret = DBERR_OK;
                // join AA for containment, intersection or disjoint
                int AAresult = joinIntervalsHybrid(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsALL, aprilS->numIntervalsALL);
                // printf("AA: %d\n", AAresult);
                if (AAresult == spatial_lib::IL_DISJOINT) {
                    // true negative
                    relation = spatial_lib::TR_DISJOINT;
                    return ret;
                } else if(AAresult == spatial_lib::IL_R_INSIDE_S) {
                    // all R_A intervals are inside S_A 
                    if (aprilS->numIntervalsFULL) {
                        // join AF for containment, intersection or disjoint
                        int AFresult = joinIntervalsHybrid(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL);
                        if (AFresult == spatial_lib::IL_R_INSIDE_S) {
                            // AF containment
                            relation = spatial_lib::TR_INSIDE;
                            return ret;
                        } else if (AFresult == spatial_lib::IL_INTERSECT) {
                            // AF intersect, refinement for: inside, covered by, [true hit intersect]
                            relation = spatial_lib::REFINE_INSIDE_COVEREDBY_TRUEHIT_INTERSECT;
                            return ret;
                        }
                    } else {
                        if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsALL, aprilS->numIntervalsALL)) {
                            // intersection
                            relation = spatial_lib::TR_INTERSECT;
                            return ret;
                        }
                    }
                } else {
                    // intersection
                    if(intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                        // intersection
                        relation = spatial_lib::TR_INTERSECT;
                        return ret;
                    } else if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsALL, aprilS->numIntervalsALL)) {
                        // intersection
                        relation = spatial_lib::TR_INTERSECT;
                        return ret;
                    }
                }

                // refinement for: disjoint, inside, covered by, meet, intersect
                relation = spatial_lib::REFINE_DISJOINT_INSIDE_COVEREDBY_MEET_INTERSECT;
                return ret;
            }

            DB_STATUS MBRSinRContainmentJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation) {
                DB_STATUS ret = DBERR_OK;
                // join AA for containment, intersection or disjoint
                int AAresult = joinIntervalsHybrid(aprilS->intervalsALL, aprilS->numIntervalsALL, aprilR->intervalsALL, aprilR->numIntervalsALL);
                if (AAresult == spatial_lib::IL_DISJOINT) {
                    // true negative
                    relation = spatial_lib::TR_DISJOINT;
                    return ret;
                } else if(AAresult == spatial_lib::IL_R_INSIDE_S) {
                    // all S_A intervals are inside R_A 
                    if (aprilR->numIntervalsFULL) {
                        // join AF for containment, intersection or disjoint
                        int AFresult = joinIntervalsHybrid(aprilS->intervalsALL, aprilS->numIntervalsALL, aprilR->intervalsFULL, aprilR->numIntervalsFULL);
                        if (AFresult == spatial_lib::IL_R_INSIDE_S) {
                            // AF containment (dont confuse IL_R_INSIDE_S, it is based on how you pass arguments to the hybrid function)
                            relation = spatial_lib::TR_CONTAINS;
                            return ret;
                        } else if (AFresult == spatial_lib::IL_INTERSECT) {
                            // AF intersect, refinement for: contains, covers, [true hit intersect]
                            relation = spatial_lib::REFINE_CONTAINS_COVERS_TRUEHIT_INTERSECT;
                            return ret;
                        }
                    } else {
                        if(intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)) {
                            // check AF intersection
                            relation = spatial_lib::TR_INTERSECT;
                            return ret;
                        }
                    }
                } else {
                    // intersection
                    if(intersectionJoinIntervalLists(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL)){
                        // intersection
                        relation = spatial_lib::TR_INTERSECT;
                        return ret;
                    } else if(intersectionJoinIntervalLists(aprilR->intervalsFULL, aprilR->numIntervalsFULL, aprilS->intervalsALL, aprilS->numIntervalsALL)) {
                        // intersection
                        relation = spatial_lib::TR_INTERSECT;
                        return ret;
                    }
                }
                // refinement for: disjoint, inside, covered by, meet, intersect
                relation = spatial_lib::REFINE_DISJOINT_CONTAINS_COVERS_MEET_INTERSECT;
                return ret;
            }

            DB_STATUS MBREqualJoinAPRIL(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation) {
                DB_STATUS ret = DBERR_OK;
                // AA join to look for exact relationship between the lists
                int AAresult = joinIntervalListsSymmetricalOptimizedTrueHitIntersect(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsALL, aprilS->numIntervalsALL);
                if (AAresult == spatial_lib::IL_MATCH) {
                    // refine for equal, covered by, covers and true hit intersect
                    relation = spatial_lib::REFINE_EQUAL_COVERS_COVEREDBY_TRUEHIT_INTERSECT;
                    return ret;
                } else if (AAresult == spatial_lib::IL_R_INSIDE_S) {
                    int AFresult = joinIntervalsHybrid(aprilR->intervalsALL, aprilR->numIntervalsALL, aprilS->intervalsFULL, aprilS->numIntervalsFULL);
                    if (AFresult == spatial_lib::IL_R_INSIDE_S) {
                        // true hit covered by (return inside because reasons)
                        relation = spatial_lib::TR_INSIDE;
                        return ret;
                    } 
                    // AA no containment, AF disjoint or intersect
                    relation = spatial_lib::REFINE_COVEREDBY_TRUEHIT_INTERSECT;
                    return ret;
                } else if(AAresult == spatial_lib::IL_S_INSIDE_R) {
                    int FAresult = joinIntervalsHybrid(aprilS->intervalsALL, aprilS->numIntervalsALL, aprilR->intervalsFULL, aprilR->numIntervalsFULL);
                    // in this case R is S because joinIntervalsHybrid handles the first list as R and the second as S
                    // and only checks for assymetrical containment R in S
                    if (FAresult == spatial_lib::IL_R_INSIDE_S) {
                        // FA true hit covers, return contains
                        relation = spatial_lib::TR_CONTAINS;
                        return ret;
                    } 
                    // AA no containment, FA disjoint or intersect
                    relation = spatial_lib::REFINE_COVERS_TRUEHIT_INTERSECT;
                    return ret;
                } else {
                    // AA no containment, true hit intersect because equal MBRs
                    // maybe a special case of meet, so refine first
                    if(spatial_lib::isMeet(polR, polS)){
                        relation = spatial_lib::TR_MEET;
                        return ret;
                    }
                    relation = spatial_lib::TR_INTERSECT;
                    return ret;
                }
                return ret;
            }

            DB_STATUS MBRIntersectionJoinAPRIL(spatial_lib::AprilDataT *aprilR, spatial_lib::AprilDataT *aprilS, int &relation) {
                DB_STATUS ret = DBERR_OK;
                int aprilResult = -1;
                // use regular APRIL
                ret = intersectionJoinAPRIL(aprilR, aprilS, aprilResult);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "APRIL intersection join failed.");
                    return ret;
                }
                switch(aprilResult) {
                    case spatial_lib::TRUE_NEGATIVE:
                        relation = spatial_lib::TR_DISJOINT;
                        return ret;
                    case spatial_lib::TRUE_HIT:
                        relation = spatial_lib::TR_INTERSECT;
                        return ret;
                    case spatial_lib::INCONCLUSIVE:
                        relation = spatial_lib::REFINE_DISJOINT_MEET_INTERSECT;
                        return ret;
                    default:
                        logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "APRIL intersection join unexpected result:", aprilResult);
                        return DBERR_APRIL_UNEXPECTED_RESULT;
                }
                return ret;
            }
        }
    }

    
}