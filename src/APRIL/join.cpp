#include "APRIL/join.h"

namespace APRIL
{
    /**
     * @brief returns 1 if the two interval lists have at least one overlap, otherwise zero
     * if any of them is empty, they can not overlap
     */
    static int intersectionJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
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
    int insideJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
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
    int joinIntervalsForMatch(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
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
            //check ALL - FULL
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

            // check FULL - ALL
            if(!insideJoinIntervalLists(aprilS->intervalsALL, aprilS->numIntervalsALL, aprilR->intervalsFULL, aprilR->numIntervalsFULL)){
                //guaranteed not hit
                result = spatial_lib::TRUE_HIT;
                return ret;
            }
            
            // mark for refinement (inconclusive)
            result = spatial_lib::INCONCLUSIVE;
            return ret;
        }

    }

    
}