#include "join.h"

namespace APRIL
{
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

            if (*end1<=*end2)
            {
                if(!intervalRcontained){
                    //we are skipping this interval because it was never contained, so return false (not inside)
                    return 0;
                }
                st1 += 2;
                end1 += 2;
                cur1++;
                intervalRcontained = false;
            }
            else 
            {
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

    int intersectionJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
        // printf("%u and %u intervals\n", numintervals1, numintervals2);
        //they may not have any intervals of this type
        if(numintervals1 == 0 || numintervals2 == 0){
            return 0;
        }
        
        // st1,st2,end1,end2;
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
            if (*st1<=*st2)
            {
                if (*end1>*st2) // overlap, end1>=st2 if intervals are [s,e] and end1>st2 if intervals are [s,e)
                {
                    //they overlap, return 1
                    return 1;
                }	
                else
                {
                    st1 += 2;
                    end1 += 2;
                    cur1++;
                }
            }
            else // st2<st1
            {
                if (*end2>*st1) // overlap, end2>=st1 if intervals are [s,e] and end2>st1 if intervals are [s,e)
                {

                    //they overlap, return 1
                    return 1;
                }
                else
                {
                    st2 += 2;
                    end2 += 2;
                    cur2++;
                }
            }
        } while(cur1<=numintervals1 && cur2<=numintervals2);
        
        //no overlap, return 0
        return 0;
    }


    int joinIntervalListsSymmetrical(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2) {
        bool intersect = false;
        // a list might be empty (disjoint)
        if(numintervals1 == 0 || numintervals2 == 0){
            return spatial_lib::IL_DISJOINT;
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

        bool RinS = true;
        bool SinR = true;

        bool currentRintervalIsContained = false;
        bool currentSintervalIsContained = false;

        do {
            // printf("Intervals:\n");
            // printf("R[%u]: [%u,%u), contained? %d\n", cur1, *st1, *end1, currentRintervalIsContained);
            // printf("S[%u]: [%u,%u), contained? %d\n", cur2, *st2, *end2, currentSintervalIsContained);

            if (*st1 >= *st2) {
                if (*end1 <= *end2) {
                    // R is contained
                    currentRintervalIsContained = true;
                    intersect = true;
                } 
                // else {
                //     if (currentRintervalIsContained){
                //         // R not in S
                //         RinS = false;
                //     }
                // }
                if (*end2 > *st1) {
                    // intersection
                    intersect = true;
                }
                
            } else {
                if (!currentRintervalIsContained && cur2 >= numintervals2){
                    // R not in S
                    RinS = false;
                }
            }
            if (*st2 >= *st1) {
                if (*end2 <= *end1) {
                    // S is contained
                    // printf("    interval of S contained\n");
                    currentSintervalIsContained = true;
                    intersect = true;
                } 
                // else {
                //     if (currentSintervalIsContained){
                //         // S not in R
                //         SinR = false;

                //         printf("    SinR set to false!\n");
                //     }
                // }
                if (*end1 > *st2) {
                    // intersection
                    intersect = true;
                }
            } else {
                if (!currentSintervalIsContained && cur1 >= numintervals1){
                    // S not in R
                    SinR = false;
                }
            }

            if (!RinS && !SinR && intersect) {
                // early break
                break;
            }

            if (*end1 < *end2) {
                if (!currentRintervalIsContained) {
                    // we skipped an interval without finding containment for it
                    RinS = false;
                }
                st1 += 2;
                end1 += 2;
                cur1++;
                if (cur1 <= numintervals1) {
                    currentRintervalIsContained = false;
                } else {
                    break;
                }
            } else {
                if (!currentSintervalIsContained) {
                    // we skipped an interval without finding containment for it
                    SinR = false;
                }
                st2 += 2;
                end2 += 2;
                cur2++;
                if (cur2 <= numintervals2) {
                    currentSintervalIsContained = false;
                } else {
                    break;
                }
            }

        } while(cur1<=numintervals1 && cur2<=numintervals2);

        // symmetrical containment
        if (RinS && SinR) {
            if(cur1 >= numintervals1 && cur2 >= numintervals2 && currentRintervalIsContained && currentSintervalIsContained) {
                return spatial_lib::IL_MATCH;
            }
        }
        // R in S
        if (cur1 >= numintervals1) {
            if (RinS && currentRintervalIsContained) {
                return spatial_lib::IL_R_INSIDE_S;
            }
        }
        // S in R
        if (cur2 >= numintervals2) {
            if (SinR && currentSintervalIsContained) {
                return spatial_lib::IL_S_INSIDE_R;
            }
        }
        // no containment
        if(intersect) {
            // intersect
            return spatial_lib::IL_INTERSECT;
        }
        // no intersect
        return spatial_lib::IL_DISJOINT;

    }

    int joinIntervalListsSymmetricalOptimized(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2) {
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
            if(intersectionJoinIntervalLists(ar1, numintervals1, ar2, numintervals2)) {
                return spatial_lib::IL_INTERSECT;
            } else {
                return spatial_lib::IL_DISJOINT;
            }
        }
    }

    int joinIntervalListsSymmetricalOptimizedTrueHitIntersect(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2) {
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

    int joinIntervalsHybrid(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2){
        bool intersect = false;
        //they may not have any intervals of this type
        if(numintervals1 == 0 || numintervals2 == 0){
            return spatial_lib::IL_DISJOINT;
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

        // printf("%d and %d ALL and FULL intervals respectively\n", numintervals1, numintervals2);

        do {
            // printf("A[%u,%u) and F[%u,%u)\n", *st1, *end1, *st2, *end2);
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

            if (*end1<=*end2)
            {
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
            }
            else 
            {
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
    // after this point it is guaranteed NO containment. So after 1 intersection is found, return
        do {
            if (*st1<=*st2)
            {
                if (*end1>*st2) // overlap, end1>=st2 if intervals are [s,e] and end1>st2 if intervals are [s,e)
                {
                    //they intersect
                    return spatial_lib::IL_INTERSECT;
                    break;
                }	
                else
                {
                    st1 += 2;
                    end1 += 2;
                    cur1++;
                }
            }
            else // st2<st1
            {
                if (*end2>*st1) // overlap, end2>=st1 if intervals are [s,e] and end2>st1 if intervals are [s,e)
                {

                    //they intersect
                    return spatial_lib::IL_INTERSECT;
                    break;
                }
                else
                {
                    st2 += 2;
                    end2 += 2;
                    cur2++;
                }
            }
        } while(cur1<=numintervals1 && cur2<=numintervals2);

        // guaranteed non-containment and no intersection at this point
        return spatial_lib::IL_DISJOINT;
    }
}