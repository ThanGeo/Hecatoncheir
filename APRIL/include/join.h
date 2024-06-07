#ifndef APRIL_JOIN_H
#define APRIL_JOIN_H

#include "SpatialLib.h"
#include <sys/types.h>

namespace APRIL
{
    /**
     * joins two interval lists for intersection
    */
    int intersectionJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);
    
    /**
     * joins two interval lists to detect symmetrical containment (R inside S, S inside R) and if there is at least
     * 1 intersection.
     * returns a spatial_lib::IntervalListsRelationshipE value indicating the lists' relationships
    */
    int joinIntervalListsSymmetrical(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);

    /**
     * joins two interval lists to detect symmetrical containment (R inside S, S inside R) and if there is at least
     * 1 intersection. 
     * Optimized way to run faster in code.: Doesn't iterate through the lists one keeping a bunch of flags. Instead, looks for specific
     * relations through multiple, simpler traversals. 
     * returns a spatial_lib::IntervalListsRelationshipE value indicating the lists' relationships
    */
    int joinIntervalListsSymmetricalOptimized(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);

    /**
     * joins two interval lists to detect symmetrical containment (R inside S, S inside R) while already knowing
     * that there is at least one intersection. 
     * Optimized way to run faster in code.: Doesn't iterate through the lists one keeping a bunch of flags. Instead, looks for specific
     * relations through multiple, simpler traversals. 
     * returns a spatial_lib::IntervalListsRelationshipE value indicating the lists' relationships
    */
    int joinIntervalListsSymmetricalOptimizedTrueHitIntersect(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);
    

    /**
     * joins two interval lists to detect if they match 100%
    */
    int joinIntervalsForMatch(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);

    /**
     * joins two interval lists for containment "R inside S" and at least 1 intersection.
     * returns a spatial_lib::IntervalListsRelationshipE value indicating the lists' relationships.
     * It is hybrid for optimization: after non-containment is found, continue looking for at least 1 intersection in a different way.
    */
    /**
     * @brief joins two interval lists for containment "R inside S" and at least 1 intersection.
     * Returns a spatial_lib::IntervalListsRelationshipE value indicating the lists' relationships.
     * It is hybrid for optimization: after non-containment is found, continue looking for at least 1 intersection in a different way.
     * 
     * @param ar1 intervals to be contained in
     * @param numintervals1 number of ar1 intervals
     * @param ar2 intervals to contain intervals of ar1
     * @param numintervals2 number of ar2 intervals
     * @return int: 1 true, 0 false
     */
    int joinIntervalsHybrid(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);


    /**
     * @brief Checks if all intervals of ar1 are completely contained in any intervals of ar2
     * 
     * @param ar1 intervals to be contained in
     * @param numintervals1 number of ar1 intervals
     * @param ar2 intervals to contain intervals of ar1
     * @param numintervals2 number of ar2 intervals
     * @return int: 1 true, 0 false
     */
    int insideJoinIntervalLists(std::vector<uint> &ar1, uint &numintervals1, std::vector<uint> &ar2, uint &numintervals2);


}

#endif