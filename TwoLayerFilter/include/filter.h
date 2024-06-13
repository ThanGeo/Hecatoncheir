#ifndef TWO_LAYER_FILTER_H
#define TWO_LAYER_FILTER_H

#include "relation.h"
#include "SpatialLib.h"
#include "APRIL.h"

namespace two_layer
{
    namespace standard
    {
        /**
         * @brief Performs the Two Layer standard spatial intersection join MBR evaluation filter
         */
        unsigned long long ForwardScanBased_PlaneSweep_CNT_Y_Less(Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartition);
    }

    /**
     * @brief performs an specialized version of two-layer mbr filter for find relation queries
     * 
     */
    namespace optimized
    {
        namespace relate
        {
            unsigned long long MBRFilter(Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartition);
        }
        namespace find_relation
        {
            unsigned long long MBRFilter(Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartition);
        }
    }

    namespace optimized_scalability
    {
        unsigned long long MBRFilter(Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartition);
    }
}

#endif