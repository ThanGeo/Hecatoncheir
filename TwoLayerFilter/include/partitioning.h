#ifndef TWO_LAYER_PARTITIONING_H
#define TWO_LAYER_PARTITIONING_H

#include "relation.h"

namespace two_layer 
{
    namespace fs_2d{
        
        namespace single{
            namespace sort{
                
                namespace oneArray{
                    
                    void SortYStartOneArray(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size,  size_t *pRC_size, size_t *pSC_size,size_t *pRD_size, size_t *pSD_size, int runNumPartitions);
    
                };
            };
            

            void PartitionTwoDimensional(const Relation& R, const Relation& S, Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitionsPerRelation);
        };

        
    }
}

#endif
