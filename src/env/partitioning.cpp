#include "env/partitioning.h"

namespace partitioning
{
    void printPartitionAssignment() {
        for (int j=0; j<g_config.partitioningInfo.partitionsPerDimension; j++) {
            for (int i=0; i<g_config.partitioningInfo.partitionsPerDimension; i++) {
                printf("%d ", g_config.partitioningInfo.getNodeRankForCell(g_config.partitioningInfo.getCellID(i,j)));
            }
            printf("\n");
        }
    }
    
}