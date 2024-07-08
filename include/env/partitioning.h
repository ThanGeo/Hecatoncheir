#ifndef D_ENV_PARTITIONING_H
#define D_ENV_PARTITIONING_H

#include "def.h"
#include "comm.h"


namespace partitioning
{
    

    void printPartitionAssignment();

    DB_STATUS partitionDataset(spatial_lib::DatasetT *dataset);

    DB_STATUS calculateCSVDatasetDataspaceBounds(spatial_lib::DatasetT &dataset);

    /**
     * @brief returns a vector containing the IDs of the partitions that intersect with the given MBR
     * 
     * @param xMin 
     * @param yMin 
     * @param xMax 
     * @param yMax 
     * @param partitionIDs 
     * @return DB_STATUS 
     */
    DB_STATUS getPartitionsForMBR(double xMin, double yMin, double xMax, double yMax, std::vector<int> &partitionIDs, std::vector<spatial_lib::TwoLayerClassE> &twoLayerClasses);
}

#endif