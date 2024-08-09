#ifndef D_ENV_PARTITIONING_H
#define D_ENV_PARTITIONING_H

#include "containers.h"
#include "comm.h"

/** @brief Data distribution/partitioning methods. */
namespace partitioning
{
    /** @brief Prints all partitions and their assigned node ranks. */
    void printPartitionAssignment();

    /** @brief Initializes the partitioning for the input dataset.
     * @param[in] dataset Contains the dataset's information but not the contents, 
     * as the partitioning reads from disk and distributes the data.
     */
    DB_STATUS partitionDataset(Dataset *dataset);

    /** @brief Calculates the given dataset's metadata about its dataspace.
     * @param[in] dataset dataset Contains the dataset's information but not the contents.
     */
    DB_STATUS calculateCSVDatasetDataspaceBounds(Dataset &dataset);


    /** @brief Calculates all partition two-layer classes for a given batch, based on the partitioning method specified in the global configuration. */
    DB_STATUS calculateTwoLayerClasses(GeometryBatch &batch);
}

#endif