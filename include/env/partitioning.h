#ifndef D_ENV_PARTITIONING_H
#define D_ENV_PARTITIONING_H

#include "containers.h"
#include "comm.h"

/** @brief Data distribution/partitioning methods. */
namespace partitioning
{
    /** @brief Prints all partitions and their assigned node ranks. */
    void printPartitionAssignment();

    namespace csv 
    {
         /** @brief Calculates the given CSV dataset's metadata with no pre-existing metadata knowledege about it.
         * @param[in] dataset Empty object that contains only dataset's path but zero other content.
         */
        DB_STATUS calculateDatasetMetadata(Dataset* dataset);
    }

    namespace wkt
    {
         /** @brief Calculates the given WKT dataset's metadata with no pre-existing metadata knowledege about it.
         * @param[in] dataset Empty object that contains only dataset's path but zero other content.
         */
        DB_STATUS calculateDatasetMetadata(Dataset* dataset);
    }

    /** @brief Initializes the partitioning for the input dataset.
     * @param[in] dataset Contains the dataset's information but not the contents, 
     * as the partitioning reads from disk and distributes the data.
     */
    DB_STATUS partitionDataset(Dataset *dataset);

    /** @brief Calculates all partition two-layer classes for a given batch, based on the partitioning method specified in the global configuration. */
    DB_STATUS calculateTwoLayerClasses(Batch &batch);
}

#endif