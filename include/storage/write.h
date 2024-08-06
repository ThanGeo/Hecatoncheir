#ifndef D_STORAGE_WRITE_H
#define D_STORAGE_WRITE_H

#include "containers.h"

/** @brief Disk-related methods for writing data. */
namespace storage
{
    /** @brief Methods for writing data to the disk. */
    namespace writer
    {
        /** @brief Methods for the partitioned data writing. */
        namespace partitionFile
        {
            /** @brief Appends the dataset's info into an already opened partition file.
             * The file pointer must point to an already opened partition data file and its position should be at the begining, 
             * write after the object count (offset += sizeof(size_t)).
             */
            DB_STATUS appendDatasetInfoToPartitionFile(FILE* outFile, Dataset* dataset);
            
            /** @brief Appends a geometry batch to an already opened partition file.
             * The file pointer must point to an already opened partition data file and its position should be at the begining.
             */
            DB_STATUS appendBatchToPartitionFile(FILE* outFile, GeometryBatch* batch, Dataset* dataset);

            /**
            @brief Updates the first bytes (sizeof(size_t)) of a partition file with the new object count value.
             */
            DB_STATUS updateObjectCountInFile(FILE* outFile, size_t objectCount);
        }
    }
}



#endif