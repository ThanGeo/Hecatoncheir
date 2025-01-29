#ifndef D_STORAGE_READ_H
#define D_STORAGE_READ_H

#include "containers.h"
#include "env/partitioning.h"

/** @brief Disk-related methods for storing system metadata and data. */
namespace storage
{
    /** @brief Methods for loading data from disk. */
    namespace reader
    {
        /** @brief Uses mmap to count the total lines in a file (regardless of file type). */
        DB_STATUS getDatasetLineCount(Dataset* dataset, size_t &totalLines);


        /** @brief Counts lines using ubuntu system call */
        DB_STATUS getDatasetLineCountWithSystemCall(Dataset* dataset, size_t &totalLines);

        /** @brief Calculate the metadata for the input dataset. 
         * The dataset's path, file type and data type must be set already in the object. */
        DB_STATUS calculateDatasetMetadata(Dataset* dataset);

        /** @brief Methods for the partitioned data loading. */
        namespace partitionFile
        {
            /**
            @brief Loads a dataset (both vector data and MBRs) in-memory, to use in query processing.
             * Creates the local index based on the objects' partitions.
             */
            DB_STATUS loadDatasetComplete(Dataset *dataset);

            /**
            @brief Loads the dataset metadata from the partition file. The file pointer must be opened to the file and 
             * its position must be in the begining of the file.
             */
            DB_STATUS loadDatasetMetadata(FILE* pFile, Dataset *dataset);

            /**
            @brief Loads the next object in its entirety from an already opened file.
             * @param[in] pFile A file pointer pointing to an opened partition file.
             * @param[out] object Where the object's contents will be stored.
             */
            DB_STATUS loadNextObjectComplete(FILE* pFile, Shape &object);
        }
    }
}


#endif