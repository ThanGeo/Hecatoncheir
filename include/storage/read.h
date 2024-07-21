#ifndef D_STORAGE_READ_H
#define D_STORAGE_READ_H

#include "def.h"
#include "env/partitioning.h"

namespace storage
{
    namespace reader
    {
        namespace partitionFile
        {
            /**
             * @brief loads a dataset (vector and MBR) in-memory, to use in query processing.
             * Creates the local index based on the objects' partitions
             */
            DB_STATUS loadDatasetComplete(Dataset &dataset);

            /**
             * loads dataset info from the partition file. The pfile pos must be in the begining
             * loads: objectCount, dataset nickname, dataspace MBR
             */
            DB_STATUS loadDatasetInfo(FILE* pFile, Dataset &dataset);


            
            DB_STATUS loadNextObjectComplete(FILE* pFile, Shape &object);
        }
    }
}


#endif