#ifndef D_STORAGE_WRITE_H
#define D_STORAGE_WRITE_H

#include "containers.h"

namespace storage
{
    namespace writer
    {
        DB_STATUS appendDatasetInfoToPartitionFile(FILE* outFile, Dataset* dataset);
        DB_STATUS appendBatchToPartitionFile(FILE* outFile, GeometryBatch* batch, Dataset* dataset);

        /**
         * updates the first entry (integer) of a binary file with the new objectCount value
         */
        DB_STATUS updateObjectCountInFile(FILE* outFile, size_t objectCount);
    }
}



#endif