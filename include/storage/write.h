#ifndef D_STORAGE_WRITE_H
#define D_STORAGE_WRITE_H

#include "def.h"

namespace storage
{
    namespace writer
    {
        DB_STATUS appendDatasetInfoToPartitionFile(FILE* outFile, spatial_lib::DatasetT* dataset);
        DB_STATUS appendBatchToPartitionFile(FILE* outFile, GeometryBatchT* batch, spatial_lib::DatasetT* dataset);

        /**
         * updates the first entry (integer) of a binary file with the new objectCount value
         */
        DB_STATUS updateObjectCountInFile(FILE* outFile, int objectCount);
    }
}



#endif