#ifndef D_STORAGE_WRITE_H
#define D_STORAGE_WRITE_H

#include "def.h"

namespace storage
{
    namespace writer
    {
        DB_STATUS appendBatchToDatasetPartitionFile(FILE* outFile, GeometryBatchT* batch, spatial_lib::DatasetT* dataset);

        DB_STATUS updateObjectCountInPartitionFile(FILE* outFile, int objectCount);
    }
}



#endif