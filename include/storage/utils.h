#ifndef D_STORAGE_UTILS_H
#define D_STORAGE_UTILS_H

#include "SpatialLib.h"
#include "def.h"

namespace storage
{
    DB_STATUS generatePartitionFilePath(spatial_lib::DatasetT &dataset);
}


#endif