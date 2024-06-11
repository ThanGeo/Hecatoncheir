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
            DB_STATUS readNextObjectForRasterization(FILE* pFile, int &recID, std::vector<int> &partitionIDs, rasterizerlib::polygon2d &rasterizerPolygon);
        }
    }
}


#endif