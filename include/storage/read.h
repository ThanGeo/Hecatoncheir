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
             * @brief reads the next object from an opened partition file and returns the rasterizer polygon object, that will be used in the rasterizer library
             *  to create april
             */
            DB_STATUS readNextObjectForRasterization(FILE* pFile, int &recID, std::vector<int> &partitionIDs, rasterizerlib::polygon2d &rasterizerPolygon);

            /**
             * @brief reads the next object from an opened partition file and fills the polygon struct with the following information:
             * recID
             * MBR
             * partition IDs
             * @warning it does not load the actual geometry in memory
             */
            DB_STATUS readNextObjectMBR(FILE* pFile, spatial_lib::PolygonT &polygon);
        }


        /**
         * @brief loads a dataset's MBRs and APRIL approximations in-memory, to use in query processing.
         * Creates the local index based on the objects' partitions
         */
        DB_STATUS loadDatasetMBRandAPRIL(spatial_lib::DatasetT &dataset);
    }
}


#endif