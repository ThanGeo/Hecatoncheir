#ifndef D_APRIL_GENERATE_H
#define D_APRIL_GENERATE_H

#include "def.h"
#include "storage/read.h"
#include "APRIL/storage.h"

namespace APRIL
{
    namespace generation
    {
        typedef enum CellType {
            EMPTY,
            UNCERTAIN,
            PARTIAL,
            FULL,
        } CellTypeE;

        struct RasterData {
            uint32_t minCellX, minCellY, maxCellX, maxCellY;
            uint32_t bufferWidth, bufferHeight;
        }RasterDataT;

        DB_STATUS setRasterBounds(double xMin, double yMin, double xMax, double yMax);
    }

    DB_STATUS create(spatial_lib::DatasetT &dataset);
}

#endif