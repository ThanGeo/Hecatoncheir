#ifndef D_APRIL_GENERATE_H
#define D_APRIL_GENERATE_H

#include "def.h"

#include "storage/read.h"
#include "APRIL/storage.h"

namespace APRIL
{
    namespace generation
    {
        extern double rasterXmin;
        extern double rasterYmin;
        extern double rasterXmax;
        extern double rasterYmax;
        extern std::vector<int> x_offset;
        extern std::vector<int> y_offset;


        typedef enum CellType {
            EMPTY,
            UNCERTAIN,
            PARTIAL,
            FULL,
        } CellTypeE;

        struct RasterData {
            uint32_t minCellX, minCellY, maxCellX, maxCellY;
            uint32_t bufferWidth, bufferHeight;
        };
        namespace disk
        {
            /**
             * @brief generates all APRIL approximations for the given dataset.
             * @note It loads each polygon one by one and creates its APRIL approximation
             */
            extern DB_STATUS init(Dataset &dataset);
        }
        
        namespace memory
        {
            /**
             * @brief generates all APRIL approximations for the given dataset.
             * @warning requires all objects to be stored already in memory (in the dataset)
             */
            extern DB_STATUS init(Dataset &dataset);
        }
    }
}

#endif