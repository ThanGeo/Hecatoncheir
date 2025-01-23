#ifndef D_APRIL_GENERATE_H
#define D_APRIL_GENERATE_H

#include "containers.h"

#include "storage/read.h"
#include "APRIL/storage.h"

/** @brief APRIL related methods. */
namespace APRIL
{
    /** @brief APRIL generation related methods. */
    namespace generation
    {
        extern std::vector<int> x_offset;
        extern std::vector<int> y_offset;

        /** @enum CellType @brief APRIL cell types. */
        typedef enum CellType {
            EMPTY,
            UNCERTAIN,
            PARTIAL,
            FULL,
        } CellType;

        /** @brief APRIL raster data. */
        struct RasterData {
            uint32_t minCellX, minCellY, maxCellX, maxCellY;
            uint32_t bufferWidth, bufferHeight;
        };

        /** @brief Sets the raster bounds for the intervalization space of APRIL */
        DB_STATUS setRasterBounds(DataspaceMetadata &dataspaceMetadata);
        
        /** @brief Disk-based APRIL generation methods, for very large datasets or when there's limited memory. */
        namespace disk
        {
            /**
            @brief Generates the APRIL approximations for the given dataset.
             * @note It loads each polygon one by one in memory and creates its APRIL approximation. Used when there's limited memory.
             */
            extern DB_STATUS init(Dataset &dataset);
        }
        
        /** @brief In-memory APRIL generation methods. Loads the entire dataset in memory and then generates APRIL. */
        namespace memory
        {
            /** @brief creates the APRIL for an object */
            extern DB_STATUS createAPRILforObject(Shape* shape, DataType dataType, AprilConfig &aprilConfig, AprilData &aprilData);
        }
    }
}

#endif