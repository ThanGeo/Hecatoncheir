#ifndef DEF_H
#define DEF_H

#include "SpatialLib.h"
#include "datatypes.h"

#include <cmath>
#include <cstdio>
#include <string>
#include <algorithm>
#include <limits>

namespace rasterizerlib
{
    typedef enum CellEnumerationType{
        CE_HILBERT,
    }CellEnumerationTypeE;

    typedef enum GenerateType{
        GT_APRIL_BEGIN = 0,
        GT_APRIL = GT_APRIL_BEGIN,
        GT_APRIL_END,
        GT_RASTER_BEGIN = 20,
        GT_RASTER = GT_RASTER_BEGIN,
        GT_RASTER_PARTIAL_ONLY,
        GT_RASTER_END
    }GenerateTypeE;

    typedef struct config
    {
        // init
        bool lib_init = false;

        // dataspace related
        double xMin, yMin, xMax, yMax;

        // rasterization related
        CellEnumerationTypeE celEnumType = CE_HILBERT;
        bool isGridSymmetrical = true;      // number of cells on X and Y axis is the same
        bool areCellsSquare;                // the cells of the grid are square (instead of rectangle)

        // for hilbert
        uint32_t orderN = 16;
        uint32_t cellsPerDim = pow(2,16);

    }configT;

    extern configT g_config;
    
    int setDataspace(double xMin, double yMin, double xMax, double yMax);
    
    polygon2d createPolygon(std::vector<spatial_lib::PointT> &vertices);
    polygon2d createPolygon(double* coords, int vertexCount);

    int init(double xMin, double yMin, double xMax, double yMax);

    void log_err(std::string errorText);

    
    void printConfig();
}



#endif