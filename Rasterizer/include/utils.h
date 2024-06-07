#ifndef UTILS_H
#define UTILS_H

#include "def.h"

namespace rasterizerlib
{
    extern std::vector<int> x_offset;
    extern std::vector<int> y_offset;

    uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y);
    void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y);

    bool binarySearchInIntervalVector(std::vector<uint32_t> &vec, uint32_t x);
    bool binarySearchInVector(std::vector<uint32_t> &vec, uint32_t &x);

    void mapPolygonToHilbert(polygon2d &pol, uint32_t cellsPerDim);
    void mapBoostPolygonToHilbert(polygon2d &pol, uint32_t cellsPerDim);
    

    std::vector<uint32_t> getPartialCellsFromMatrix(polygon2d &polygon, uint32_t **M);

    bool checkIfPolygonIsInsideDataspace(polygon2d &polygon);
}

#endif