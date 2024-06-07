#ifndef RASTERIZATION_H
#define RASTERIZATION_H

#include "def.h"
#include "utils.h"

#include <deque>

namespace rasterizerlib
{
    uint32_t** calculatePartialAndUncertain(polygon2d &polygon, uint32_t &orderN);
    uint32_t** calculatePartialAndUncertainBGPolygon(polygon2d &polygon, uint32_t &cellsPerDim);

    void rasterizationBegin(polygon2d &polygon);
    void rasterizationPartialOnlyBegin(polygon2d &polygon);
}


#endif