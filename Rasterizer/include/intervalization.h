#ifndef INTERVALIZATION_H
#define INTERVALIZATION_H

#include "def.h"
#include "utils.h"
#include "rasterization.h"

namespace rasterizerlib
{
    spatial_lib::AprilDataT intervalizationBegin(polygon2d &polygon);
    spatial_lib::AprilDataT intervalizationBGPolygon(spatial_lib::bg_polygon &bg_polygon);
}

#endif