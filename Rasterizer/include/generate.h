#ifndef GENERATE_H
#define GENERATE_H

#include "def.h"
#include "rasterization.h"
#include "intervalization.h"

namespace rasterizerlib
{
    spatial_lib::AprilDataT generate(polygon2d polygon, GenerateTypeE generateType);

    spatial_lib::AprilDataT generateAPRILForBoostGeometry(spatial_lib::bg_polygon &polygon);
}

#endif