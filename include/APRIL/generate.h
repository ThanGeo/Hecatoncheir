#ifndef D_APRIL_GENERATE_H
#define D_APRIL_GENERATE_H

#include "def.h"
#include "Rasterizer.h"
#include "storage/read.h"
#include "APRIL/storage.h"

namespace APRIL
{
    DB_STATUS generate(spatial_lib::DatasetT &dataset);
}

#endif