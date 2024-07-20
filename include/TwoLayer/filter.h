#ifndef D_TWOLAYER_FILTER_H
#define D_TWOLAYER_FILTER_H

#include "def.h"
#include "APRIL/filter.h"
#include <omp.h>

namespace twolayer
{
    DB_STATUS processQuery(QueryOutputT &queryOutput);
}


#endif