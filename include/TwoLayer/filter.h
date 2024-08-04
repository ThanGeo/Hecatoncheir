#ifndef D_TWOLAYER_FILTER_H
#define D_TWOLAYER_FILTER_H

#include "containers.h"
#include "APRIL/filter.h"
#include <omp.h>

namespace twolayer
{
    DB_STATUS processQuery(QueryOutput &queryOutput);
}


#endif