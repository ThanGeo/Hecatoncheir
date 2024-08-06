#ifndef D_TWOLAYER_FILTER_H
#define D_TWOLAYER_FILTER_H

#include "containers.h"
#include "APRIL/filter.h"
#include <omp.h>

/** @brief The two-layer MBR filter methods. */
namespace twolayer
{
    /** @brief Begins the query processing based on the system's query configuration. */
    DB_STATUS processQuery(QueryOutput &queryOutput);
}


#endif