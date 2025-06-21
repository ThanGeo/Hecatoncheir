#ifndef D_TWOLAYER_FILTER_H
#define D_TWOLAYER_FILTER_H

#include "containers.h"
#include "APRIL/filter.h"
#include "../API/containers.h"
#include <omp.h>

/** @brief The two-layer MBR filter methods. */
namespace twolayer
{
    /** @brief Begins the query processing specified by the query object and stores the result in the query result object. */
    DB_STATUS processQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult);
}


#endif