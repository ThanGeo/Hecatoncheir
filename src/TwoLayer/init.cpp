#include "TwoLayer/init.h"

namespace twolayer
{
    DB_STATUS setup(spatial_lib::DatasetT &dataset) {
        DB_STATUS ret = DBERR_OK;

        // normalize 
        dataset.normalizeMBRs();


        return ret;
    }
}