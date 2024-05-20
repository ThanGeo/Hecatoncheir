#include "data.h"

namespace spatial_lib 
{

    AprilDataT createEmptyAprilDataObject() {
        AprilDataT aprilData;
        aprilData.numIntervalsALL = 0;
        aprilData.numIntervalsFULL = 0;
        return aprilData;
    }
}