#include "data.h"

namespace spatial_lib 
{

    AprilDataT createEmptyAprilDataObject() {
        AprilDataT aprilData;
        aprilData.numIntervalsALL = 0;
        aprilData.numIntervalsFULL = 0;
        return aprilData;
    }

    void Polygon::printMBR() {
        printf("polygon((%f,%f),(%f,%f),(%f,%f),(%f,%f))\n", mbr.minPoint.x, mbr.minPoint.y, mbr.maxPoint.x, mbr.minPoint.y, mbr.maxPoint.x, mbr.maxPoint.y, mbr.minPoint.x, mbr.maxPoint.y);
    }
}