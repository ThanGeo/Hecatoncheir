#include "data.h"

namespace spatial_lib 
{

    


    void Polygon::printMBR() {
        printf("polygon((%f,%f),(%f,%f),(%f,%f),(%f,%f))\n", mbr.minPoint.x, mbr.minPoint.y, mbr.maxPoint.x, mbr.minPoint.y, mbr.maxPoint.x, mbr.maxPoint.y, mbr.minPoint.x, mbr.maxPoint.y);
    }
}