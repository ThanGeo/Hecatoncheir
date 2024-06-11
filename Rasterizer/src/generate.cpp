#include "generate.h"


namespace rasterizerlib
{
    

    spatial_lib::AprilDataT generate(polygon2d polygon, GenerateTypeE generateType) {
        // safety checks
        if (!g_config.lib_init) {
            spatial_lib::AprilDataT aprilData;
            log_err("lib not initialized");
            return aprilData;
        }
        // if(!checkIfPolygonIsInsideDataspace(polygon)){
        //     log_err("Polygon is not copletely inside the pre-defined data space");
        //     return 0;
        // }

        // choose based on config and generate type
        if (generateType >= GT_APRIL_BEGIN && generateType < GT_APRIL_END) {
            // APRIL
            switch (generateType) {
                case GT_APRIL:
                    // complete APRIL
                    return intervalizationBegin(polygon);
                    break;
            }
        } else if (generateType >= GT_RASTER_BEGIN && generateType < GT_RASTER_END) {
            // RASTER
            switch (generateType) {
                case GT_RASTER:
                    // complete raster 
                    rasterizationBegin(polygon);
                    break;
                case GT_RASTER_PARTIAL_ONLY:
                    // only partial cell rasterization
                    rasterizationPartialOnlyBegin(polygon);
                    break;
            }
        } else {
            // unknown generate type
            log_err("Unknown generate type.");
            spatial_lib::AprilDataT aprilData;
            return aprilData;
        }
    }


    spatial_lib::AprilDataT generateAPRILForBoostGeometry(spatial_lib::bg_polygon &bg_polygon) {
        // complete APRIL
        return intervalizationBGPolygon(bg_polygon);
    }



}