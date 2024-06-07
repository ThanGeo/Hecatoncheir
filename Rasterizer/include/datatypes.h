#ifndef DATA_TYPES_H
#define DATA_TYPES_H

#include <vector>
#include <stdint.h>

#include <boost/geometry.hpp>
#include <boost/geometry/algorithms/assign.hpp>
#include <boost/foreach.hpp>
#include <boost/assign.hpp>

namespace rasterizerlib
{
    // boost geometry
    typedef boost::geometry::model::d2::point_xy<double> bg_point_xy;
    typedef boost::geometry::model::d2::point_xy<uint32_t> bg_point_xy_uint;
    typedef boost::geometry::model::linestring<bg_point_xy> bg_linestring;
    typedef boost::geometry::model::linestring<bg_point_xy_uint> bg_linestring_uint;
    typedef boost::geometry::model::polygon<boost::geometry::model::d2::point_xy<double> > bg_polygon;

    enum rasterDataTypeE {
        RD_CELL,
        RD_INTERVAL
    };

    struct raster_data {
        uint32_t minCellX, minCellY, maxCellX, maxCellY;
        uint32_t bufferWidth, bufferHeight;
        rasterDataTypeE type;
    };

    struct polygon2d {
        std::vector<spatial_lib::PointT> vertices;
        spatial_lib::MbrT mbr;
        bg_polygon bgPolygon;
        raster_data rasterData;
    };

    typedef enum CellType {
        EMPTY,
        UNCERTAIN,
        PARTIAL,
        FULL,
    } CellTypeE;

}

#endif