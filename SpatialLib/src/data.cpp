#include "data.h"

namespace spatial_lib 
{

    template<typename T>
    void Shape<T>::print() const {
        if constexpr (std::is_same_v<T, bg_polygon>) {
            printf("Polygon %d: \n", recID);
            for (const auto& point : boostObject.outer()) {
                printf("(%f,%f),", boost::geometry::get<0>(point), boost::geometry::get<1>(point));
            }
        } else if constexpr (std::is_same_v<T, bg_linestring>) {
            printf("Linestring %d: \n", recID);
            for (const auto& point : boostObject) {
                printf("(%f,%f),", boost::geometry::get<0>(point), boost::geometry::get<1>(point));
            }
        } else if constexpr (std::is_same_v<T, bg_rectangle>) {
            printf("Rectangle %d: \n", recID);
            printf("(%f,%f),(%f,%f)", boost::geometry::get<0>(boostObject.min_corner()), boost::geometry::get<1>(boostObject.min_corner()), boost::geometry::get<0>(boostObject.max_corner()), boost::geometry::get<1>(boostObject.max_corner()));
        }
        printf("\n");
    }

    template<typename T>
    DataTypeE Shape<T>::getDataType() {
        if constexpr (std::is_same_v<T, bg_polygon>) {
            return DT_POLYGON;
        } else if constexpr (std::is_same_v<T, bg_linestring>) {
            return DT_LINESTRING;
        } else if constexpr (std::is_same_v<T, bg_rectangle>) {
            return DT_RECTANGLE;
        } else if constexpr (std::is_same_v<T, bg_point_xy>) {
            return DT_POINT;
        } else {
            return DT_INVALID;
        }
    }

    void Polygon::printMBR() {
        printf("polygon((%f,%f),(%f,%f),(%f,%f),(%f,%f))\n", mbr.minPoint.x, mbr.minPoint.y, mbr.maxPoint.x, mbr.minPoint.y, mbr.maxPoint.x, mbr.maxPoint.y, mbr.minPoint.x, mbr.maxPoint.y);
    }
}