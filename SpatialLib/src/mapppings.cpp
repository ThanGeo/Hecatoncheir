#include "mappings.h"

namespace spatial_lib
{
    std::string queryTypeIntToText(int val){
        switch(val) {
            case spatial_lib::Q_INTERSECT: return "intersect";
            case spatial_lib::Q_INSIDE: return "inside";
            case spatial_lib::Q_DISJOINT: return "disjoint";
            case spatial_lib::Q_EQUAL: return "equal";
            case spatial_lib::Q_COVERED_BY: return "covered by";
            case spatial_lib::Q_COVERS: return "covers";
            case spatial_lib::Q_CONTAINS: return "contains";
            case spatial_lib::Q_MEET: return "meet";
            case spatial_lib::Q_FIND_RELATION: return "find relation";
        }
    }

    std::string dataTypeIntToText(DataTypeE val){
        switch(val) {
            case spatial_lib::DT_POLYGON: return "POLYGON";
            case spatial_lib::DT_POINT: return "POINT";
            case spatial_lib::DT_LINESTRING: return "LINESTRING";
        }
    }

    DataTypeE dataTypeTextToInt(std::string val){
        if (val.compare("POLYGON") == 0) return spatial_lib::DT_POLYGON;
        else if (val.compare("POINT") == 0) return spatial_lib::DT_POINT;
        else if (val.compare("LINESTRING") == 0) return spatial_lib::DT_LINESTRING;

        return DT_INVALID;
    }
}