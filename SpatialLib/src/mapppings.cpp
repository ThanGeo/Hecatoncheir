#include "mappings.h"

namespace spatial_lib
{
    namespace mapping
    {
        std::string queryTypeIntToStr(int val){
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
                default: return "";
            }
        }

        int queryTypeStrToInt(std::string &str) {
            if (str == "range") {
                return spatial_lib::Q_RANGE;
            } else if (str == "intersect") {
                return spatial_lib::Q_INTERSECT;
            } else if (str == "inside") {
                return spatial_lib::Q_INSIDE;
            } else if (str == "disjoint") {
                return spatial_lib::Q_DISJOINT;
            } else if (str == "equal") {
                return spatial_lib::Q_EQUAL;
            } else if (str == "meet") {
                return spatial_lib::Q_MEET;
            } else if (str == "contains") {
                return spatial_lib::Q_CONTAINS;
            } else if (str == "covered_by") {
                return spatial_lib::Q_COVERED_BY;
            } else if (str == "covers") {
                return spatial_lib::Q_COVERS;
            } else if (str == "find_relation") {
                return spatial_lib::Q_FIND_RELATION;
            } else {
                return -1;
            }
        }

        std::string dataTypeIntToStr(DataTypeE val){
            switch(val) {
                case spatial_lib::DT_POLYGON: return "POLYGON";
                case spatial_lib::DT_RECTANGLE: return "RECTANGLE";
                case spatial_lib::DT_POINT: return "POINT";
                case spatial_lib::DT_LINESTRING: return "LINESTRING";
                default: return "";
            }
        }

        DataTypeE dataTypeTextToInt(std::string str){
            if (str.compare("POLYGON") == 0) return spatial_lib::DT_POLYGON;
            else if (str.compare("RECTANGLE") == 0) return spatial_lib::DT_RECTANGLE;
            else if (str.compare("POINT") == 0) return spatial_lib::DT_POINT;
            else if (str.compare("LINESTRING") == 0) return spatial_lib::DT_LINESTRING;

            return DT_INVALID;
        }

        FileTypeE fileTypeTextToInt(std::string str) {
            if (str.compare("BINARY") == 0) return spatial_lib::FT_BINARY;
            else if (str.compare("CSV") == 0) return spatial_lib::FT_CSV;
            else if (str.compare("WKT") == 0) return spatial_lib::FT_WKT;

            return FT_INVALID;
        }

        std::string datatypeCombinationIntToStr(DatatypeCombinationE val) {
            switch(val) {
                case spatial_lib::DC_POINT_POINT: return "POINT-POINT";
                case spatial_lib::DC_POINT_LINESTRING: return "POINT-LINESTRING";
                case spatial_lib::DC_POINT_RECTANGLE: return "POINT-RECTANGLE";
                case spatial_lib::DC_POINT_POLYGON: return "POINT-POLYGON";
                
                case spatial_lib::DC_LINESTRING_POINT: return "LINESTRING-POINT";
                case spatial_lib::DC_LINESTRING_LINESTRING: return "LINESTRING-LINESTRING";
                case spatial_lib::DC_LINESTRING_RECTANGLE: return "LINESTRING-RECTANGLE";
                case spatial_lib::DC_LINESTRING_POLYGON: return "LINESTRING-POLYGON";

                case spatial_lib::DC_RECTANGLE_POINT: return "RECTANGLE-POINT";
                case spatial_lib::DC_RECTANGLE_LINESTRING: return "RECTANGLE-LINESTRING";
                case spatial_lib::DC_RECTANGLE_RECTANGLE: return "RECTANGLE-RECTANGLE";
                case spatial_lib::DC_RECTANGLE_POLYGON: return "RECTANGLE-POLYGON";

                case spatial_lib::DC_POLYGON_POINT: return "POLYGON-POINT";
                case spatial_lib::DC_POLYGON_LINESTRING: return "POLYGON-LINESTRING";
                case spatial_lib::DC_POLYGON_RECTANGLE: return "POLYGON-RECTANGLE";
                case spatial_lib::DC_POLYGON_POLYGON: return "POLYGON-POLYGON";
                default: return "";
            }
        }

        std::string relationIntToStr(int relation) {
            switch(relation) {
                case TR_INTERSECT: return "INTERSECT";
                case TR_CONTAINS: return "CONTAINS";
                case TR_DISJOINT: return "DISJOINT";
                case TR_EQUAL: return "EQUAL";
                case TR_COVERS: return "COVERS";
                case TR_MEET: return "MEET";
                case TR_COVERED_BY: return "COVERED BY";
                case TR_INSIDE: return "INSIDE";
                default: return "";
            }
        }
    }
}