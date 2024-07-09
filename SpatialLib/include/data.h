#ifndef SPATIAL_LIB_DATA_H
#define SPATIAL_LIB_DATA_H

#include <boost/geometry.hpp>
#include <boost/geometry/algorithms/assign.hpp>
#include <boost/foreach.hpp>
#include <boost/assign.hpp>

namespace spatial_lib
{
    // boost geometry
    typedef boost::geometry::model::d2::point_xy<double> bg_point_xy;
    typedef boost::geometry::model::linestring<bg_point_xy> bg_linestring;
    typedef boost::geometry::model::box<bg_point_xy> bg_rectangle;
    typedef boost::geometry::model::polygon<bg_point_xy> bg_polygon;

    // MBR topology relations
    typedef enum MBRRelationCase {
        MBR_R_IN_S,
        MBR_S_IN_R,
        MBR_EQUAL,
        MBR_CROSS,
        MBR_INTERSECT,
    } MBRRelationCaseE;
    
    // topological
    typedef enum TopologyRelation {
        TR_DISJOINT,
        TR_EQUAL,
        TR_INSIDE,
        TR_CONTAINS,
        TR_MEET,
        TR_COVERS,
        TR_COVERED_BY,
        TR_INTERSECT,
        // specific refinement cases
        REFINE_CONTAINS_PLUS = 1000,
        REFINE_INSIDE_PLUS,
        REFINE_CONTAINMENT_ONLY,
        REFINE_NO_CONTAINMENT,
        REFINE_ALL_NO_EQUAL,
        REFINE_ALL,
        /**
         * @brief SPECIALIZED MBR TOPOLOGY
         */
        // MBR(r) inside MBR(s)
        REFINE_INSIDE_COVEREDBY_TRUEHIT_INTERSECT = 2000,
        REFINE_DISJOINT_INSIDE_COVEREDBY_MEET_INTERSECT,
        // MBR(s) inside MBR(r)
        REFINE_CONTAINS_COVERS_TRUEHIT_INTERSECT,
        REFINE_DISJOINT_CONTAINS_COVERS_MEET_INTERSECT,
        // MBR(r) intersects MBR(s)
        REFINE_DISJOINT_MEET_INTERSECT,
        // MBR(r) = MBR(s)
        REFINE_COVEREDBY_TRUEHIT_INTERSECT,
        REFINE_COVERS_TRUEHIT_INTERSECT,
        REFINE_COVERS_COVEREDBY_TRUEHIT_INTERSECT,
        REFINE_EQUAL_COVERS_COVEREDBY_TRUEHIT_INTERSECT,
    } TopologyRelationE;

    // spatial data types
    typedef enum DataType{
        DT_INVALID,
        DT_POINT,
        DT_LINESTRING,
        DT_RECTANGLE,
        DT_POLYGON,
    } DataTypeE;

    // query data type combinations
    typedef enum DatatypeCombination {
        // invalid
        DC_INVALID_COMBINATION,
        // heterogenous
        DC_POINT_LINESTRING,
        DC_POINT_RECTANGLE,
        DC_POINT_POLYGON,
        DC_RECTANGLE_POINT,
        DC_RECTANGLE_LINESTRING,
        DC_RECTANGLE_POLYGON,
        DC_LINESTRING_POINT,
        DC_LINESTRING_RECTANGLE,
        DC_LINESTRING_POLYGON,
        DC_POLYGON_POINT,
        DC_POLYGON_LINESTRING,
        DC_POLYGON_RECTANGLE,
        //homogenous
        DC_POINT_POINT,
        DC_LINESTRING_LINESTRING,
        DC_RECTANGLE_RECTANGLE,
        DC_POLYGON_POLYGON,
    }DatatypeCombinationE;


    // spatial approximations
    typedef enum ApproximationType{
        AT_NONE,
        // mine
        AT_APRIL,
        AT_RI,
        // competitors
        AT_5CCH,
        AT_RA,
        AT_GEOS,
    } ApproximationTypeE;

    typedef enum IntermediateFilterType{
        IF_NONE,
        IF_MARK_APRIL_BEGIN,
        IF_APRIL_STANDARD = IF_MARK_APRIL_BEGIN,
        IF_APRIL_OPTIMIZED,
        IF_APRIL_OTF,
        IF_APRIL_SCALABILITY,
        IF_MARK_APRIL_END
    } IntermediateFilterTypeE;

    typedef enum MBRFilterType {
        MF_INTERSECTION,
        MF_TOPOLOGY,
    } MBRFilterTypeE;

    /*************************************
     * 
     * 
     * 
     * 
     *              APRIL
     * 
     * 
     * 
     * 
     *********************************** */

    // APRIL data
    typedef struct AprilData {
        // APRIL data
        uint numIntervalsALL = 0;
        std::vector<uint> intervalsALL;
        uint numIntervalsFULL = 0;
        std::vector<uint> intervalsFULL;
    }AprilDataT;

    // APRIL configuration
    typedef struct AprilConfig {
        private:
            // Hilbert curve order
            int N = 16;
            // cells per dimension in Hilbert grid: 2^N
            uint cellsPerDim = pow(2,16);
        public:
            // compression enabled, disabled
            bool compression = 0;
            // how many partitions (sections) in the data space
            int partitions = 1;
            // APRIL data file paths
            std::string ALL_intervals_path;
            std::string FULL_intervals_path;

        void setN(int N) {
            this->N = N;
            this->cellsPerDim = pow(2,N);
        }

        int getN() {
            return N;
        }
    } AprilConfigT;

    // types of possible relationships between interval lists (IL)
    typedef enum IntervalListsRelationship {
        IL_DISJOINT,        // no containment, no intersection
        IL_INTERSECT,       // no containment, yes intersection
        IL_R_INSIDE_S,
        IL_S_INSIDE_R,
        IL_MATCH,           // match == symmetrical containment
    } IntervalListsRelationshipE;

    /*************************************
     * 
     * 
     * 
     * 
     *          DATA STRUCTS
     * 
     * 
     * 
     * 
     *********************************** */


    struct ShapeBase {
        DataTypeE dataType;

        ShapeBase(DataTypeE type) : dataType(type) {}

        virtual void print() const = 0;
    };

    template <typename T>
    struct Shape : public ShapeBase {
        int recID;
        bg_rectangle mbr;
        T boostObject;

        Shape() : ShapeBase(getDataType()) {}

        void print() const override {
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

    private:
        static DataTypeE getDataType() {
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


    };

    typedef struct Point {
        double x,y;
        Point() {}
        Point(double x, double y) {
            this->x = x;
            this->y = y;
        }
    } PointT;

    typedef struct Mbr {
        PointT minPoint;
        PointT maxPoint;
    }MbrT;

    typedef struct Polygon {
        int recID;
        // partitionID -> class type of object in this partition
        std::unordered_map<int, int> partitions;
        MbrT mbr;
        bg_polygon boostPolygon;
        AprilDataT aprilData;

        void printMBR();
    } PolygonT;


    
}

#endif