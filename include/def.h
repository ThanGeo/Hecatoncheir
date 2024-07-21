#ifndef D_DEF_H
#define D_DEF_H

#include <unistd.h>
#include <iostream>
#include <vector>
#include <stdio.h>
#include <mpi.h>
#include <math.h>
#include <any>
#include <cstring>

#include <variant>

#include "utils.h"
#include "env/comm_def.h"
#include "config/containers.h"

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/linestring.hpp>
#include <boost/geometry/geometries/box.hpp>

#define AGENT_PROGRAM_NAME "./agent"

#define HOST_RANK 0
#define AGENT_RANK 0
#define PARENT_RANK 0
#define PARENTLESS_RANK -1

#define RED "\e[0;31m"
#define GREEN "\e[0;32m"
#define YELLOW "\e[0;33m"
#define BLUE "\e[0;34m"
#define PURPLE "\e[0;35m"
#define NC "\e[0m"

#define EPS 1e-08

extern int g_world_size;
extern int g_node_rank;
extern int g_host_rank;
extern int g_parent_original_rank;

extern MPI_Comm g_global_comm;
extern MPI_Comm g_local_comm;

// boost geometry
typedef boost::geometry::model::d2::point_xy<double> bg_point_xy;
typedef boost::geometry::model::linestring<bg_point_xy> bg_linestring;
typedef boost::geometry::model::box<bg_point_xy> bg_rectangle;
typedef boost::geometry::model::polygon<bg_point_xy> bg_polygon;

/* STATUS AND ERROR CODES */
#define DBBASE 100000
typedef enum DB_STATUS {
    DBERR_OK = DBBASE + 0,
    DB_FIN,

    // general
    DBERR_MISSING_FILE = DBBASE + 1000,
    DBERR_CREATE_DIR = DBBASE + 1001,
    DBERR_UNKNOWN_ARGUMENT = DBBASE + 1002,
    DBERR_INVALID_PARAMETER = DBBASE + 1003,
    DBERR_FEATURE_UNSUPPORTED = DBBASE + 1004,
    DBERR_MALLOC_FAILED = DBBASE + 1005,
    DBERR_BATCH_FAILED = DBBASE + 1006,
    DBERR_DISK_WRITE_FAILED = DBBASE + 1007,
    DBERR_DISK_READ_FAILED = DBBASE + 1008,
    DBERR_CONFIG_FILE = DBBASE + 1009,
    DBERR_INVALID_OPERATION = DBBASE + 1010,
    DBERR_OUT_OF_BOUNDS = DBBASE + 1011,
    DBERR_NULL_PTR_EXCEPTION = DBBASE + 1012,

    // comm
    DBERR_COMM_RECV = DBBASE + 2000,
    DBERR_COMM_SEND = DBBASE + 2001,
    DBERR_COMM_BCAST = DBBASE + 2002,
    DBERR_COMM_GET_COUNT = DBBASE + 2003,
    DBERR_COMM_WRONG_MSG_FORMAT = DBBASE + 2004,
    DBERR_COMM_UNKNOWN_INSTR = DBBASE + 2005,
    DBERR_COMM_INVALID_MSG_TAG = DBBASE + 2006,
    DBERR_COMM_WRONG_MESSAGE_ORDER = DBBASE + 2007,
    DBERR_COMM_PROBE_FAILED = DBBASE + 2008,
    DBERR_COMM_RECEIVED_NACK = DBBASE + 2009,
    
    // processes/mpi
    DBERR_MPI_INIT_FAILED = DBBASE + 3000,
    DBERR_PROC_INIT_FAILED = DBBASE + 3001,

    // data
    DBERR_INVALID_DATATYPE = DBBASE + 4000,
    DBERR_INVALID_FILETYPE = DBBASE + 4001,
    DBERR_UNSUPPORTED_DATATYPE_COMBINATION = DBBASE + 4002,
    DBERR_MISSING_DATASET_INFO = DBBASE + 4003,
    DBERR_MISSING_PATH = DBBASE + 4004,

    // partitioning
    DBERR_INVALID_PARTITION = DBBASE + 5000,
    DBERR_PARTITIONING_FAILED = DBBASE + 5001,

    // APRIL
    DBERR_APRIL_CREATE = DBBASE + 6000,
    DBERR_APRIL_UNEXPECTED_RESULT = DBBASE + 6001,

    // query
    DBERR_QUERY_INVALID_INPUT = DBBASE + 7000,
    DBERR_QUERY_INVALID_TYPE = DBBASE + 7001,
} DB_STATUS;


namespace logger
{
    // Base case of the variadic template recursion
    static inline void print_args() {
        std::cout << std::endl;
        std::cout.flush();
    }

    // Recursive template function
    template<typename T, typename... Args>
    static inline void print_args(T first, Args... rest) {
        std::cout << first << " ";
        std::cout.flush();
        print_args(rest...);
    }

    /**
     * @brief Log error function with undefined number of arguments. Separates input parameters with spaces.
     * 
     */
    template<typename T, typename... Args>
    inline void log_error(int errorCode, T first, Args... rest) {
        if (g_parent_original_rank != PARENTLESS_RANK) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
        } else {
            // controllers
            if (g_node_rank == HOST_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
            }
        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /**
     * @brief Log success function with undefined number of arguments. Separates input parameters with spaces.
     * 
     */
    template<typename T, typename... Args>
    inline void log_success(T first, Args... rest) {
        if (g_parent_original_rank != PARENTLESS_RANK) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC GREEN "[SUCCESS]" NC ": ";
        } else {
            // controllers
            if (g_node_rank == HOST_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC GREEN "[SUCCESS]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC GREEN "[SUCCESS]" NC ": ";
            }
        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /**
     * @brief Log task function with undefined number of arguments. Separates input parameters with spaces.
     * 
     */
    template<typename T, typename... Args>
    inline void log_task(T first, Args... rest) {
        if (g_parent_original_rank != PARENTLESS_RANK) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ": ";
        } else {
            // controllers
            if (g_node_rank == HOST_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC ": ";
            }
        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /**
     * @brief Log task function with undefined number of arguments. Separates input parameters with spaces.
     * 
     */
    template<typename T, typename... Args>
    inline void log_task_single_node(int rank, T first, Args... rest) {
        if (g_parent_original_rank == rank) {
            if (g_parent_original_rank != PARENTLESS_RANK) {
                // agents
                std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ": ";
            } else {
                // controllers
                if (g_node_rank == HOST_RANK) { 
                    // host controller
                    std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC ": ";
                } else {
                    // worker controller
                    std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC ": ";
                }
            }
            std::cout.flush();
            print_args(first, rest...);
        }
    }
}

typedef enum FileType {
    FT_INVALID,
    FT_BINARY,
    FT_CSV,
    FT_WKT,
} FileTypeE;

typedef enum FilterResult {
    TRUE_NEGATIVE,
    TRUE_HIT,
    INCONCLUSIVE,
} FilterResultT;

typedef enum QueryType{
    Q_NONE, // no query
    Q_RANGE,
    Q_INTERSECT,
    Q_INSIDE,
    Q_DISJOINT,
    Q_EQUAL,
    Q_MEET,
    Q_CONTAINS,
    Q_COVERS,
    Q_COVERED_BY,
    Q_FIND_RELATION,    // find what type of topological relation is there
}QueryTypeE;

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

typedef enum TwoLayerClass {
    CLASS_A,
    CLASS_B,
    CLASS_C,
    CLASS_D,
} TwoLayerClassE;

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

// rotate/flip a quadrant appropriately
inline void rot(uint32_t n, uint32_t &x, uint32_t &y, uint32_t rx, uint32_t ry) {
    if (ry == 0) {
        if (rx == 1) {
            x = n-1 - x;
            y = n-1 - y;
        }
        //Swap x and y
        uint32_t t  = x;
        x = y;
        y = t;
    }
}

// convert (x,y) to d
inline uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y) {
    uint32_t rx, ry, s;
    uint32_t d=0;
    for (s=n/2; s>0; s/=2) {
        rx = (x & s) > 0;
        ry = (y & s) > 0;
        d += s * s * ((3 * rx) ^ ry);
        rot(s, x, y, rx, ry);
    }
    return d;
}

// convert d to (x,y)
inline void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y) {
    uint32_t rx, ry, s, t=d;
    x = y = 0;
    for (s=1; s<n; s*=2) {
        rx = 1 & (t/2);
        ry = 1 & (t ^ rx);
        rot(s, x, y, rx, ry);
        x += s * rx;
        y += s * ry;
        t /= 4;
    }
}

// APRIL data
typedef struct AprilData {
    // APRIL data
    uint numIntervalsALL = 0;
    std::vector<uint32_t> intervalsALL;
    uint numIntervalsFULL = 0;
    std::vector<uint32_t> intervalsFULL;

    void printALLintervals() {
        for (int i=0; i<intervalsALL.size(); i+=2) {
            printf("[%u,%u)\n", intervalsALL[i], intervalsALL[i+1]);
        }
    }

    void printALLcells(uint32_t n) {
        uint32_t x,y,d;
        for (int i=0; i<intervalsALL.size(); i+=2) {
            for (int j=intervalsALL[i]; j<intervalsALL[i+1]; j++) {
                d2xy(n, j, x, y);
                printf("(%u,%u),", x, y);
            }
        }
        printf("\n");
    }

    void printFULLintervals() {
        for (int i=0; i<intervalsFULL.size(); i+=2) {
            printf("[%u,%u)\n", intervalsFULL[i], intervalsFULL[i+1]);
        }
    }
}AprilDataT;

// APRIL configuration
typedef struct AprilConfig {
    private:
        // Hilbert curve order
        int N = 16;
        // cells per dimension in Hilbert grid: 2^N
        int cellsPerDim = pow(2,16);
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

    int getCellsPerDim() {
        return cellsPerDim;
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

struct Point {
    double x, y;
    Point(double xVal, double yVal) : x(xVal), y(yVal) {}
};

struct MBR {
    Point pMin;
    Point pMax;

    MBR() : pMin(Point(std::numeric_limits<int>::max(), std::numeric_limits<int>::max())), pMax(Point(-std::numeric_limits<int>::max(), -std::numeric_limits<int>::max())) {}
};

// wrapper struct 
template<typename GeometryType>
struct GeometryWrapper {
public:
    GeometryType geometry;

    GeometryWrapper() {}
    explicit GeometryWrapper(const GeometryType& geom) : geometry(geom) {}

    void setGeometry(const GeometryType& geom) {
        geometry = geom;
    }

    void reset() {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: reset");
    }

    void addPoint(const double x, const double y) {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: addPoint");
    }

    bool pipTest(const bg_point_xy &point) const {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: pipTest");
        return false;
    }

    void printGeometry() {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: printGeometry");
    }

    void correctGeometry() {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: correctGeometry");
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        // Default behavior: Do nothing (for types that don't support modifying points)
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: modifyBoostPointByIndex");
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: getReferenceToPoints.");
        return nullptr;
    }

    int getVertexCount() const {
        logger::log_error(DBERR_INVALID_OPERATION, "Geometry wrapper can be accessed directly for operation: getVertexCount.");
        return 0;
    }

    template<typename OtherGeometryType>
    std::string createMaskCode(const GeometryWrapper<OtherGeometryType> &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "createMaskCode unsupported for the invoked shapes.");
        return "";
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool inside(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "intersects predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool coveredBy(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "covered bt predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool contains(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "contains predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool covers(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "covers predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool meets(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "meets predicate unsupported for the invoked shapes.");
        return false;
    }

    template<typename OtherBoostGeometryObj>
    bool equals(const OtherBoostGeometryObj &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "meets predicate unsupported for the invoked shapes.");
        return false;
    }
};

// point
template<>
struct GeometryWrapper<bg_point_xy> {
public:
    bg_point_xy geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_point_xy &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        // For points, simply replace the existing point
        geometry = bg_point_xy(x, y);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    void reset() {
        boost::geometry::clear(geometry);
    }

    bool pipTest(const bg_point_xy &point) const {
        return false;
    }

    void printGeometry() {
        printf("(%f,%f)\n", geometry.x(), geometry.y());
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index != 0) {
            logger::log_error(DBERR_INVALID_OPERATION, "Ignoring non-zero index for point shape, modifying the point anyway.");
        }
        geometry = bg_point_xy(x, y);
    }


    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        logger::log_error(DBERR_INVALID_OPERATION, "Can't return reference to points on Point shape.");
        return nullptr;
    }

    int getVertexCount() const {
        return 1;
    }

    /**
     * queries
     */

    std::string createMaskCode(const GeometryWrapper<bg_polygon>& other) const;
    std::string createMaskCode(const GeometryWrapper<bg_point_xy>& other) const {return "";}
    std::string createMaskCode(const GeometryWrapper<bg_linestring>& other) const {return "";}
    std::string createMaskCode(const GeometryWrapper<bg_rectangle>& other) const {return "";}

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_polygon>& other) const;
    bool inside(const GeometryWrapper<bg_linestring>& other) const;
    bool inside(const GeometryWrapper<bg_rectangle>& other) const;
    bool inside(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_rectangle>& other) const {return false;}

    bool meets(const GeometryWrapper<bg_polygon>& other) const;
    bool meets(const GeometryWrapper<bg_linestring>& other) const;
    bool meets(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool meets(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }

    bool equals(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};


// rectangle
template<>
struct GeometryWrapper<bg_rectangle> {
public:
    bg_rectangle geometry;
    std::vector<bg_point_xy> vertices;
    GeometryWrapper(){}
    GeometryWrapper(const bg_rectangle &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        bg_point_xy point(x, y);
        if (geometry.min_corner().x() == 0 && geometry.min_corner().y() == 0) {
            // no points yet, add min point
            geometry.min_corner() = point;
        } else if (geometry.max_corner().x() == 0 && geometry.max_corner().y() == 0) {
            // one point exists, add max point
            geometry.max_corner() = point;
        } else {
            // both points already exist, error
            logger::log_error(DBERR_INVALID_OPERATION, "Cannot add more than two points to a rectangle");
        }
        vertices.emplace_back(point);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    bool pipTest(const bg_point_xy &point) const {
        return boost::geometry::within(point, geometry);
    }

    void reset() {
        boost::geometry::clear(geometry);
        vertices.clear();
    }

    void printGeometry() {
        printf("(%f,%f),(%f,%f),(%f,%f),(%f,%f)\n", geometry.min_corner().x(), geometry.min_corner().y(),
        geometry.max_corner().x(), geometry.min_corner().y(),
        geometry.max_corner().x(), geometry.max_corner().y(),
        geometry.min_corner().x(), geometry.max_corner().y());
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index == 0) {
            geometry.min_corner() = bg_point_xy(x, y);
        } else if (index == 1) {
            geometry.max_corner() = bg_point_xy(x, y);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Rectangle point index out of bounds for modifyBoostPointByIndex:", index);
        }
        vertices[index] = bg_point_xy(x, y);
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        return &vertices;
    }

    int getVertexCount() const {
        return 2;
    }

    /**
     * queries
     */
    
    template<typename OtherGeometryType>
    std::string createMaskCode(const GeometryWrapper<OtherGeometryType> &other) const {
        logger::log_error(DBERR_INVALID_OPERATION, "createMaskCode unsupported for the invoked shapes.");
        return "";
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_linestring> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_rectangle> &other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_linestring> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_rectangle> &other) const {
        return boost::geometry::within(other.geometry, geometry);
    }
    bool contains(const GeometryWrapper<bg_point_xy> &other) const {
        return boost::geometry::within(other.geometry, geometry);
    }

    bool meets(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool meets(const GeometryWrapper<bg_linestring> &other) const {return false;}
    bool meets(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool meets(const GeometryWrapper<bg_rectangle> &other) const {return false;}

    bool equals(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const;
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};

// linestring
template<>
struct GeometryWrapper<bg_linestring> {
public:
    bg_linestring geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_linestring &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        bg_point_xy point(x, y);
        geometry.push_back(point);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    bool pipTest(const bg_point_xy &point) const {
        return false;
    }

    void printGeometry() {
        for(auto &it: geometry) {
            printf("(%f,%f),", it.x(), it.y());
        }
        printf("\n");
    }

    void reset() {
        boost::geometry::clear(geometry);
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index < geometry.size()) {
            geometry[index] = bg_point_xy(x, y);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Linestring point index out of bounds for modifyBoostPointByIndex:", index);
        }
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        return &geometry;
    }

    int getVertexCount() const {
        return geometry.size();
    }

    // topology
    // declaration
    std::string createMaskCode(const GeometryWrapper<bg_polygon>& other) const;
    std::string createMaskCode(const GeometryWrapper<bg_point_xy>& other) const {return "";}
    std::string createMaskCode(const GeometryWrapper<bg_rectangle>& other) const {return "";}
    // definitions
    std::string createMaskCode(const GeometryWrapper<bg_linestring>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }
    
    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_polygon> &other) const;
    bool inside(const GeometryWrapper<bg_rectangle> &other) const {return false;}
    bool inside(const GeometryWrapper<bg_linestring> &other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_point_xy> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_rectangle> &other) const {return false;}
    bool contains(const GeometryWrapper<bg_linestring> &other) const {return false;}

    bool meets(const GeometryWrapper<bg_polygon>& other) const;
    bool meets(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool meets(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }
    bool meets(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }

    bool equals(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};

// polygon
template<>
struct GeometryWrapper<bg_polygon> {
public:
    bg_polygon geometry;
    GeometryWrapper(){}
    GeometryWrapper(const bg_polygon &geom) : geometry(geom) {}

    void addPoint(const double x, const double y) {
        bg_point_xy point(x, y);
        boost::geometry::append(geometry.outer(), point);
    }

    void correctGeometry() {
        boost::geometry::correct(geometry);
    }

    void printGeometry() {
        for(auto &it: geometry.outer()) {
            printf("(%f,%f),", it.x(), it.y());
        }
        printf("\n");
    }

    void reset() {
        boost::geometry::clear(geometry);
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        if (index < geometry.outer().size()) {
            geometry.outer()[index] = bg_point_xy(x, y);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Polygon point index out of bounds for modifyBoostPointByIndex:", index);
        }
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() const {
        return &geometry.outer();
    }

    int getVertexCount() const {
        return geometry.outer().size();
    }

    // APRIL
    bool pipTest(const bg_point_xy &point) const {
        return boost::geometry::within(point, geometry);
    }

    // topology definitions
    std::string createMaskCode(const GeometryWrapper<bg_rectangle>& other) const {return "";};
    std::string createMaskCode(const GeometryWrapper<bg_polygon>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }
    std::string createMaskCode(const GeometryWrapper<bg_linestring>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }
    std::string createMaskCode(const GeometryWrapper<bg_point_xy>& other) const {
        boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
        return matrix.str();
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return boost::geometry::intersects(geometry, other.geometry);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return boost::geometry::disjoint(geometry, other.geometry);
    }

    bool inside(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool inside(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool inside(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool inside(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::within(geometry, other.geometry);
    }

    bool contains(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool contains(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::within(other.geometry, geometry);
    }
    bool contains(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::within(other.geometry, geometry);
    }
    bool contains(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::within(other.geometry, geometry);
    }

    bool meets(const GeometryWrapper<bg_rectangle>& other) const {return false;}
    bool meets(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }
    bool meets(const GeometryWrapper<bg_point_xy>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }
    bool meets(const GeometryWrapper<bg_linestring>& other) const {
        return boost::geometry::touches(geometry, other.geometry);
    }

    bool equals(const GeometryWrapper<bg_point_xy>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_linestring>& other) const {return false;}
    bool equals(const GeometryWrapper<bg_polygon>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
    bool equals(const GeometryWrapper<bg_rectangle>& other) const {
        return boost::geometry::equals(geometry, other.geometry);
    }
};

/* define forward declared member functions for the wrappers */
// linestring
inline std::string GeometryWrapper<bg_linestring>::createMaskCode(const GeometryWrapper<bg_polygon>& other) const {
    boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
    return matrix.str();
}
inline bool GeometryWrapper<bg_linestring>::inside(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_linestring>::meets(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}

// point
inline std::string GeometryWrapper<bg_point_xy>::createMaskCode(const GeometryWrapper<bg_polygon>& other) const  {
    boost::geometry::de9im::matrix matrix = boost::geometry::relation(geometry, other.geometry);
    return matrix.str();
}
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_linestring> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_rectangle> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::inside(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::within(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::meets(const GeometryWrapper<bg_linestring> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}
inline bool GeometryWrapper<bg_point_xy>::meets(const GeometryWrapper<bg_polygon> &other) const {
    return boost::geometry::touches(geometry, other.geometry);
}

//rectangle
inline bool GeometryWrapper<bg_rectangle>::equals(const GeometryWrapper<bg_polygon>& other) const {
    return boost::geometry::equals(geometry, other.geometry);
}

// define shape wrappers and shape struct
using PointWrapper = GeometryWrapper<bg_point_xy>;
using PolygonWrapper = GeometryWrapper<bg_polygon>;
using LineStringWrapper = GeometryWrapper<bg_linestring>;
using RectangleWrapper = GeometryWrapper<bg_rectangle>;

using ShapeVariant = std::variant<PointWrapper, PolygonWrapper, LineStringWrapper, RectangleWrapper>;

struct Shape {
    size_t recID;
    DataTypeE dataType;
    MBR mbr;
    // partition ID -> two layer class of this object in that partition
    std::unordered_map<int, int> partitions;
    // shape variant
    ShapeVariant shape;

    Shape() {}

    template<typename T>
    explicit Shape(T geom) : shape(geom) {}

    /**
     * utils
     */

    // adds a point to the shape
    void addPoint(const double x, const double y) {
        // then in boost object
        std::visit([&x, &y](auto&& arg) {
            arg.addPoint(x, y);
        }, shape);
    }

    void correctGeometry() {
        std::visit([](auto&& arg) {
            arg.correctGeometry();
        }, shape);
    }

    void modifyBoostPointByIndex(int index, double x, double y) {
        std::visit([index, &x, &y](auto&& arg) {
            arg.modifyBoostPointByIndex(index, x, y);
        }, shape);
    }

    void printGeometry() {
        printf("id: %zu\n", recID);
        std::visit([](auto&& arg) {
            arg.printGeometry();
        }, shape);
    }

    void reset() {
        recID = 0;
        dataType = DT_INVALID;
        mbr = MBR();
        partitions.clear();
        std::visit([](auto&& arg) {
            arg.reset();
        }, shape);
    }

    const std::vector<bg_point_xy>* getReferenceToPoints() {
        return std::visit([](auto&& arg) -> const std::vector<bg_point_xy>* {
            return arg.getReferenceToPoints();
        }, shape);
    }

    int getVertexCount() {
        return std::visit([](auto&& arg) -> int {
            return arg.getVertexCount();
        }, shape);
    }

    /**
     * APRIL
     */

    bool pipTest(const bg_point_xy& point) const {
        return std::visit([&point](auto&& arg) -> bool {
            return arg.pipTest(point);
        }, shape);
    }

    /**
     * queries/spatial operations
     */
    std::string createMaskCode(const Shape &other) const {
        return std::visit([&other](auto&& arg) -> std::string {
            return std::visit([&arg](auto&& otherArg) -> std::string {
                return arg.createMaskCode(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool intersects(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.intersects(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool disjoint(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.disjoint(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool inside(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.inside(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool coveredBy(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.inside(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool contains(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.contains(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool covers(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.contains(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool meets(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.meets(otherArg);
            }, other.shape);
        }, shape);
    }

    template<typename OtherBoostGeometryObj>
    bool equals(const OtherBoostGeometryObj &other) const {
        return std::visit([&other](auto&& arg) -> bool {
            return std::visit([&arg](auto&& otherArg) -> bool {
                return arg.equals(otherArg);
            }, other.shape);
        }, shape);
    }

};

namespace shape_factory
{
    // Create empty shapes
    Shape createEmptyPointShape();
    Shape createEmptyPolygonShape();
    Shape createEmptyLineStringShape();
    Shape createEmptyRectangleShape();
}

typedef struct QueryOutput {
    // for regular query rsesults
    int queryResults;
    // for topology relations results
    std::unordered_map<int,uint> topologyRelationsResultMap;
    // statistics
    int postMBRFilterCandidates;
    int refinementCandidates;
    int trueHits;
    int trueNegatives;
    // times
    double totalTime;
    double mbrFilterTime;
    double iFilterTime;
    double refinementTime;
    // on the fly april
    uint rasterizationsDone;

    QueryOutput();


    void reset();
    void countAPRILresult(int result);
    void countResult();
    void countMBRresult();
    void countRefinementCandidate();
    void countTopologyRelationResult(int result);
    int getResultForTopologyRelation(TopologyRelationE relation);
    void setTopologyRelationResult(int relation, int result);
    /**
     * @brief copies the contents of the 'other' object into this struct
     */
    void shallowCopy(QueryOutput &other);
} QueryOutputT;
// global query output variable
extern QueryOutputT g_queryOutput;


typedef struct Section {
    uint sectionID;
    // axis position indexes
    uint i,j;
    //objects that intersect this MBR will be assigned to this area
    double interestxMin, interestyMin, interestxMax, interestyMax;
    // double normInterestxMin, normInterestyMin, normInterestxMax, normInterestyMax;
    //this MBR defines the rasterization area (widened interest area to include intersecting polygons completely)
    double rasterxMin, rasteryMin, rasterxMax, rasteryMax;
    // double normRasterxMin, normRasteryMin, normRasterxMax, normRasteryMax;
    // APRIL data
    size_t objectCount = 0;
    std::unordered_map<size_t, AprilDataT> aprilData;
} SectionT;

typedef struct DataspaceInfo {
    double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
    double xExtent, yExtent, maxExtent;
    bool boundsSet = false;

    DataspaceInfo() {
        xMinGlobal = std::numeric_limits<int>::max();
        yMinGlobal = std::numeric_limits<int>::max();
        xMaxGlobal = -std::numeric_limits<int>::max();
        yMaxGlobal = -std::numeric_limits<int>::max();
    }

    void set(double xMinGlobal, double yMinGlobal, double xMaxGlobal, double yMaxGlobal) {
        this->xMinGlobal = xMinGlobal - EPS;
        this->yMinGlobal = yMinGlobal - EPS;
        this->xMaxGlobal = xMaxGlobal + EPS;
        this->yMaxGlobal = yMaxGlobal + EPS;
        this->xExtent = this->xMaxGlobal - this->xMinGlobal;
        this->yExtent = this->yMaxGlobal - this->yMinGlobal;
        this->maxExtent = std::max(this->xExtent, this->yExtent);
        // printf("Dataset bounds: (%f,%f),(%f,%f)\n", this->xMinGlobal, this->yMinGlobal, this->xMaxGlobal, this->yMaxGlobal);
        // printf("xExtent: %f, yExtent: %f\n", this->xExtent, this->yExtent);
        // printf("Max extent: %f\n", this->maxExtent);
    }

    void clear() {
        xMinGlobal = 0;
        yMinGlobal = 0;
        xMaxGlobal = 0;
        yMaxGlobal = 0;
        xExtent = 0;
        yExtent = 0;
    }
} DataspaceInfoT;


struct TwoLayerContainer {
    std::unordered_map<TwoLayerClassE, std::vector<Shape>> classIndex;

    std::vector<Shape>* getOrCreateContainerClassContents(TwoLayerClassE classType) {
        auto it = classIndex.find(classType);
        if (it == classIndex.end()) {
            // does not exist, create
            createClassEntry(classType);
            // return its reference
            return &classIndex.find(classType)->second;
        }
        // exists
        return &it->second;
    }

    std::vector<Shape>* getContainerClassContents(TwoLayerClassE classType) {
        auto it = classIndex.find(classType);
        if (it == classIndex.end()) {
            // does not exist
            return nullptr;
        }
        // exists
        return &it->second;
    }

    void createClassEntry(TwoLayerClassE classType) {
        std::vector<Shape> vec;
        auto it = classIndex.find(classType);
        if (it == classIndex.end()) {
            classIndex.insert(std::make_pair(classType, vec));
        } else {
            classIndex[classType] = vec;
        }
    }
};

struct TwoLayerIndex {
    std::unordered_map<int, TwoLayerContainer> partitionIndex;
    std::vector<int> partitionIDs;
    // methods
private:
    static bool compareByY(const Shape &a, const Shape &b) {
        return a.mbr.pMin.y < b.mbr.pMin.y;
    }
public:
    /** @brief add an object reference to the partition with partitionID, with the specified classType */
    void addObject(int partitionID, TwoLayerClassE classType, Shape &object) {
        // get or create new partition entry
        TwoLayerContainer* partition = getOrCreatePartition(partitionID);
        // get or create new class entry of this class type, for this partition
        std::vector<Shape>* classObjects = partition->getOrCreateContainerClassContents(classType);
        // add object
        classObjects->emplace_back(object);
    }

    TwoLayerContainer* getOrCreatePartition(int partitionID) {
        auto it = partitionIndex.find(partitionID);
        if (it == partitionIndex.end()) {
            // does not exist, create it
            createPartitionEntry(partitionID);
            // return its reference
            TwoLayerContainer* ref = &partitionIndex.find(partitionID)->second;
            return ref;
        } 
        // exists
        return &it->second;
    }

    TwoLayerContainer* getPartition(int partitionID) {
        auto it = partitionIndex.find(partitionID);
        if (it == partitionIndex.end()) {
            // does not exist
            return nullptr;
        } 
        // exists
        return &it->second;
    }

    void createPartitionEntry(int partitionID) {
        TwoLayerContainer container;
        auto it = partitionIndex.find(partitionID);
        if (it == partitionIndex.end()) {
            // insert new
            partitionIndex.insert(std::make_pair(partitionID, container));
            partitionIDs.emplace_back(partitionID);
        } else {
            // replace existing
            partitionIndex[partitionID] = container;
        }
    }
    
    void sortPartitionsOnY() {
        for (auto &it: partitionIndex) {
            // sort A
            std::vector<Shape>* objectsA = it.second.getContainerClassContents(CLASS_A);
            if (objectsA != nullptr) {
                std::sort(objectsA->begin(), objectsA->end(), compareByY);
            }
            // sort C
            std::vector<Shape>* objectsC = it.second.getContainerClassContents(CLASS_C);
            if (objectsC != nullptr) {
                std::sort(objectsC->begin(), objectsC->end(), compareByY);
            }
        }
    }

};

struct Dataset{
    DataTypeE dataType;
    FileTypeE fileType;
    std::string path;
    // derived from the path
    std::string datasetName;
    // as given by arguments and specified by datasets.ini config file
    std::string nickname;
    // holds the dataset's dataspace info (MBR, extent)
    DataspaceInfoT dataspaceInfo;
    // unique object count
    size_t totalObjects = 0;
    // two layer
    TwoLayerIndex twoLayerIndex;
    /* approximations */ 
    ApproximationTypeE approxType;
    // APRIL
    AprilConfigT aprilConfig;
    std::unordered_map<uint, SectionT> sectionMap;                        // map: k,v = sectionID,(unordered map of k,v = recID,aprilData)
    std::unordered_map<size_t,std::vector<uint>> recToSectionIdMap;         // map: k,v = recID,vector<sectionID>: maps recs to sections 

    /**
     * Methods
    */
    void addObject(Shape &object) {
        // insert reference to partition index
        for (auto &partitionIT: object.partitions) {
            int partitionID = partitionIT.first;
            TwoLayerClassE classType = (TwoLayerClassE) partitionIT.second;
            // add to twolayer index
            this->twoLayerIndex.addObject(partitionID, classType, object);
        }
    }

    // calculate the size needed for the serialization buffer
    int calculateBufferSize();
    /**
     * serialize dataset info (only specific stuff)
     */
    int serialize(char **buffer);
    /**
     * deserializes a serialized buffer that contains the dataset info, 
     * into this Dataset object
     */
    void deserialize(const char *buffer, int bufferSize);
    // APRIL
    void addAprilDataToApproximationDataMap(const uint sectionID, const size_t recID, const AprilDataT &aprilData);
    void addObjectToSectionMap(const uint sectionID, const size_t recID);
    void addIntervalsToAprilData(const uint sectionID, const size_t recID, const int numIntervals, const std::vector<uint32_t> &intervals, const bool ALL);
    AprilDataT* getAprilDataBySectionAndObjectID(uint sectionID, size_t recID);
};

struct Query{
    QueryTypeE type;
    int numberOfDatasets;
    Dataset R;         // R: left dataset
    Dataset S;         // S: right dataset
    bool boundsSet = false;
    // double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
    DataspaceInfoT dataspaceInfo;
};

inline uint getSectionIDFromIdxs(uint i, uint j, uint partitionsNum) {
    return (j * partitionsNum) + i;
}

/**
 * @brief returns a list of the common section ids between two objects of two datasets
 * 
 * @param datasetR 
 * @param datasetS 
 * @param idR 
 * @param idS 
 * @return std::vector<uint> 
 * 
 */
typedef struct DirectoryPaths {
    std::string configFilePath = "../config.ini";
    const std::string datasetsConfigPath = "../datasets.ini";
    std::string dataPath = "../data/";
    std::string partitionsPath = "../data/partitions/";
    std::string approximationPath = "../data/approximations/";

    void setNodeDataDirectories(std::string &dataPath) {
        this->dataPath = dataPath;
        this->partitionsPath = dataPath + "partitions/";
        this->approximationPath = dataPath + "approximations/";
    }
} DirectoryPathsT;

typedef enum ActionType {
    ACTION_NONE,
    ACTION_LOAD_DATASETS,
    ACTION_PERFORM_PARTITIONING,
    ACTION_CREATE_APRIL,
    ACTION_LOAD_APRIL,
    ACTION_PERFORM_VERIFICATION,
    ACTION_QUERY,
} ActionTypeE;
std::string actionIntToStr(ActionTypeE action);

typedef enum PartitioningType {
    PARTITIONING_ROUND_ROBIN,
    PARTITIONING_PREFIX_BASED,
} PartitioningTypeE;

typedef struct PartitioningInfo {
    PartitioningTypeE type;                 // type of the partitioning technique
    int partitionsPerDimension;             // # of partitions per dimension
    int batchSize;                          // size of each batche, in number of objects
    
    // cell enumeration function
    int getCellID(int i, int j) {
        return (i + (j * partitionsPerDimension));
    }
    // node assignment function
    int getNodeRankForPartitionID(int partitionID) {
        return (partitionID % g_world_size);
    }
} PartitioningInfoT;

typedef struct Action {
    ActionTypeE type;

    Action(){
        this->type = ACTION_NONE;
    }

    Action(ActionTypeE type) {
        this->type = type;
    }
    
} ActionT;

struct DatasetInfo {
    private:
    Dataset* R;
    Dataset* S;
    int numberOfDatasets;

    public:
    std::unordered_map<std::string,Dataset> datasets;
    DataspaceInfoT dataspaceInfo;
    
    Dataset* getDatasetByNickname(std::string &nickname);

    int getNumberOfDatasets() {
        return numberOfDatasets;
    }

    void clear() {
        numberOfDatasets = 0;
        R = nullptr;
        S = nullptr;
        datasets.clear();
        dataspaceInfo.clear();
    }

    inline Dataset* getDatasetR() {
        return R;
    }

    inline Dataset* getDatasetS() {
        return S;
    }

    void addDataset(Dataset &dataset) {
        // add to datasets struct
        datasets.insert(std::make_pair(dataset.nickname, dataset));
        if (numberOfDatasets < 1) {
            // R is being added
            R = &datasets.find(dataset.nickname)->second;
        } else {
            // S is being added
            S = &datasets.find(dataset.nickname)->second;
        }
        numberOfDatasets++;
        // update dataspace info
        this->updateDataspace();
    }

    void updateDataspace() {
        // find the bounds that enclose both datasets
        for (auto &it: datasets) {
            dataspaceInfo.xMinGlobal = std::min(dataspaceInfo.xMinGlobal, it.second.dataspaceInfo.xMinGlobal);
            dataspaceInfo.yMinGlobal = std::min(dataspaceInfo.yMinGlobal, it.second.dataspaceInfo.yMinGlobal);
            dataspaceInfo.xMaxGlobal = std::max(dataspaceInfo.xMaxGlobal, it.second.dataspaceInfo.xMaxGlobal);
            dataspaceInfo.yMaxGlobal = std::max(dataspaceInfo.yMaxGlobal, it.second.dataspaceInfo.yMaxGlobal);
        }
        dataspaceInfo.xExtent = dataspaceInfo.xMaxGlobal - dataspaceInfo.xMinGlobal;
        dataspaceInfo.yExtent = dataspaceInfo.yMaxGlobal - dataspaceInfo.yMinGlobal;
        // set as both datasets' bounds
        for (auto &it: datasets) {
            it.second.dataspaceInfo = dataspaceInfo;
        }
    }
};

extern std::vector<int> getCommonSectionIDsOfObjects(Dataset *datasetR, Dataset *datasetS, size_t idR, size_t idS);

typedef struct ApproximationInfo {
    ApproximationTypeE type;   // sets which of the following fields will be used
    AprilConfigT aprilConfig;  
    
    ApproximationInfo() {
        this->type = AT_NONE;
    }

    ApproximationInfo(ApproximationTypeE type) {
        this->type = type;
    }
} ApproximationInfoT;

typedef struct QueryInfo {
    QueryTypeE type = Q_NONE;
    int MBRFilter = 1;
    int IntermediateFilter = 1;
    int Refinement = 1;
} QueryInfoT;

struct Config {
    DirectoryPathsT dirPaths;
    SystemOptionsT options;
    std::vector<ActionT> actions;
    PartitioningInfoT partitioningInfo;
    DatasetInfo datasetInfo;
    ApproximationInfoT approximationInfo;
    QueryInfoT queryInfo;
};

typedef struct Geometry {
    size_t recID;
    int partitionCount;
    std::vector<int> partitions;    // tuples of <partition ID, twolayer class>
    int vertexCount;
    std::vector<double> coords;

    Geometry(size_t recID, int vertexCount, std::vector<double> &coords) {
        this->recID = recID;
        this->vertexCount = vertexCount;
        this->coords = coords;
    }

    Geometry(size_t recID, std::vector<int> &partitions, int vertexCount, std::vector<double> &coords) {
        this->recID = recID;
        this->partitions = partitions;
        this->partitionCount = partitions.size() / 2; 
        this->vertexCount = vertexCount;
        this->coords = coords;
    }

    void setPartitions(std::vector<int> &ids, std::vector<TwoLayerClassE> &classes) {
        for (int i=0; i<ids.size(); i++) {
            partitions.emplace_back(ids.at(i));
            partitions.emplace_back(classes.at(i));
        }
        this->partitionCount = ids.size();
    }

} GeometryT;

typedef struct GeometryBatch {
    // serializable
    size_t objectCount = 0;
    std::vector<GeometryT> geometries;
    // unserializable/unclearable (todo: make const?)
    int destRank = -1;   // destination node rank
    size_t maxObjectCount; 
    MPI_Comm* comm; // communicator that the batch will be send through
    int tag = -1;        // MPI tag = indicates spatial data type

    bool isValid() {
        return !(destRank == -1 || tag == -1 || comm == nullptr);
    }

    void addGeometryToBatch(GeometryT &geometry) {
        geometries.emplace_back(geometry);
        objectCount += 1;
    }

    void setDestNodeRank(int destRank) {
        this->destRank = destRank;
    }

    // calculate the size needed for the serialization buffer
    int calculateBufferSize() {
        int size = 0;
        size += sizeof(size_t);                        // objectCount
        
        for (auto &it: geometries) {
            size += sizeof(size_t); // recID
            size += sizeof(int);    // partition count
            size += it.partitions.size() * 2 * sizeof(int); // partitions id + class
            size += sizeof(it.vertexCount); // vertex count
            size += it.vertexCount * 2 * sizeof(double);    // vertices
        }
        
        return size;
    }

    void clear() {
        objectCount = 0;
        geometries.clear();
    }

    int serialize(char **buffer) {
        // calculate size
        int bufferSize = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSize * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            return -1;
        }
        char* localBuffer = *buffer;

        // add object count
        *reinterpret_cast<size_t*>(localBuffer) = objectCount;
        localBuffer += sizeof(size_t);

        // add batch geometry info
        for (auto &it : geometries) {
            *reinterpret_cast<size_t*>(localBuffer) = it.recID;
            localBuffer += sizeof(size_t);
            *reinterpret_cast<int*>(localBuffer) = it.partitionCount;
            localBuffer += sizeof(int);
            std::memcpy(localBuffer, it.partitions.data(), it.partitionCount * 2 * sizeof(int));
            localBuffer += it.partitionCount * 2 * sizeof(int);
            *reinterpret_cast<int*>(localBuffer) = it.vertexCount;
            localBuffer += sizeof(int);
            std::memcpy(localBuffer, it.coords.data(), it.vertexCount * 2 * sizeof(double));
            localBuffer += it.vertexCount * 2 * sizeof(double);
        }

        return bufferSize;
    }

    /**
     * @brief fills the struct with data from the input serialized buffer
     * The caller must free the buffer memory
     * @param buffer 
     */
    void deserialize(const char *buffer, int bufferSize) {
        size_t recID;
        int vertexCount, partitionCount;
        const char *localBuffer = buffer;

        // get object count
        size_t objectCount = *reinterpret_cast<const size_t*>(localBuffer);
        localBuffer += sizeof(size_t);

        // extend reserve space
        geometries.reserve(geometries.size() + objectCount);

        // deserialize fields for each object in the batch
        for (size_t i=0; i<objectCount; i++) {
            // rec id
            recID = *reinterpret_cast<const size_t*>(localBuffer);
            localBuffer += sizeof(size_t);
            // partition count
            partitionCount = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            // partitions
            std::vector<int> partitions(partitionCount * 2);
            const int* partitionPtr = reinterpret_cast<const int*>(localBuffer);
            for (size_t j = 0; j < partitionCount*2; j++) {
                partitions[j] = partitionPtr[j];
            }
            localBuffer += partitionCount * 2 * sizeof(int);
            // vertex count
            vertexCount = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            // vertices
            std::vector<double> coords(vertexCount * 2);
            const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
            for (size_t j = 0; j < vertexCount * 2; j++) {
                coords[j] = vertexDataPtr[j];
            }
            localBuffer += vertexCount * 2 * sizeof(double);
            
            // add to batch
            GeometryT geometry(recID, partitions, vertexCount, coords);
            this->addGeometryToBatch(geometry);
        }
    }

} GeometryBatchT;

/**
 * @brief global configuration variable 
*/
extern Config g_config;


/**
 * @brief based on query type in config, it appropriately adds the query output in parallel
 * Used only in parallel sections for OpenMP
 */
void queryResultReductionFunc(QueryOutputT &in, QueryOutputT &out);

// Declare the parallel reduction
#pragma omp declare reduction \
    (query_output_reduction: QueryOutputT: queryResultReductionFunc(omp_in, omp_out)) \
    initializer(omp_priv = QueryOutput())


namespace mapping
{
    extern std::string actionIntToStr(ActionTypeE action);
    extern std::string queryTypeIntToStr(QueryTypeE val);
    extern QueryTypeE queryTypeStrToInt(std::string &str);
    extern std::string dataTypeIntToStr(DataTypeE val);
    extern DataTypeE dataTypeTextToInt(std::string str);
    extern FileTypeE fileTypeTextToInt(std::string str);
    extern std::string relationIntToStr(int relation);
}



#endif