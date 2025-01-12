#ifndef D_DEF_H
#define D_DEF_H

#include <mpi.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/linestring.hpp>
#include <boost/geometry/geometries/box.hpp>

/** @typedef bg_point_xy @brief boost geometry point definition. */
typedef boost::geometry::model::d2::point_xy<double> bg_point_xy;
/** @typedef bg_linestring @brief boost geometry linestring definition. */
typedef boost::geometry::model::linestring<bg_point_xy> bg_linestring;
/** @typedef bg_rectangle @brief boost geometry rectangle definition. */
typedef boost::geometry::model::box<bg_point_xy> bg_rectangle;
/** @typedef bg_polygon @brief boost geometry polygon definition. */
typedef boost::geometry::model::polygon<bg_point_xy> bg_polygon;

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
#define ORANGE "\e[38;5;208m"
#define NC "\e[0m"

#define EPS 1e-08
#define DBBASE 100000

extern int g_world_size;
extern int g_node_rank;
extern int g_host_rank;
extern int g_parent_original_rank;

extern MPI_Comm g_global_comm;
extern MPI_Comm g_local_comm;

/** @enum DB_STATUS 
@brief Flags/states for status reporting. 
 * 
 * All functions should return a DB_STATUS value. After each call, there should always be a check whether 
 * the returned value is DBERR_OK. If not, the error should propagate and the program should terminate safely.
 * 
 */
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
    DBERR_DUPLICATE_ENTRY = DBBASE + 1013,
    DBERR_INVALID_KEY = DBBASE + 1014,
    DBERR_DESERIALIZE = DBBASE + 1015,
    DBERR_INVALID_FILE_PATH = DBBASE + 1016,
    DBERR_DIR_NOT_EXIST = DBBASE + 1017,
    DBERR_OPEN_DIR_FAILED = DBBASE + 1018,

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
    DBERR_MISSING_DATASET_METADATA = DBBASE + 4003,
    DBERR_MISSING_PATH = DBBASE + 4004,
    DBERR_INVALID_GEOMETRY = DBBASE + 4005,

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

enum DatasetIndexE {
    DATASET_R,
    DATASET_S,
};

/** @enum FileTypeE @brief Data file types. */
enum FileTypeE {
    FT_INVALID,
    FT_BINARY,
    FT_CSV,
    FT_WKT,
};

/** @enum FilterResultE @brief Possible results for the intermediate filter. */
enum FilterResultE {
    TRUE_NEGATIVE,
    TRUE_HIT,
    INCONCLUSIVE,
};

/** @enum QueryType @brief Query types. */
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

/** @enum MBRRelationCase
@brief Specific relationships between the MBRs of two objects R and S. 
 * 
 * Used in the topological MBR filter.
 * */
typedef enum MBRRelationCase {
    MBR_R_IN_S,
    MBR_S_IN_R,
    MBR_EQUAL,
    MBR_CROSS,
    MBR_INTERSECT,
} MBRRelationCaseE;

/** @enum TopologyRelation @brief Topological relation related flags. 
 * 
 * @details Includes both the types of topological relations and the different refinement cases for the APRIL intermediate filter.  */
typedef enum TopologyRelation {
    TR_DISJOINT,
    TR_EQUAL,
    TR_INSIDE,
    TR_CONTAINS,
    TR_MEET,
    TR_COVERS,
    TR_COVERED_BY,
    TR_INTERSECT,

    REFINE_INSIDE_COVEREDBY_TRUEHIT_INTERSECT = 1000,
    REFINE_DISJOINT_INSIDE_COVEREDBY_MEET_INTERSECT,

    REFINE_CONTAINS_COVERS_TRUEHIT_INTERSECT,
    REFINE_DISJOINT_CONTAINS_COVERS_MEET_INTERSECT,

    REFINE_DISJOINT_MEET_INTERSECT,

    REFINE_COVEREDBY_TRUEHIT_INTERSECT,
    REFINE_COVERS_TRUEHIT_INTERSECT,
    REFINE_COVERS_COVEREDBY_TRUEHIT_INTERSECT,
    REFINE_EQUAL_COVERS_COVEREDBY_TRUEHIT_INTERSECT,
} TopologyRelationE;

/** @enum DataType @brief Spatial data types. */
typedef enum DataType{
    DT_INVALID,
    DT_POINT,
    DT_LINESTRING,
    DT_RECTANGLE,
    DT_POLYGON,
} DataTypeE;

/** @enum ApproximationType @brief Spatial approximation types (mostly for polygons). */
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

/** @enum TwoLayerClass @brief The two-layer index classes. (see paper) */
typedef enum TwoLayerClass {
    CLASS_A = 0,
    CLASS_B = 1,
    CLASS_C = 2,
    CLASS_D = 3,
    CLASS_NONE = 777,
} TwoLayerClassE;

/** @enum IntervalListsRelationshipE @brief Types of possible relationships between interval lists (IL). */
enum IntervalListsRelationshipE {
    // no containment, no intersection
    IL_DISJOINT,
    // no containment, yes intersection
    IL_INTERSECT,
    IL_R_INSIDE_S,
    IL_S_INSIDE_R,
    // match == symmetrical containment
    IL_MATCH,           
};

/** @enum ActionType @brief Different actions (jobs/tasks) the host controller will initiate/broadcast. */
typedef enum ActionType {
    ACTION_NONE,
    ACTION_LOAD_DATASET_R,
    ACTION_LOAD_DATASET_S,
    ACTION_PERFORM_PARTITIONING,
    ACTION_CREATE_APRIL,
    ACTION_LOAD_APRIL,
    ACTION_PERFORM_VERIFICATION,
    ACTION_QUERY,
} ActionTypeE;

/** @enum PartitioningType @brief Data partitioning methods. Only round robin is supported currently. */
typedef enum PartitioningType {
    /** @brief The grid's cells are assigned to the nodes in a round-robin fashion. */
    PARTITIONING_ROUND_ROBIN,
    /** @brief Prefix-based assignment. @warning UNSUPPORTED. */
    PARTITIONING_PREFIX_BASED,
    /** @brief A distribution grid is used to distribute the data, then a fine-grid is used at each local node for the partitioning. */
    PARTITIONING_TWO_GRID,
} PartitioningTypeE;

/** @enum SystemSetupType @brief System type: Cluster or single machine (VM) */
typedef enum SystemSetupType {
    SYS_LOCAL_MACHINE,
    SYS_CLUSTER,
} SystemSetupTypeE;


#endif