#ifndef D_DEF_H
#define D_DEF_H

#include <mpi.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/linestring.hpp>
#include <boost/geometry/geometries/box.hpp>

#include "config_pub.h"

/** @typedef bg_point_xy @brief boost geometry point definition. */
typedef boost::geometry::model::d2::point_xy<double> bg_point_xy;
/** @typedef bg_linestring @brief boost geometry linestring definition. */
typedef boost::geometry::model::linestring<bg_point_xy> bg_linestring;
/** @typedef bg_rectangle @brief boost geometry rectangle definition. */
typedef boost::geometry::model::box<bg_point_xy> bg_rectangle;
/** @typedef bg_polygon @brief boost geometry polygon definition. */
typedef boost::geometry::model::polygon<bg_point_xy> bg_polygon;

#define DRIVER_GLOBAL_RANK 0
#define HOST_GLOBAL_RANK 1
#define HOST_LOCAL_RANK 0
#define AGENT_RANK 0
#define PARENT_RANK 0

extern std::string AGENT_EXECUTABLE_PATH;
extern std::string CONTROLLER_EXECUTABLE_PATH;

#define RED "\e[0;31m"
#define GREEN "\e[0;32m"
#define YELLOW "\e[0;33m"
#define BLUE "\e[0;34m"
#define PURPLE "\e[0;35m"
#define ORANGE "\e[38;5;208m"
#define NAVY "\e[38;5;17m"
#define NC "\e[0m"

#define EPS 1e-08

extern int g_world_size;
extern int g_node_rank;
extern int g_global_rank;
extern int g_parent_original_rank;

enum PROCESS_TYPE {
    DRIVER,
    CONTROLLER,
    AGENT,
};

extern PROCESS_TYPE g_proc_type;

extern MPI_Comm g_global_inter_comm;
extern MPI_Comm g_global_intra_comm;
extern MPI_Comm g_controller_comm;
extern MPI_Comm g_agent_comm;

#define DBBASE 100000

/** @enum DB_STATUS 
@brief Flags/states for status reporting. 
 * 
 * All functions should return a DB_STATUS value. After each call, there should always be a check whether 
 * the returned value is DBERR_OK. If not, the error should propagate and the program should terminate safely.
 * 
 */
enum DB_STATUS {
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
    DBERR_SERIALIZE_FAILED = DBBASE + 1015,
    DBERR_DESERIALIZE_FAILED = DBBASE + 1016,
    DBERR_INVALID_FILE_PATH = DBBASE + 1017,
    DBERR_DIR_NOT_EXIST = DBBASE + 1018,
    DBERR_OPEN_DIR_FAILED = DBBASE + 1019,
    DBERR_OPERATION_FAILED = DBBASE + 1020,
    DBERR_BUFFER_SIZE_MISMATCH = DBBASE + 1021,

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
    DBERR_QUERY_RESULT_INVALID_TYPE = DBBASE + 7002,
};

enum DatasetIndex {
    DATASET_R,
    DATASET_S,
};

/** @enum DataType @brief Spatial data types. */
enum DataType{
    DT_POINT,
    DT_LINESTRING,
    DT_RECTANGLE,
    DT_POLYGON,
};

/** @enum FilterResult @brief Possible results for the intermediate filter. */
enum FilterResult {
    TRUE_NEGATIVE,
    TRUE_HIT,
    INCONCLUSIVE,
};

/** @enum MBRRelationCase
@brief Specific relationships between the MBRs of two objects R and S. 
 * 
 * Used in the topological MBR filter.
 * */
enum MBRRelationCase {
    MBR_R_IN_S,
    MBR_S_IN_R,
    MBR_EQUAL,
    MBR_CROSS,
    MBR_INTERSECT,
};

/** @enum TopologyRelation @brief Topological relation related flags. 
 * 
 * @details Includes both the types of topological relations and the different refinement cases for the APRIL intermediate filter.  */
enum TopologyRelation {
    TR_EQUAL,
    TR_DISJOINT,
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
};

/** @enum ApproximationType @brief Spatial approximation types (mostly for polygons). */
enum ApproximationType{
    AT_NONE,
    // mine
    AT_APRIL,
    AT_RI,
    // competitors
    AT_5CCH,
    AT_RA,
    AT_GEOS,
};

/** @enum TwoLayerClass @brief The two-layer index classes. (see paper) */
enum TwoLayerClass {
    CLASS_A = 0,
    CLASS_B = 1,
    CLASS_C = 2,
    CLASS_D = 3,
    CLASS_NONE = 777,
};

/** @enum IntervalListsRelationship @brief Types of possible relationships between interval lists (IL). */
enum IntervalListsRelationship {
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
enum ActionType {
    ACTION_NONE,
    ACTION_LOAD_DATASET_R,
    ACTION_LOAD_DATASET_S,
    ACTION_PERFORM_PARTITIONING,
    ACTION_CREATE_APRIL,
    ACTION_LOAD_APRIL,
    ACTION_PERFORM_VERIFICATION,
    ACTION_QUERY,
};

/** @enum PartitioningType @brief Data partitioning methods. Only round robin is supported currently. */
enum PartitioningType {
    /** @brief The grid's cells are assigned to the nodes in a round-robin fashion. */
    PARTITIONING_ROUND_ROBIN,
    /** @brief Prefix-based assignment. @warning UNSUPPORTED. */
    PARTITIONING_PREFIX_BASED,
    /** @brief A distribution grid is used to distribute the data, then a fine-grid is used at each local node for the partitioning. */
    PARTITIONING_TWO_GRID,
};

/** @enum SystemSetupType @brief System type: Cluster or single machine (VM) */
enum SystemSetupType {
    SYS_LOCAL_MACHINE,
    SYS_CLUSTER,
};


#endif