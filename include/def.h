#ifndef D_DEF_H
#define D_DEF_H

#include <mpi.h>

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/geometries/polygon.hpp>
#include <boost/geometry/geometries/linestring.hpp>
#include <boost/geometry/geometries/box.hpp>

// boost geometry
typedef boost::geometry::model::d2::point_xy<double> bg_point_xy;
typedef boost::geometry::model::linestring<bg_point_xy> bg_linestring;
typedef boost::geometry::model::box<bg_point_xy> bg_rectangle;
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
#define NC "\e[0m"

#define EPS 1e-08

extern int g_world_size;
extern int g_node_rank;
extern int g_host_rank;
extern int g_parent_original_rank;

extern MPI_Comm g_global_comm;
extern MPI_Comm g_local_comm;

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


// types of possible relationships between interval lists (IL)
typedef enum IntervalListsRelationship {
    IL_DISJOINT,        // no containment, no intersection
    IL_INTERSECT,       // no containment, yes intersection
    IL_R_INSIDE_S,
    IL_S_INSIDE_R,
    IL_MATCH,           // match == symmetrical containment
} IntervalListsRelationshipE;

typedef enum ActionType {
    ACTION_NONE,
    ACTION_LOAD_DATASETS,
    ACTION_PERFORM_PARTITIONING,
    ACTION_CREATE_APRIL,
    ACTION_LOAD_APRIL,
    ACTION_PERFORM_VERIFICATION,
    ACTION_QUERY,
} ActionTypeE;

typedef enum PartitioningType {
    PARTITIONING_ROUND_ROBIN,
    PARTITIONING_PREFIX_BASED,
} PartitioningTypeE;

typedef enum SystemSetupType {
    SYS_LOCAL_MACHINE,
    SYS_CLUSTER,
} SystemSetupTypeE;


#endif