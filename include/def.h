#ifndef D_DEF_H
#define D_DEF_H

#include <unistd.h>
#include <iostream>
#include <vector>
#include <stdio.h>
#include <mpi.h>

#include "SpatialLib.h"
#include "utils.h"
#include "env/comm_def.h"
#include "config/containers.h"

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

typedef struct DatasetInfo {
    private:
    spatial_lib::DatasetT* R;
    spatial_lib::DatasetT* S;
    int numberOfDatasets;
    spatial_lib::DatatypeCombinationE datatypeCombination = spatial_lib::DC_INVALID_COMBINATION;

    public:
    std::unordered_map<std::string,spatial_lib::DatasetT> datasets;
    spatial_lib::DataspaceInfoT dataspaceInfo;
    
    spatial_lib::DatasetT* getDatasetByNickname(std::string &nickname) {
        auto it = datasets.find(nickname);
        if (it == datasets.end()) {
            return nullptr;
        }        
        return &it->second;
    }

    int getNumberOfDatasets() {
        return numberOfDatasets;
    }

    void clear() {
        numberOfDatasets = 0;
        R = nullptr;
        S = nullptr;
        datasets.clear();
        dataspaceInfo.clear();
        datatypeCombination = spatial_lib::DC_INVALID_COMBINATION;
    }

    inline spatial_lib::DatasetT* getDatasetR() {
        return R;
    }

    inline spatial_lib::DatasetT* getDatasetS() {
        return S;
    }

    spatial_lib::DatatypeCombinationE getDatatypeCombination() {
        return datatypeCombination;
    }

    void setDatatypeCombination() {
        // R is points
        if (getDatasetR()->dataType == spatial_lib::DT_POINT) {
            if (getDatasetS()->dataType == spatial_lib::DT_POINT) {
                datatypeCombination = spatial_lib::DC_POINT_POINT;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_LINESTRING) {
                datatypeCombination = spatial_lib::DC_POINT_LINESTRING;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_RECTANGLE) {
                datatypeCombination = spatial_lib::DC_POINT_RECTANGLE;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_POLYGON) {
                datatypeCombination = spatial_lib::DC_POINT_POLYGON;
            }
        }
        // R is linestrings
        if (getDatasetR()->dataType == spatial_lib::DT_LINESTRING) {
            if (getDatasetS()->dataType == spatial_lib::DT_POINT) {
                datatypeCombination = spatial_lib::DC_LINESTRING_POINT;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_LINESTRING) {
                datatypeCombination = spatial_lib::DC_LINESTRING_LINESTRING;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_RECTANGLE) {
                datatypeCombination = spatial_lib::DC_LINESTRING_RECTANGLE;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_POLYGON) {
                datatypeCombination = spatial_lib::DC_LINESTRING_POLYGON;
            }
        }
        // R is rectangles
        if (getDatasetR()->dataType == spatial_lib::DT_RECTANGLE) {
            if (getDatasetS()->dataType == spatial_lib::DT_POINT) {
                datatypeCombination = spatial_lib::DC_RECTANGLE_POINT;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_LINESTRING) {
                datatypeCombination = spatial_lib::DC_RECTANGLE_LINESTRING;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_RECTANGLE) {
                datatypeCombination = spatial_lib::DC_RECTANGLE_RECTANGLE;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_POLYGON) {
                datatypeCombination = spatial_lib::DC_RECTANGLE_POLYGON;
            }
        }
        // R is polygons
        if (getDatasetR()->dataType == spatial_lib::DT_POLYGON) {
            if (getDatasetS()->dataType == spatial_lib::DT_POINT) {
                datatypeCombination = spatial_lib::DC_POLYGON_POINT;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_LINESTRING) {
                datatypeCombination = spatial_lib::DC_POLYGON_LINESTRING;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_RECTANGLE) {
                datatypeCombination = spatial_lib::DC_POLYGON_RECTANGLE;
            }
            if (getDatasetS()->dataType == spatial_lib::DT_POLYGON) {
                datatypeCombination = spatial_lib::DC_POLYGON_POLYGON;
            }
        }
    }

    void addDataset(spatial_lib::DatasetT &dataset) {
        // add to datasets struct
        datasets.insert(std::make_pair(dataset.nickname, dataset));

        // test
        // if (g_parent_original_rank == 0) {
        //     logger::log_task("before insertion: dataset", dataset.nickname, "partition",507736);

        //     spatial_lib::TwoLayerContainerT* tlContainerR = dataset.twoLayerIndex.getPartition(507736);
        //     if (tlContainerR != nullptr) {
        //         std::vector<spatial_lib::PolygonT>* pols = tlContainerR->getContainerClassContents(spatial_lib::CLASS_A);
        //         if (pols != nullptr) { 
        //             for (auto& it: *pols) {
        //                 logger::log_task(it.recID, it.mbr.minPoint.x, it.mbr.minPoint.y, it.mbr.maxPoint.x, it.mbr.maxPoint.y);
        //             }
        //         }
        //     }
        // }
        if (numberOfDatasets < 1) {
            // R is being added
            R = &datasets.find(dataset.nickname)->second;
        } else {
            // S is being added
            S = &datasets.find(dataset.nickname)->second;
            // set the datatypecombination
            setDatatypeCombination();
        }

        numberOfDatasets++;
    }

    void updateDataspace() {
        for (auto &it: datasets) {
            dataspaceInfo.xMinGlobal = std::min(dataspaceInfo.xMinGlobal, it.second.dataspaceInfo.xMinGlobal);
            dataspaceInfo.yMinGlobal = std::min(dataspaceInfo.yMinGlobal, it.second.dataspaceInfo.yMinGlobal);
            dataspaceInfo.xMaxGlobal = std::max(dataspaceInfo.xMaxGlobal, it.second.dataspaceInfo.xMaxGlobal);
            dataspaceInfo.yMaxGlobal = std::max(dataspaceInfo.yMaxGlobal, it.second.dataspaceInfo.yMaxGlobal);
        }
        dataspaceInfo.xExtent = dataspaceInfo.xMaxGlobal - dataspaceInfo.xMinGlobal;
        dataspaceInfo.yExtent = dataspaceInfo.yMaxGlobal - dataspaceInfo.yMinGlobal;
    }
} DatasetInfoT;

typedef struct ApproximationInfo {
    spatial_lib::ApproximationTypeE type;   // sets which of the following fields will be used
    spatial_lib::AprilConfigT aprilConfig;  
    
    ApproximationInfo() {
        this->type = spatial_lib::AT_NONE;
    }

    ApproximationInfo(spatial_lib::ApproximationTypeE type) {
        this->type = type;
    }
} ApproximationInfoT;

typedef struct QueryInfo {
    spatial_lib::QueryTypeE type = spatial_lib::Q_NONE;
    int MBRFilter = 1;
    int IntermediateFilter = 1;
    int Refinement = 1;
} QueryInfoT;

typedef struct Config {
    DirectoryPathsT dirPaths;
    SystemOptionsT options;
    std::vector<ActionT> actions;
    PartitioningInfoT partitioningInfo;
    DatasetInfoT datasetInfo;
    ApproximationInfoT approximationInfo;
    QueryInfoT queryInfo;
} ConfigT;

/**
 * @brief global configuration variable 
*/
extern ConfigT g_config;


/**
 * @brief based on query type in config, it appropriately adds the query output in parallel
 * Used only in parallel sections for OpenMP
 */
void queryResultReductionFunc(spatial_lib::QueryOutputT &in, spatial_lib::QueryOutputT &out);

// Declare the parallel reduction
#pragma omp declare reduction \
    (query_output_reduction: spatial_lib::QueryOutputT: queryResultReductionFunc(omp_in, omp_out)) \
    initializer(omp_priv = spatial_lib::QueryOutput())



#endif