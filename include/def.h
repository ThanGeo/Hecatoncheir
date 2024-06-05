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
    ACTION_PERFORM_PARTITIONING,
    ACTION_CREATE_APRIL,
    ACTION_PERFORM_VERIFICATION,
} ActionTypeE;

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

    Action(ActionTypeE type) {
        this->type = type;
    }
    
} ActionT;

typedef struct DatasetInfo {
    int numberOfDatasets;
    std::unordered_map<std::string,spatial_lib::DatasetT> datasets;
    spatial_lib::DataspaceInfoT dataspaceInfo;
    
    spatial_lib::DatasetT* getDatasetByNickname(std::string &nickname) {
        auto it = datasets.find(nickname);
        if (it == datasets.end()) {
            return nullptr;
        }        
        return &it->second;
    }

    void addDataset(spatial_lib::DatasetT dataset) {
        datasets.insert(std::make_pair(dataset.nickname,dataset));
        numberOfDatasets++;
    }

} DatasetInfoT;

typedef struct Config {
    DirectoryPathsT dirPaths;
    SystemOptionsT options;
    std::vector<ActionT> actions;
    PartitioningInfoT partitioningInfo;
    DatasetInfoT datasetInfo;
} ConfigT;

/**
 * @brief global configuration variable 
*/
extern ConfigT g_config;




#endif