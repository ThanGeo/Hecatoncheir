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

#define RED "\e[0;31m"
#define GREEN "\e[0;32m"
#define YELLOW "\e[0;33m"
#define BLUE "\e[0;34m"
#define PURPLE "\e[0;35m"
#define NC "\e[0m"

#define AGENT_PROGRAM_NAME "./agent"

#define HOST_RANK 0
#define PARENT_RANK 0
#define PARENTLESS_RANK -1
#define AGENT_RANK 0

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

    // comm
    DBERR_COMM_RECV = DBBASE + 2000,
    DBERR_COMM_SEND = DBBASE + 2001,
    DBERR_COMM_BCAST = DBBASE + 2002,
    DBERR_COMM_GET_COUNT = DBBASE + 2003,
    DBERR_COMM_WRONG_MSG_FORMAT = DBBASE + 2004,
    DBERR_COMM_UNKNOWN_INSTR = DBBASE + 2005,
    DBERR_COMM_INVALID_MSG_TYPE = DBBASE + 2006,
    DBERR_COMM_WRONG_PACK_ORDER = DBBASE + 2007,
    DBERR_COMM_PROBE_FAILED = DBBASE + 2008,
    
    // processes/mpi
    DBERR_MPI_INIT_FAILED = DBBASE + 3000,
    DBERR_PROC_INIT_FAILED = DBBASE + 3001,

    // data
    DBERR_INVALID_DATATYPE = DBBASE + 4000,
    DBERR_INVALID_FILETYPE = DBBASE + 4001,
    DBERR_UNSUPPORTED_DATATYPE_COMBINATION = DBBASE + 4002,

    // partitioning
    DBERR_INVALID_PARTITION = DBBASE + 5000,
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
            std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
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
            std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC GREEN "[SUCCESS]" NC ": ";
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
            std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC ": ";
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
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC ": ";
            }
            std::cout.flush();
            print_args(first, rest...);
        }
    }

}

typedef struct DirectoryPaths {
    const std::string configFilePath = "../config.ini";
    const std::string datasetsConfigPath = "../datasets.ini";
    const std::string dataPath = "../data/";
    const std::string partitionsPath = "../data/partitions/";
    const std::string approximationPath = "../data/approximations/";
} DirectoryPathsT;

typedef enum ActionType {
    ACTION_PERFORM_PARTITIONING,
    ACTION_CREATE_APRIL,
    ACTION_PERFORM_VERIFICATION,
} ActionTypeE;

typedef enum PartitioningType {
    PARTITIONING_ROUND_ROBIN,
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
    std::vector<spatial_lib::DatasetT> datasets;
    spatial_lib::DataspaceInfoT dataspaceInfo;
    
    /**
     * @brief Returns a pointer to the requested dataset object.
     * R is 0, S is 1
     * 
     * @param idx 
     * @return spatial_lib::DatasetT* 
     */
    spatial_lib::DatasetT* getDatasetByIdx(int idx) {
        if (idx >= numberOfDatasets) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Cant retrieve dataset in position", idx, "Total datasets:",numberOfDatasets);
            return nullptr;
        }
        return &datasets[idx];
    }

    void addDataset(spatial_lib::DatasetT dataset) {
        datasets.emplace_back(dataset);
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

typedef struct Batch {
    int nodeRank;       // to whom it is destined for
    int size;           // how many objects it contains
    int maxSize;        // maximum capacity in terms of objects
    /**
     * @brief info pack for a geometry batch
     * |OBJECT COUNT|FINAL MSG FLAG|REC ID|PARTITION ID|VERTEX COUNT|REC ID|PARTITION ID|VERTEX COUNT|...|
     * 
     */
    std::vector<int> infoPack;
    /**
     * @brief coords pack for a geometry batch
     * |X|Y|X|Y|...|
     * 
     */
    std::vector<double> coordsPack;

    void updateInfoPackObjectCount(int objectCount) {
        infoPack.at(0) = objectCount;
    }

    void updateInfoPackFlag(int flag) {
        infoPack.at(1) = flag;
    }

    void addObjectToBatch(int recID, int partitionID, int vertexCount, std::vector<double> &coords) {
        infoPack.emplace_back(recID);
        infoPack.emplace_back(partitionID);
        infoPack.emplace_back(vertexCount);
        coordsPack.insert(std::end(coordsPack), std::begin(coords), std::end(coords));
        size += 1;
        updateInfoPackObjectCount(size);
    }

    void clearBatch(int flag) {
        infoPack.clear();
        infoPack.emplace_back(0);       // geometries count
        infoPack.emplace_back(flag);    // final message flag 
        coordsPack.clear();
        size = 0;
    }

} BatchT;

/**
 * @brief global configuration variable 
*/
extern ConfigT g_config;




#endif