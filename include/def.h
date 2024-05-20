#ifndef D_DEF_H
#define D_DEF_H

#include <unistd.h>
#include <iostream>
#include <vector>
#include <stdio.h>

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

/* STATUS AND ERROR CODES */
#define DBBASE 100000
typedef enum DB_STATUS {
    DBERR_OK = DBBASE + 0,
    DB_FIN,

    // general
    DBERR_MISSING_FILE = DBBASE + 1000,
    DBERR_CREATE_DIR = DBBASE + 1001,
    DBERR_UNKNOWN_ARGUMENT = DBBASE + 1002,

    // comm
    DBERR_COMM_RECV = DBBASE + 2000,
    DBERR_COMM_SEND = DBBASE + 2001,
    DBERR_COMM_BCAST = DBBASE + 2002,
    DBERR_COMM_GET_COUNT = DBBASE + 2003,
    DBERR_COMM_WRONG_MSG_FORMAT = DBBASE + 2004,
    DBERR_COMM_UNKNOWN_INSTR = DBBASE + 2005,
    DBERR_COMM_INVALID_MSG_TYPE = DBBASE + 2006,
    
    // processes
    DBERR_PROC_INIT_FAILED = DBBASE + 3000,
} DB_STATUS;

typedef struct DirectoryPaths {
    const std::string configFilePath = "../config.ini";
    const std::string datasetsConfigPath = "../datasets.ini";
    const std::string dataPath = "../data/";
    const std::string partitionsPath = "../data/partitions/";
    const std::string approximationPath = "../data/approximations/";
} DirectoryPathsT;

typedef struct Config {
    DirectoryPathsT dirPaths;
    SystemOptionsT options;
} ConfigT;

/**
 * @brief global configuration variable 
*/
extern ConfigT g_config;

namespace logger
{
    // Base case of the variadic template recursion
    static inline void print_args() {
        std::cout << std::endl;
    }

    // Recursive template function
    template<typename T, typename... Args>
    static inline void print_args(T first, Args... rest) {
        std::cout << first << " ";
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
            std::cout << YELLOW "[C" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
        } else {
            // controllers
            std::cout << YELLOW "[C" + std::to_string(g_node_rank) + "]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
        }
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
            std::cout << YELLOW "[C" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC GREEN "[SUCCESS]" NC ": ";
        } else {
            // controllers
            std::cout << YELLOW "[C" + std::to_string(g_node_rank) + "]" NC GREEN "[SUCCESS]" NC ": ";
        }
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
            std::cout << YELLOW "[C" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ": ";
        } else {
            // controllers
            std::cout << YELLOW "[C" + std::to_string(g_node_rank) + "]" NC ": ";
        }
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
                std::cout << YELLOW "[C" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ": ";
            } else {
                // controllers
                std::cout << YELLOW "[C" + std::to_string(g_node_rank) + "]" NC ": ";
            }
            print_args(first, rest...);
        }
    }

}


#endif