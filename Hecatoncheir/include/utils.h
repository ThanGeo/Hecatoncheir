#ifndef D_UTILS_H
#define D_UTILS_H

#include <dirent.h>
#include <string>
#include <sstream>
#include <vector>
#include <iostream>

#include <mpi.h>

#include "def.h"

/** @brief Verifies that the file at the specified filepath exists on disk.
 * @param filePath Can be relative or absolute path. Must point to a file on disk.
 * 
 * Will return false if the file does not exists or cannot be opened (e.g. no access rights)
 */
DB_STATUS verifyFilepath(std::string filePath);
/** @brief Verifies that the directory at the specified path exists on disk.
 * @param Can be relative or absolute path. Must point to a directory on disk.
 * 
 * Will return false if the directory does not exists or cannot be opened (e.g. no access rights)
 */
DB_STATUS verifyDirectory(std::string directoryPath);
/** @brief Strips a file path to get the file's name (no file extension) 
 * 
*/
std::string getFileNameFromPath(std::string &filePath);
/** @brief Performs binary search on a vector of intervals for the given value.
 * @param vec A vector of consecutive uint32 values that represent intervals in pairs.
 * @param x The uint32 number to look for.
 * 
 * If the value exists inside any of the [start,end) intervals, it returns true.
 */
bool binarySearchInIntervalVector(std::vector<uint32_t> &vec, uint32_t x);
/** @brief Performs binary search on a vector numbers for the given value.
 * @param vec A vector of uint32 numbers.
 * @param x The uint32 number to look for.
 * 
 */
bool binarySearchInVector(std::vector<uint32_t> &vec, uint32_t &x);

/** @brief Hilbert curve related methods. */
namespace hilbert
{
    /**
    @brief Convert a cell's x,y indices to its Hilbert order.
     */
    uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y);
    /**
    @brief Convert a Hilbert order to its cell indices x,y.
     */
    void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y);
}

/** @brief Timing methods for MPI apps.*/
namespace mpi_timer
{
    /**
    @brief get the timestamp of the current time
     * 
     * @return double 
     */
    double markTime();

    /**
    @brief Get the elapsed time in seconds, from the given start timestamp.
     * 
     * @param startTime The timestamp to calculate the elapsed time from.
     * @return double The elapsed time from the given start time.
     */
    double getElapsedTime(double startTime);
}

/** @brief Logging methods for the system. Logs are printed in terminal. */
namespace logger
{
    /** @brief Base case of the variadic template recursion. */
    static inline void print_args() {
        std::cout << std::endl;
        std::cout.flush();
    }

    /** @brief Recursive template function. */
    template<typename T, typename... Args>
    static inline void print_args(T first, Args... rest) {
        std::cout << first << " ";
        std::cout.flush();
        print_args(rest...);
    }

    /** @brief Error logging function with template arguments. Separates input parameters with spaces. 
     * Prints the node's rank and process type (Agent or Controller), coloured appropriately. */
    template<typename T, typename... Args>
    inline void log_error(int errorCode, T first, Args... rest) {
        if (g_proc_type == AGENT) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
        } else if (g_proc_type == CONTROLLER) {
            // controllers
            if (g_node_rank == HOST_LOCAL_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";
            }
        } else {
            std::cout << NAVY "[D]" NC RED "[ERROR: " + std::to_string(errorCode) + "]" NC ": ";

        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /** @brief Warning logging function with template arguments. Separates input parameters with spaces. 
     * Prints the node's rank and process type (Agent or Controller), coloured appropriately. */
    template<typename T, typename... Args>
    inline void log_warning(T first, Args... rest) {
        if (g_proc_type == AGENT) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ORANGE "[WARNING]" NC ": ";
        } else if (g_proc_type == CONTROLLER) {
            // controllers
            if (g_node_rank == HOST_LOCAL_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC ORANGE "[WARNING]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC ORANGE "[WARNING]" NC ": ";
            }
        } else {
            std::cout << NAVY "[D]" NC ORANGE "[WARNING]" NC ": ";
        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /** @brief Success logging function with template arguments. Separates input parameters with spaces. 
     * Prints the node's rank and process type (Agent or Controller), coloured appropriately. */
    template<typename T, typename... Args>
    inline void log_success(T first, Args... rest) {
        if (g_proc_type == AGENT) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC GREEN "[SUCCESS]" NC ": ";
        } else if (g_proc_type == CONTROLLER) {
            // controllers
            if (g_node_rank == HOST_LOCAL_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC GREEN "[SUCCESS]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC GREEN "[SUCCESS]" NC ": ";
            }
        } else {
            std::cout << NAVY "[D]" NC GREEN "[SUCCESS]" NC ": ";
        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /** @brief Task logging function with template arguments. Separates input parameters with spaces. 
     * Prints the node's rank and process type (Agent or Controller), coloured appropriately. */
    template<typename T, typename... Args>
    inline void log_task(T first, Args... rest) {
        if (g_proc_type == AGENT) {
            // agents
            std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ": ";
        } else if (g_proc_type == CONTROLLER) {
            // controllers
            if (g_node_rank == HOST_LOCAL_RANK) {
                // host controller
                std::cout << PURPLE "[" + std::to_string(g_node_rank) + "]" "[C]" NC ": ";
            } else {
                // worker controller
                std::cout << YELLOW "[" + std::to_string(g_node_rank) + "]" NC PURPLE "[C]" NC ": ";
            }
        } else {
            std::cout << NAVY "[D]" NC ": ";
        }
        std::cout.flush();
        print_args(first, rest...);
    }

    /** @brief Task logging function with template arguments for a specific node. 
     * Separates input parameters with spaces. 
     * Prints the node's rank and process type (Agent or Controller), coloured appropriately. 
     * 
     * @param rank The rank of the node that will log the given task message. */
    template<typename T, typename... Args>
    inline void log_task_single_node(int rank, T first, Args... rest) {
        if (g_parent_original_rank == rank) {
            if (g_proc_type == AGENT) {
                // agents
                std::cout << YELLOW "[" + std::to_string(g_parent_original_rank) + "]" NC BLUE "[A]" NC ": ";
            } else {
                // controllers
                if (g_node_rank == HOST_LOCAL_RANK) { 
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

/** @brief Mapping functions from int to string and back, for printing/parsing purposes. */
namespace mapping
{
    extern std::string actionIntToStr(ActionType action);
    std::string twoLayerClassIntToStr(TwoLayerClass classType);
    extern std::string queryTypeIntToStr(QueryType val);
    extern QueryType queryTypeStrToInt(std::string &str);
    extern std::string dataTypeIntToStr(DataType val);
    extern DataType dataTypeTextToInt(std::string str);
    extern FileType fileTypeTextToInt(std::string str);
    extern std::string relationIntToStr(int relation);
}

#endif