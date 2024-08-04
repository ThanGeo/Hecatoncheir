#ifndef D_UTILS_H
#define D_UTILS_H

#include <dirent.h>
#include <string>
#include <sstream>
#include <vector>
#include <iostream>

#include <mpi.h>

#include "def.h"

bool verifyFilepath(std::string filePath);
bool verifyDirectory(std::string directoryPath);
std::string getDatasetNameFromPath(std::string &datasetPath);
bool binarySearchInIntervalVector(std::vector<uint32_t> &vec, uint32_t x);
bool binarySearchInVector(std::vector<uint32_t> &vec, uint32_t &x);


namespace hilbert
{
    /**
     * Hilbert order: convert x,y cell indices to hilbert order d
     */
    uint32_t xy2d (uint32_t n, uint32_t x, uint32_t y);
    /**
     * Hilbert order: convert order d to x,y cell indices
     */
    void d2xy(uint32_t n, uint32_t d, uint32_t &x, uint32_t &y);
}

namespace mpi_timer
{
    /**
     * @brief get the timestamp of the current time
     * 
     * @return double 
     */
    double markTime();

    /**
     * @brief Get the elapsed time in seconds, from the given start timestamp
     * 
     * @param startTime 
     * @return double 
     */
    double getElapsedTime(double startTime);
}

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