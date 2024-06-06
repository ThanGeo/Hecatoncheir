#ifndef D_UTILS_H
#define D_UTILS_H

#include <dirent.h>
#include <string>
#include <sstream>

#include <mpi.h>

bool verifyFilepath(std::string filePath);
bool verifyDirectory(std::string directoryPath);
std::string getDatasetNameFromPath(std::string &datasetPath);

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

#endif