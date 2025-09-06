#ifndef D_CONFIG_H
#define D_CONFIG_H

#include <sys/stat.h>

#include "containers.h"
#include "statement.h"
#include "parse.h"

/** @brief System configuration methods. */
namespace configurer
{
    /**
    @brief Initializes the system's MPI environment.
     * @param[in] argc Number of user input arguments.
     * @param[in] argv User input arguments. Contain information about how many nodes to initialize.
     */
    DB_STATUS initMPIController(int argc, char* argv[]);
    DB_STATUS initMPIAgent(int argc, char* argv[]);
    DB_STATUS initWorker(int argc, char* argv[]);

    /** @brief Verifies the required directories for the system and/or creates any missing ones. */
    DB_STATUS verifySystemDirectories();

    /** @brief Configures the system based on the parsed options. */
    DB_STATUS createConfiguration();

    /** @brief Configures the specified dataset metadata from the input dataset statement. 
     * @param[in] datasetStmt Contains all dataset related parsed metadata.
    */
    DB_STATUS setDatasetMetadata(DatasetStatement* datasetStmt);
}

#endif