#ifndef D_CONFIG_H
#define D_CONFIG_H

#include <sys/stat.h>

#include "def.h"
#include "containers.h"
#include "env/comm.h"
#include "statement.h"
#include "parse.h"

namespace configure
{
    /**
     * @brief initialize MPI environment
     * 
     * @return DB_STATUS 
     */
    DB_STATUS initMPI(int argc, char* argv[]);

    /**
     * @brief verifies the required directories and/or creates any missing ones.
    */
    DB_STATUS verifySystemDirectories();
    /**
     * @brief creates configuration for the system based on the options
    */
    DB_STATUS createConfiguration();


    DB_STATUS setDatasetInfo(DatasetStatementT* datasetStmt);
}

#endif