#ifndef D_CONFIG_H
#define D_CONFIG_H

#include <sys/stat.h>

#include "def.h"
#include "containers.h"
#include "env/comm.h"

namespace configure
{
    /**
     * @brief verifies the required directories and/or creates any missing ones.
    */
    DB_STATUS verifySystemDirectories();
    /**
     * @brief creates configuration for the system based on the options
    */
    DB_STATUS createConfiguration(SystemOptionsT &sysOps);
}

#endif