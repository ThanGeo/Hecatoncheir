#ifndef D_CONFIG_SETUP_H
#define D_CONFIG_SETUP_H

#include "containers.h"
#include "config/parse.h"
#include "config/configure.h"

/** @brief System setup methods. */
namespace setup
{
    /** @brief Entryway method for setting up the system. */
    DB_STATUS setupSystem(int argc, char* argv[]);
}


#endif