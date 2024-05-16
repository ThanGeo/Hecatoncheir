#ifndef D_CONFIG_SETUP_H
#define D_CONFIG_SETUP_H

#include "def.h"
#include "config/parse.h"
#include "config/configure.h"

namespace setup
{
    DB_STATUS setupSystem(int argc, char* argv[]);
}


#endif