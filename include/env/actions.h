#ifndef D_ACTIONS_H
#define D_ACTIONS_H

#include "def.h"
#include "partitioning.h"
#include "containers.h"

namespace actions
{
    /** @brief Initializes the environment termination process by broadcasting the appropriate message to all workers and the local agent. */
    DB_STATUS initTermination();
    /** @brief Performs all the necessary actions in the proper order, as parsed and stored in the configuration. */
    DB_STATUS performActions();
}




#endif 