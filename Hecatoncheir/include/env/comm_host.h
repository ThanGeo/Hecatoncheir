#ifndef D_COMM_HOST_H
#define D_COMM_HOST_H

#include "containers.h"

namespace comm
{
    namespace host
    {
        /** @brief Listen for messages from the driver (server standing by).*/
        DB_STATUS listen();

        /** @brief Broadcasts the configured system options to all participants. */
        DB_STATUS broadcastSysMetadata();

        /** @brief Gathers responses (ACK/NACK) from workers or the local agent about the last instructed action/job. */
        DB_STATUS gatherResponses();
    }
}


#endif