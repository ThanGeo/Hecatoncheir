#ifndef D_COMM_WORKER_H
#define D_COMM_WORKER_H

#include "containers.h"

namespace comm
{
    namespace worker
    {
        /** @brief Listen for messages from the host worker. */
        DB_STATUS listen();

        /**
        @brief Serializes a geometry batch and sends it to the appropriate ranks.
         * All the information is stored in the batch.
         */
        DB_STATUS serializeAndSendGeometryBatch(Batch* batch);

    }
}

#endif