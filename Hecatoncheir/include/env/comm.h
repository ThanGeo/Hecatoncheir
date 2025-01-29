#ifndef D_COMM_H
#define D_COMM_H


#include "containers.h"
#include "config/configure.h"
#include "env/recv.h"
#include "env/send.h"
#include "env/partitioning.h"
#include "env/pack.h"
#include "storage/write.h"
#include "storage/utils.h"
#include "APRIL/generate.h"
#include "TwoLayer/filter.h"

#include <omp.h>

/** @brief System inter- and intra- communication methods. */
namespace comm 
{
    /**
     * @brief Probes for a message in a communicator (blocking).
     * @param sourceRank Source rank to probe.
     * @param tag MPI tag.
     * @param comm Communicator to probe.
     * @param status MPI status.
     * @return DB_STATUS Error or success status.
     */
    DB_STATUS probe(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status);
    /**
     * @brief Probes for a message in a communicator (non-blocking).
     * @param sourceRank Source rank to probe.
     * @param tag MPI tag.
     * @param comm Communicator to probe.
     * @param status MPI status.
     * @param messageExists set to true if a message with the specification was probed to exist
     * @return DB_STATUS Error or success status.
     */
    DB_STATUS probe(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status, int &messageExists);

    /** @brief The agents' communication methods. */
    namespace agent
    {
        /** @brief Listen for messages from the parent controller. */
        DB_STATUS listen();
    }

    /**  @brief The controllers' communication methods. */
    namespace controller
    {   
        /**
        @brief Serializes a geometry batch and sends it to the appropriate ranks.
         * All the information is stored in the batch.
         */
        DB_STATUS serializeAndSendGeometryBatch(Batch* batch);

        DB_STATUS serializeAndSendGeometry(Shape *shape, int destRank, MPI_Comm &comm);

        /** @brief Broadcasts the configured system options to all participants. */
        DB_STATUS broadcastSysMetadata();

        /** @brief The controller listens (probes) for inbound messages from other controllers or the local agent.*/
        DB_STATUS listen();
        
    }
    
    /** @brief The host controller's communication methods. */
    namespace host
    {
        /** @brief Packs and broadcasts the dataset metadata to all worker nodes. */
        DB_STATUS broadcastDatasetMetadata(Dataset* dataset);

        /** @brief Gathers responses (ACK/NACK) from workers or the local agent about the last instructed action/job. */
        DB_STATUS gatherResponses();

        /** @brief Gathers query results by the workers and the local agent. */
        DB_STATUS gatherResults();

        /** @brief The host controller listens (probes) for inbound messages from the driver.*/
        DB_STATUS listen();
    }


}



#endif