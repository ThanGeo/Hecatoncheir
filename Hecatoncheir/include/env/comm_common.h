#ifndef D_COMM_COMMON_H
#define D_COMM_COMMON_H

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

#include <unordered_set>
#include "omp.h"

namespace comm
{
    #define QUERY_BATCH_SIZE 1000

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

    namespace execute
    {
        /** @brief 
         * Performs the operations for preparing the system for a dataset partitioning.
         * @param msg Contains the received, serialized message with the prepare dataset info.
         * @return datasetID Returns the dataset ID for the prepared dataset
         */
        DB_STATUS prepareDataset(SerializedMsg<char> &msg, int &datasetID);

        /** @brief 
         * Builds the index for the dataset specified in the serialized msg.
         * @param msg Contains a serialized message specifying the indexing parameters.
         */
        DB_STATUS buildIndex(SerializedMsg<char> &msg);

        /** @brief
         * Unpacks a message containing a join query and executes it, storing the results in the queryResult object.
         */
        DB_STATUS joinQuery(SerializedMsg<char> &msg, std::unique_ptr<hec::QResultBase> &queryResult);

    }

}

#endif