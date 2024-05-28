#ifndef D_COMM_H
#define D_COMM_H

#include "SpatialLib.h"
#include "def.h"
#include "proc.h"
#include "env/recv.h"
#include "env/send.h"
#include "config/configure.h"
#include "pack.h"
#include "env/partitioning.h"

namespace comm 
{
    namespace agent
    {
        /**
         * @brief listens for messages from its parent controller
         * 
         * @return DB_STATUS 
         */
        DB_STATUS listen();
    }


    namespace controller
    {   
        /**
         * @brief Serializes a geometry batch and sends it to destrank, with tag through comm
         * 
         * @param batch 
         * @param destRank 
         * @param tag 
         * @param comm 
         * @return DB_STATUS 
         */
        DB_STATUS serializeAndSendGeometryBatch(GeometryBatchT &batch, int destRank, int tag, MPI_Comm &comm);

        /**
         * @brief Sends an instruction message with tag to the children (agent)
         * 
         * @param tag 
         * @return DB_STATUS 
         */
        DB_STATUS sendInstructionToAgent(int tag);

        /**
         * @brief Packs and sends the dataset info to all worker nodes
         * 
         * @param dataset 
         * @param destRank 
         * @return DB_STATUS 
         */
        DB_STATUS broadcastDatasetInfo(spatial_lib::DatasetT* dataset);
        

        /**
         * @brief listens (probes) for messages from other controllers and the local agent 
         * 
         * @return DB_STATUS 
         */
        DB_STATUS listen();
    }

    
}



#endif