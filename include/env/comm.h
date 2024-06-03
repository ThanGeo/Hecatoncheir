#ifndef D_COMM_H
#define D_COMM_H

#include "SpatialLib.h"
#include "def.h"
#include "config/configure.h"
#include "env/recv.h"
#include "env/send.h"
#include "env/partitioning.h"
#include "env/pack.h"
#include "storage/write.h"
#include "storage/utils.h"

#include <omp.h>

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
         * @brief Serializes a geometry batch and sends it to its assigned destination rank, with tag through comm
         * The batch must contain all of this information
         * @param batch 
         * @param destRank 
         * @param tag 
         * @param comm 
         * @return DB_STATUS 
         */
        DB_STATUS serializeAndSendGeometryBatch(GeometryBatchT* batch);

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

        DB_STATUS broadcastSysInfo();
        

        /**
         * @brief listens (probes) for messages from other controllers and the local agent 
         * 
         * @return DB_STATUS 
         */
        DB_STATUS listen();

        DB_STATUS gatherResponses();
    }

    
}



#endif