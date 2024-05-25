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
         * @brief sends a single polygon to destination with destRank, using tag 
         * The message is first packed, then sent using the appropriate amount of individual messages,
         * and then all pack memory is freed.
         */
        DB_STATUS sendPolygonToNode(spatial_lib::PolygonT &polygon, int partitionID, int destRank, int tag);

        /**
         * @brief sends a batch of objects to node with destRank, using tag
         * 
         */
        DB_STATUS sendGeometryBatchToNode(BatchT &batch, int destRank, int tag);

        /**
         * @brief Sends an instruction message with tag to the children (agent)
         * 
         * @param tag 
         * @return DB_STATUS 
         */
        DB_STATUS sendInstructionToAgent(int tag);

        /**
         * @brief Sends a batch to the controller's respective agent
         * 
         * @param batch 
         * @param tag 
         * @return DB_STATUS 
         */
        DB_STATUS sendGeometryBatchToAgent(BatchT &batch, int tag);
        DB_STATUS serializeAndSendGeometryBatchToAgent(GeometryBatchT &batch, int tag);

        DB_STATUS serializeAndSendGeometryBatchToNode(GeometryBatchT &batch, int destRank, int tag);

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