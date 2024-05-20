#ifndef D_COMM_H
#define D_COMM_H

#include "SpatialLib.h"
#include "def.h"
#include "proc.h"
#include "env/recv.h"
#include "env/send.h"
#include "config/configure.h"
#include "task.h"
#include "pack.h"

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
         * @brief sends a single polygon to destination with destRank, using tag and comm
         * The message is first packed, then sent using the appropriate amount of individual messages,
         * and then all pack memory is freed.
         */
        DB_STATUS sendPolygonToNode(spatial_lib::PolygonT &polygon, int partitionID, int destRank, int tag, MPI_Comm comm);

        /**
         * @brief Sends an instruction message with tag to the children (agent)
         * 
         * @param tag 
         * @return DB_STATUS 
         */
        DB_STATUS sendInstructionMessageToAgent(int tag);

        /**
         * @brief receive an instruction message from a controller (usually host) ands send it to the agent process
         * 
         * @param sourceRank 
         * @param sourceTag 
         * @param sourceComm 
         * @return DB_STATUS 
         */
        DB_STATUS forwardInstructionMsgToAgent(MPI_Status status);

        /**
         * @brief listens for messages from other controllers and this node's agent 
         * 
         * @return DB_STATUS 
         */
        DB_STATUS listen();
    }

    
}



#endif