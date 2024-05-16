#ifndef D_COMM_H
#define D_COMM_H

#include <mpi.h>


#include "def.h"
#include "proc.h"
#include "env/recv.h"
#include "env/send.h"
#include "config/configure.h"
#include "task.h"

namespace comm 
{

    namespace controller
    {   
        /**
         * @brief Sends an instruction message with tag to the children (agent)
         * 
         * @param tag 
         * @return DB_STATUS 
         */
        DB_STATUS SendInstructionMessageToAgent(int tag);

        /**
         * @brief receives an instruction message and sends it to all children processes
         * 
         * @param sourceRank 
         * @param sourceTag 
         * @param sourceComm 
         * @return DB_STATUS 
         */
        DB_STATUS forwardInstructionMsgToAgent(int sourceRank, int sourceTag, MPI_Comm sourceComm);

        /**
         * @brief performs the instruction included in the status variable
         * 
         */
        DB_STATUS performInstructionFromMessage(MPI_Status &status);

        /**
         * @brief listens for messages from other controllers and this node's agent 
         * 
         * @return DB_STATUS 
         */
        DB_STATUS listen();
    }

    namespace agent
    {
        /**
         * @brief listens for messages from its parent controller
         * 
         * @return DB_STATUS 
         */
        DB_STATUS listen();
    }
}



#endif