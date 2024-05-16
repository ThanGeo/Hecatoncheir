#ifndef D_COMM_TASK_H
#define D_COMM_TASK_H

#include <mpi.h>

#include "comm.h"



namespace task
{
    namespace controller
    {
        /**
         * @brief An instruction message was probed, so based on the source rank and tag, perform the appropriate instruction.
         * The instruction message has not yet been received.
         * 
         * @param sourceRank sender's rank
         * @param tag message tag
         * @return DB_STATUS 
         */
        DB_STATUS performInstructionFromMessage(int sourceRank, int tag);
    }
}




#endif