#ifndef D_COMM_RECV_H
#define D_COMM_RECV_H

#include <mpi.h>
#include "def.h"

namespace comm
{
    namespace recv
    {
        /**
         * @brief receives a single int message from senderRank with messageTag.
         * message contents are stored in the contents parameter. 
         * 
         */
        DB_STATUS receiveSingleIntMsg(int sourceRank, int messageTag, int &contents);

        /**
         * @brief receive an instruction message from sourceRank with tag.
         * Instruction messages are always of size 0 (empty) and their tag indicates which instruction to execute.
        */
        DB_STATUS receiveInstructionMsg(int sourceRank, int tag, MPI_Comm comm, MPI_Status &status);
    }

}


#endif