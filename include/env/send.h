#ifndef D_COMM_SEND_H
#define D_COMM_SEND_H

#include <mpi.h>
#include "def.h"
#include "proc.h"
#include "comm_def.h"

namespace comm
{
    namespace send
    {   

        DB_STATUS sendSingleIntMessage(int value, int destRank, int tag, MPI_Comm comm);
        
        /**
         * sends an instruction message to one specific node with destRank.
        */
        DB_STATUS sendInstructionMessage(int destRank, int tag, MPI_Comm comm);



        DB_STATUS sendGeometryMessage(MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack, int destRank, int tag, MPI_Comm comm);
    }

    namespace broadcast
    {
        /**
         * broadcasts an instruction message to all worker nodes.
         * messages are broadcasted parallely
        */
        DB_STATUS broadcastInstructionMessage(int tag);
    }

}


#endif