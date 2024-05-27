#ifndef D_COMM_SEND_H
#define D_COMM_SEND_H

#include "def.h"
#include "proc.h"
#include "comm_def.h"

namespace comm
{
    namespace send
    {   

        DB_STATUS sendSingleIntMessage(int value, int destRank, int tag, MPI_Comm &comm);
        
        /**
         * sends an instruction message to one specific node with destRank.
        */
        DB_STATUS sendInstructionMessage(int destRank, int tag, MPI_Comm &comm);

        DB_STATUS sendDatasetInfoMessage(SerializedMsgT<char> &datasetInfoPack, int destRank, int tag, MPI_Comm &comm);
        
        template <typename T>
        DB_STATUS sendSerializedMessage(SerializedMsgT<T> &msg, int destRank, int tag, MPI_Comm &comm) {
            // send the serialized message
            int mpi_ret = MPI_Send(msg.data, msg.count, msg.type, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Sending serialized message failed.");
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }
    }

    namespace broadcast
    {
        /**
         * broadcasts an instruction message to all worker nodes.
         * messages are broadcasted parallely
        */
        DB_STATUS broadcastInstructionMessage(int tag);


        /**
         * @brief broadcasts the dataset info to all worker nodes
         * messages are broadcasted parallely
         * @param msgPack 
         * @return DB_STATUS 
         */
        DB_STATUS broadcastDatasetInfo(SerializedMsgT<char> &msgPack);
    }

}


#endif