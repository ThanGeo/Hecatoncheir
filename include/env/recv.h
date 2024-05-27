#ifndef D_COMM_RECV_H
#define D_COMM_RECV_H

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
        DB_STATUS receiveSingleIntMessage(int sourceRank, int messageTag, int &contents);

        /**
         * @brief receive an instruction message from sourceRank with tag.
         * Instruction messages are always of size 0 (empty) and their tag indicates which instruction to execute.
        */
        DB_STATUS receiveInstructionMessage(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status);
        
        /**
         * @brief receives a single message pack of type T
         * the msgPack MPI datatype parameter must already be set (constructor handles that)
         * the msgPack data buffer is allocated by this function. The caller is responsible for freeing this memory
         * 
         * @tparam T 
         * @param status 
         * @param comm 
         * @param msgPack 
         * @return DB_STATUS 
         */
        template <typename T> 
        DB_STATUS receiveMessagePack(MPI_Status &status, MPI_Datatype dataType, MPI_Comm &comm, SerializedMsgT<T> &msgPack) {
            DB_STATUS ret = DBERR_OK;
            // get message size 
            int mpi_ret = MPI_Get_count(&status, dataType, &msgPack.count);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_GET_COUNT, "Failed when trying to get the message size");
                return DBERR_COMM_GET_COUNT;
            }

            // // allocate memory
            msgPack.data = (T*) malloc(msgPack.count * sizeof(T));

            // // receive the message
            mpi_ret = MPI_Recv(msgPack.data, msgPack.count, dataType, status.MPI_SOURCE, status.MPI_TAG, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive msg pack with tag", status.MPI_TAG);
                return DBERR_COMM_RECV;
            }

            return ret;
        }

        /**
         * @brief receives a serialized message with sourceTag from sourceRank
         * the buffer must be allocated and freed by the caller
        */
        template <typename T>
        DB_STATUS receiveSerializedMessage(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status, SerializedMsgT<T> &msg) {
            int mpi_ret = MPI_Recv(msg.data, msg.count, msg.type, sourceRank, sourceTag, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive serialized message");
                return DBERR_COMM_RECV;
            }
            return DBERR_OK;
        }
        
    }

}


#endif