#ifndef D_COMM_RECV_H
#define D_COMM_RECV_H

#include "containers.h"

/** @brief System inter- and intra- communication methods. */
namespace comm
{
    /** @brief Methods for receiving inbound messages. */
    namespace recv
    {
        /** @brief Receives a response message. 
         * @param[in] sourceRank The rank of the message's source.
         * @param[in] sourceTag The tag of the message to receive.
         * @param[in] comm The communicator through which the message will received from.
         * @param[out] status Stores the receive operation's status results.
        */
        DB_STATUS receiveResponse(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status);

        /**
        @brief Receives an instruction message.
         * @param[in] sourceRank The rank of the message's source.
         * @param[in] sourceTag The tag of the message to receive.
         * @param[in] comm The communicator through which the message will received from.
         * @param[out] status Stores the receive operation's status results.
        */
        DB_STATUS receiveInstructionMessage(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status);
        
        /**
        @brief Receives an already probed message.
         * the msg MPI datatype parameter must already be set (constructor handles that)
         * @param[in] status Contains the probed message's status metadata.
         * @param[in] dataType The message's data type.
         * @param[in] comm The communicator through which the message should be received.
         * @param[out] msg Where the message will be stored after received successfully.
         * @note The msg data buffer is allocated by this function. The caller is responsible for freeing this memory
         */
        template <typename T> 
        DB_STATUS receiveMessage(MPI_Status &status, MPI_Datatype dataType, MPI_Comm &comm, SerializedMsg<T> &msg) {
            DB_STATUS ret = DBERR_OK;
            // get message size 
            int mpi_ret = MPI_Get_count(&status, dataType, &msg.count);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_GET_COUNT, "Failed when trying to get the message size");
                return DBERR_COMM_GET_COUNT;
            }

            // // allocate memory
            msg.data = (T*) malloc(msg.count * sizeof(T));

            // // receive the message
            mpi_ret = MPI_Recv(msg.data, msg.count, dataType, status.MPI_SOURCE, status.MPI_TAG, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive msg pack with tag", status.MPI_TAG);
                return DBERR_COMM_RECV;
            }

            return ret;
        }
    }

}


#endif