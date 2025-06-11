#ifndef D_COMM_SEND_H
#define D_COMM_SEND_H

#include "containers.h"
#include "proc.h"
#include "comm_def.h"

/** @brief System inter- and intra- communication methods. */
namespace comm
{
    /** @brief Methods for sending messages. 
     * @todo Merge some methods like sendResponse and sendInstruction message (or even more) into a single one for multi-use. */
    namespace send
    {   
        /** @brief Sends a response that is either ACK or NACK.
         * @param[in] destRank The rank of the destination of the message.
         * @param[in] tag The tag of the message.
         * @param[in] comm The communicator through which the message will be sent through.
         */
        DB_STATUS sendResponse(int destRank, int tag, MPI_Comm &comm);

        /** @brief Sends a query result.
         * @param[in] result The result to be sent.
         * @param[in] destRank The rank of the destination of the message.
         * @param[in] tag The tag of the message.
         * @param[in] comm The communicator through which the message will be sent through.
         */
        DB_STATUS sendResult(unsigned long long result, int destRank, int tag, MPI_Comm &comm);
        
        /** @brief Sends an instruction message.
         * @param[in] destRank The rank of the destination of the message.
         * @param[in] tag The tag of the message.
         * @param[in] comm The communicator through which the message will be sent through.
         */
        DB_STATUS sendInstructionMessage(int destRank, int tag, MPI_Comm &comm);
        
        /** @brief Sends a message containing dataset metadata.
         * @param[in] datasetMetadataMsg The already serialized message.
         * @param[in] destRank The rank of the destination of the message.
         * @param[in] tag The tag of the message.
         * @param[in] comm The communicator through which the message will be sent through.
         */
        // DB_STATUS sendDatasetMetadataMessage(SerializedMsg<char> &datasetMetadataMsg, int destRank, int tag, MPI_Comm &comm);
        

        /** @brief Sends a serialized message of template type T.
         * @param[in] msg The already serialized message.
         * @param[in] destRank The rank of the destination of the message.
         * @param[in] tag The tag of the message.
         * @param[in] comm The communicator through which the message will be sent through.
         */
        template <typename T>
        DB_STATUS sendMessage(SerializedMsg<T> &msg, int destRank, int tag, MPI_Comm &comm) {
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
        @brief broadcasts a serialized/packed message with the given tag to all worker nodes.
         * Sending the same message to the agent must be handled separately
         * 
         * @tparam T 
         * @param msg 
         * @param tag 
         * @return DB_STATUS 
         */
        template <typename T>
        DB_STATUS broadcastMessage(SerializedMsg<T> &msg, int tag) {
            DB_STATUS ret = DBERR_OK;
            // broadcast to all other controllers parallely
            #pragma omp parallel num_threads(MAX_THREADS)
            {
                DB_STATUS local_ret = DBERR_OK;
                #pragma omp for
                for(int i=0; i<g_world_size; i++) {
                    if (i != HOST_LOCAL_RANK) {
                        // send to controller i
                        local_ret = send::sendMessage(msg, i, tag, g_controller_comm);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        } 
                    } else {
                        // send to local agent
                        ret = comm::send::sendMessage(msg, AGENT_RANK, tag, g_agent_comm);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
            }
            return ret;
        }
    }

}


#endif