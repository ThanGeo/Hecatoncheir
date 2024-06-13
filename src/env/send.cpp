#include "env/send.h"

namespace comm
{
    namespace send
    {
        DB_STATUS sendResponse(int destRank, int tag, MPI_Comm &comm) {
            if (tag != MSG_ACK && tag != MSG_NACK) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Response tag must by either ACK or NACK");
                return DBERR_INVALID_PARAMETER;
            }
            int mpi_ret = MPI_Send(NULL, 0, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send response failed. Tag:", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }

        DB_STATUS sendInstructionMessage(int destRank, int tag, MPI_Comm &comm) {
            // check tag validity
            if (tag < MSG_INSTR_BEGIN || tag >= MSG_INSTR_END) {
                logger::log_error(DBERR_COMM_WRONG_MSG_FORMAT, "Instruction messages must have an instruction tag. Current tag", tag);
                return DBERR_COMM_WRONG_MSG_FORMAT;
            }
            // send the message
            int mpi_ret = MPI_Send(NULL, 0, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send message with tag", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }

        DB_STATUS sendDatasetInfoMessage(SerializedMsgT<char> &datasetInfoMsg, int destRank, int tag, MPI_Comm &comm) {
            // check tag validity
            if (tag != MSG_DATASET_INFO) {
                logger::log_error(DBERR_COMM_WRONG_MSG_FORMAT, "Dataset info messages must have the appropriate tag. Current tag", tag);
                return DBERR_COMM_WRONG_MSG_FORMAT;
            }
            // send the message
            int mpi_ret = MPI_Send(datasetInfoMsg.data, datasetInfoMsg.count, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send message with tag", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }
        
    }

    namespace broadcast
    {
        DB_STATUS broadcastInstructionMessage(int tag) {
            DB_STATUS ret = DBERR_OK;
            // broadcast to all other controllers parallely
            #pragma omp parallel
            {
                DB_STATUS local_ret = DBERR_OK;
                #pragma omp for
                for(int i=0; i<g_world_size; i++) {
                    if (i != g_host_rank) {
                        // send to workers
                        local_ret = send::sendInstructionMessage(i, tag, g_global_comm);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        } 
                    } else {
                        // send to local agent
                        local_ret = send::sendInstructionMessage(AGENT_RANK, tag, g_local_comm);
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