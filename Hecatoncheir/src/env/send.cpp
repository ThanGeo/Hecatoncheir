#include "env/send.h"

namespace comm
{
    namespace send
    {
        DB_STATUS sendResponse(int destRank, int tag, MPI_Comm &comm) {
            if (tag != MSG_ACK && tag != MSG_NACK) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Response tag must be either ACK or NACK");
                return DBERR_COMM_INVALID_MSG_TAG;
            }
            int mpi_ret = MPI_Send(NULL, 0, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send response failed. Tag:", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }

        DB_STATUS sendResult(unsigned long long result, int destRank, int tag, MPI_Comm &comm) {
            if (tag != MSG_QUERY_RESULT) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Inappropriate tag for query result message. Current tag:", tag);
                return DBERR_COMM_INVALID_MSG_TAG;
            }
            int mpi_ret = MPI_Send(&result, 1, MPI_UNSIGNED_LONG_LONG, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send result failed. Tag:", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }

        DB_STATUS sendInstructionMessage(int destRank, int tag, MPI_Comm &comm) {
            // check tag validity
            if (tag < MSG_INSTR_BEGIN || tag >= MSG_INSTR_END) {
                logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Instruction messages must have an instruction tag. Current tag", tag);
                return DBERR_COMM_INVALID_MSG_TAG;
            }
            // send the message
            int mpi_ret = MPI_Send(NULL, 0, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send message with tag", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }

        DB_STATUS sendDatasetMetadataMessage(SerializedMsg<char> &datasetMetadataMsg, int destRank, int tag, MPI_Comm &comm) {
            // check tag validity
            if (tag != MSG_DATASET_METADATA) {
                logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Dataset metadata messages must have the appropriate tag. Current tag", tag);
                return DBERR_COMM_INVALID_MSG_TAG;
            }
            // send the message
            int mpi_ret = MPI_Send(datasetMetadataMsg.data, datasetMetadataMsg.count, MPI_CHAR, destRank, tag, comm);
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
                    if (i != HOST_LOCAL_RANK) {
                        // send to workers
                        local_ret = send::sendInstructionMessage(i, tag,g_controller_comm);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        } 
                    } else {
                        // send to local agent
                        local_ret = send::sendInstructionMessage(AGENT_RANK, tag, g_agent_comm);
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