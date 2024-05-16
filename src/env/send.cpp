#include "env/send.h"

namespace comm
{
    namespace send
    {
        DB_STATUS sendSingleIntMessage(int value, int destRank, int tag, MPI_Comm comm) {
            // send the message
            int mpi_ret = MPI_Send(&value, 1, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                log::log_error(DBERR_COMM_SEND, "Failed to send int message with tag", tag);
                return DBERR_COMM_SEND;
            }
        }

        DB_STATUS sendInstructionMsg(int destRank, int tag, MPI_Comm comm) {
            // check tag validity
            if (tag < MSG_INSTR_BEGIN || tag >= MSG_INSTR_END) {
                log::log_error(DBERR_COMM_WRONG_MSG_FORMAT, "Instruction messages must have an instruction tag. Current tag", tag);
                return DBERR_COMM_WRONG_MSG_FORMAT;
            }
            // send the message
            int mpi_ret = MPI_Send(NULL, 0, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                log::log_error(DBERR_COMM_SEND, "Send message with tag", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }
    }

    namespace broadcast
    {
        DB_STATUS broadcastInstructionMsg(int tag) {
            DB_STATUS ret = DBERR_OK;
            // broadcast to all other controllers parallely
            #pragma omp parallel
            {
                DB_STATUS local_ret = DBERR_OK;
                #pragma omp for
                for(int i=0; i<g_world_size; i++) {
                    if (i != g_host_rank) {
                        // printf("Broadcasting to controller %d\n", i);
                        local_ret = send::sendInstructionMsg(i, tag, MPI_COMM_WORLD);
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