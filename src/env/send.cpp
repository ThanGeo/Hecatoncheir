#include "env/send.h"

namespace comm
{
    namespace send
    {
        DB_STATUS sendSingleIntMessage(int value, int destRank, int tag, MPI_Comm &comm) {
            // send the message
            int mpi_ret = MPI_Send(&value, 1, MPI_CHAR, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Failed to send int message with tag", tag);
                return DBERR_COMM_SEND;
            }
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

        template <typename T> 
        DB_STATUS sendMsgPack(MsgPackT<T> &msgPack, int destRank, int tag, MPI_Comm &comm) {
            // send the info pack message           
            int mpi_ret = MPI_Send(msgPack.data, msgPack.count, msgPack.type, destRank, tag, comm);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_SEND, "Send message with tag", tag);
                return DBERR_COMM_SEND;
            }
            return DBERR_OK;
        }


        DB_STATUS sendGeometryMessage(MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack, int destRank, int tag, MPI_Comm &comm) {
            DB_STATUS ret = DBERR_OK;
            // check tag validity
            if (tag < MSG_DATA_BEGIN || tag >= MSG_DATA_END) {
                logger::log_error(DBERR_COMM_WRONG_MSG_FORMAT, "Data messages must have a data tag. Current tag", tag);
                return DBERR_COMM_WRONG_MSG_FORMAT;
            }
            
            // send the info pack message
            ret = sendMsgPack(infoPack, destRank, tag, comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending info pack message for single polygon failed.");
                return ret;
            }

            // send the coords pack message
            ret = sendMsgPack(coordsPack, destRank, tag, comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending coords pack message for single polygon failed.");
                return ret;
            }
            return ret;
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
                        // printf("Broadcasting to controller %d\n", i);
                        local_ret = send::sendInstructionMessage(i, tag, g_global_comm);
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