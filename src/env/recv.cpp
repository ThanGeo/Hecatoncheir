#include "env/recv.h"

namespace comm 
{

    


    namespace recv
    {
        DB_STATUS receiveSingleIntMessage(int sourceRank, int tag, int &contents) {
            MPI_Status status;
            // receive msg
            int mpi_ret = MPI_Recv(&contents, 1, MPI_CHAR, sourceRank, tag, g_global_comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive single int message with tag", tag);
                return DBERR_COMM_RECV;
            }
            return DBERR_OK;
        }

        DB_STATUS receiveInstructionMessage(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status) {
            // check tag validity
            if (sourceTag < MSG_INSTR_BEGIN || sourceTag >= MSG_INSTR_END) {
                logger::log_error(DBERR_COMM_WRONG_MSG_FORMAT, "Instruction messages must have an instruction tag. Current tag:", sourceTag);
                return DBERR_COMM_WRONG_MSG_FORMAT;
            }
            // receive msg
            int mpi_ret = MPI_Recv(NULL, 0, MPI_CHAR, sourceRank, sourceTag, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive message with tag ", sourceTag);
                return DBERR_COMM_RECV;
            }
            
            return DBERR_OK;
        }

        DB_STATUS receiveSerializedMessage(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status, char** buffer, int bufferSize) {
            int mpi_ret = MPI_Recv(*buffer, bufferSize, MPI_CHAR, sourceRank, sourceTag, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive serialized message");
                return DBERR_COMM_RECV;
            }
            // if (bufferSize > 0) {
            // } else {
            //     int mpi_ret = MPI_Recv(NULL, 0, MPI_CHAR, sourceRank, sourceTag, comm, &status);
            //     if (mpi_ret != MPI_SUCCESS) {
            //         logger::log_error(DBERR_COMM_RECV, "Failed to receive serialized message");
            //         return DBERR_COMM_RECV;
            //     }
            // }
            return DBERR_OK;
        }

    }
    namespace controller
    {
        namespace recv 
        {
            DB_STATUS receiveInstruction(int sourceRank, int tag, MPI_Comm &comm) {
                
            }
        }
    }
}