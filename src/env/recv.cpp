#include "env/recv.h"

namespace comm 
{
    namespace recv
    {
        DB_STATUS receiveResponse(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status) {
            // check tag validity
            if (sourceTag != MSG_ACK && sourceTag != MSG_NACK) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Response tag must by either ACK or NACK. Tag:", sourceTag);
                return DBERR_INVALID_PARAMETER;
            }
            // receive response
            int mpi_ret = MPI_Recv(NULL, 0, MPI_CHAR, sourceRank, sourceTag, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                logger::log_error(DBERR_COMM_RECV, "Failed to receive response with tag ", sourceTag);
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
    }
    namespace controller
    {
        namespace recv 
        {
            
        }
    }
}