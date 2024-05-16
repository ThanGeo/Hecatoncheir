#include "env/recv.h"

namespace comm 
{

    


    namespace recv
    {
        DB_STATUS receiveSingleIntMsg(int sourceRank, int tag, int &contents) {
            MPI_Status status;
            // receive msg
            int mpi_ret = MPI_Recv(&contents, 1, MPI_CHAR, sourceRank, tag, MPI_COMM_WORLD, &status);
            if (mpi_ret != MPI_SUCCESS) {
                log::log_error(DBERR_COMM_RECV, "Failed to receive single int message with tag", tag);
                return DBERR_COMM_RECV;
            }
            return DBERR_OK;
        }

        DB_STATUS receiveInstructionMsg(int sourceRank, int tag, MPI_Comm comm, MPI_Status &status) {
            // check tag validity
            if (tag < MSG_INSTR_BEGIN || tag >= MSG_INSTR_END) {
                log::log_error(DBERR_COMM_WRONG_MSG_FORMAT, "Instruction messages must have an instruction tag. Current tag:", tag);
                return DBERR_COMM_WRONG_MSG_FORMAT;
            }
            // receive msg
            int mpi_ret = MPI_Recv(NULL, 0, MPI_CHAR, sourceRank, tag, comm, &status);
            if (mpi_ret != MPI_SUCCESS) {
                log::log_error(DBERR_COMM_RECV, "Failed to receive message with tag ", tag);
                return DBERR_COMM_RECV;
            }
            
            return DBERR_OK;
        }

        
        
    }
    namespace controller
    {
        namespace recv 
        {
            DB_STATUS receiveInstruction(int sourceRank, int tag, MPI_Comm comm) {
                
            }
        }
    }
}