#include "proc.h"


namespace proc
{    
    DB_STATUS setupProcesses() {
        // MPI_Info object to specify additional information for process spawning (todo: useful?)
        MPI_Info info;
        MPI_Info_create(&info);
         // Array to store the error codes for each spawned process
        int* error_codes = (int*)malloc(1 * sizeof(int));
                
        int mpi_ret = MPI_Comm_spawn(AGENT_PROGRAM_NAME, MPI_ARGV_NULL, 1, info, 0, MPI_COMM_SELF, &g_local_comm, error_codes);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_PROC_INIT_FAILED, "Creating agent process failed");
            return DBERR_PROC_INIT_FAILED;
        }

        // broadcast the controller's node rank to the agent
        mpi_ret = MPI_Bcast(&g_node_rank, 1, MPI_INT, MPI_ROOT, g_local_comm);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_COMM_BCAST, "Broadcasting controller rank failed");
            return DBERR_COMM_BCAST;
        }

        // cleanup
        MPI_Info_free(&info);
        free(error_codes);

        // sanity check
        if (HOST_RANK != AGENT_RANK) {
            // this will never happen unless somebody did a malakia
            logger::log_error(DBERR_INVALID_PARAMETER, "Host node rank and every agent's rank must match");
            return DBERR_INVALID_PARAMETER;
        }

        return DBERR_OK;
    }

}