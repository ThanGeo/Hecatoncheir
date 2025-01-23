#include "proc.h"


namespace proc
{    
    DB_STATUS setupProcesses() {
        char processor_name[MPI_MAX_PROCESSOR_NAME];
        int name_len;
        // Get the hostname of the current controller process
        MPI_Get_processor_name(processor_name, &name_len);
        // MPI_Info object to specify additional information for process spawning (todo: useful?)
        MPI_Info mpi_info;
        MPI_Info_create(&mpi_info);
        MPI_Info_set(mpi_info, "host", processor_name); 
         // Array to store the error codes for each spawned process
        int* error_codes = (int*)malloc(1 * sizeof(int));
        
        // logger::log_task("processor name for agent", processor_name);

        // spawn the agent
        int mpi_ret = MPI_Comm_spawn(AGENT_EXECUTABLE_PATH.c_str(), NULL, 1, mpi_info, 0, MPI_COMM_SELF, &g_agent_comm, error_codes);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_PROC_INIT_FAILED, "Creating agent process failed");
            return DBERR_PROC_INIT_FAILED;
        }

        // broadcast the controller's node rank to the agent
        mpi_ret = MPI_Send(&g_node_rank, 1, MPI_INT, AGENT_RANK, 0, g_agent_comm);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_COMM_BCAST, "Broadcasting controller rank failed");
            return DBERR_COMM_BCAST;
        }

        // cleanup
        MPI_Info_free(&mpi_info);
        free(error_codes);

        // syncrhonize with agent
        MPI_Barrier(g_agent_comm);

        return DBERR_OK;
    }

}