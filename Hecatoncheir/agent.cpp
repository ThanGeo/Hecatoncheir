#include <stdio.h>
#include <unistd.h>

#include "containers.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm.h"

void terminateAgent() {
    // logger::log_success("Waiting for exit...");
    // wait for the rest of the processes in the intercomm to finish
    MPI_Barrier(g_agent_comm);
    // Finalize the MPI environment.
    logger::log_task("Calling MPI_Finalize()");
    MPI_Finalize();
}

int main(int argc, char* argv[]) {
    // initialize MPI environment
    int mpi_ret;
    DB_STATUS ret = configurer::initMPIAgent(argc, argv);
    if (ret != DBERR_OK) {
        return ret;
    }
    
    // logger::log_task("Runs on cpu", sched_getcpu());
    logger::log_task("My controller's local rank is", g_parent_original_rank, "and my local rank is", g_node_rank);
    
    // printf("Agent with parent %d and rank %d runs on cpu %d\n", g_parent_original_rank, g_node_rank, sched_getcpu());
    ret = comm::agent::listen();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Interrupted");
        terminateAgent();
        return ret;
    }

    // return
    terminateAgent();
    return 0;
}
