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
    // logger::log_task("C. Hello! My rank is", g_node_rank, "and my known world has size", g_world_size, "and my parent is", g_parent_original_rank);
    
    // printf("Agent with parent %d and rank %d runs on cpu %d\n", g_parent_original_rank, g_node_rank, sched_getcpu());
    ret = comm::agent::listen();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Interrupted");
        terminateAgent();
        return ret;
    }

    // return
    terminateAgent();
    logger::log_success("A. Terminated.");
    return 0;
}

// int main(int argc, char* argv[]) {
//     // initialize MPI environment
//     int mpi_ret;
//     DB_STATUS ret = configurer::initMPI(argc, argv);
//     if (ret != DBERR_OK) {
//         goto EXIT_SAFELY;
//     }
    
//     // get parent process intercomm
//     MPI_Comm_get_parent(&g_agent_comm);
//     if (g_agent_comm == MPI_COMM_NULL) {
//         logger::log_error(DBERR_PROC_INIT_FAILED, "The agent can't be parentless");
//         goto EXIT_SAFELY;
//     }
//     // receive parent's original rank from the parent
//     mpi_ret = MPI_Bcast(&g_parent_original_rank, 1, MPI_INT, 0, g_agent_comm);
//     if (mpi_ret) {
//         logger::log_error(DBERR_COMM_BCAST, "Receiving parent rank failed", DBERR_COMM_BCAST);
//         goto EXIT_SAFELY;
//     }
//     // print cpu
//     // logger::log_task("Runs on cpu", sched_getcpu());
    
//     // printf("Agent with parent %d and rank %d runs on cpu %d\n", g_parent_original_rank, g_node_rank, sched_getcpu());
//     ret = comm::agent::listen();
//     if (ret != DBERR_OK) {
//         logger::log_error(ret, "Interrupted");
//         goto EXIT_SAFELY;
//     }
// EXIT_SAFELY:
//     // logger::log_success("Waiting for exit...");
//     // wait for the rest of the processes in the intercomm to finish
//     MPI_Barrier(g_agent_comm);
//     // Finalize the MPI environment.
//     MPI_Finalize();
//     return 0;
// }