#include <stdio.h>
#include <unistd.h>
#include <mpi.h>

#include "def.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm.h"

int main(int argc, char* argv[]) {
    DB_STATUS ret;
    char c;
    int provided;
    int rank, wsize;
    int mpi_ret;
    // init MPI
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	MPI_Comm_size(MPI_COMM_WORLD, &wsize);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    g_world_size = wsize;
    g_node_rank = rank;
    
    // get parent process intercomm
    MPI_Comm_get_parent(&proc::g_intercomm);
    if (proc::g_intercomm == MPI_COMM_NULL) {
        logger::log_error(DBERR_PROC_INIT_FAILED, "The agent can't be parentless");
        goto EXIT_SAFELY;
    }
    // receive parent's original rank from the parent
    mpi_ret = MPI_Bcast(&g_parent_original_rank, 1, MPI_INT, 0, proc::g_intercomm);
    if (mpi_ret) {
        logger::log_error(DBERR_COMM_BCAST, "Receiving parent rank failed", DBERR_COMM_BCAST);
        goto EXIT_SAFELY;
    }
    // print cpu
    // logger::log_task("Runs on cpu", sched_getcpu());
    
    // printf("Agent with parent %d and rank %d runs on cpu %d\n", g_parent_original_rank, g_node_rank, sched_getcpu());
    ret = comm::agent::listen();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Agent failed while listening");
        goto EXIT_SAFELY;
    }
EXIT_SAFELY:
    // logger::log_success("Waiting for exit...");
    // wait for the rest of the processes in the intercomm to finish
    MPI_Barrier(proc::g_intercomm);
    // Finalize the MPI environment.
    MPI_Finalize();
    return 0;
}