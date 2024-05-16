#include <stdio.h>
#include <unistd.h>
#include <mpi.h>

#include "def.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm.h"
#include "env/task.h"

static DB_STATUS terminateAllWorkers() {
    // broadcast finalization instruction
    DB_STATUS ret = comm::broadcast::broadcastInstructionMsg(MSG_INSTR_FIN);
    if (ret != DBERR_OK) {
        log::log_error(ret, "Broadcasting termination instruction failed");
        return ret;
    }

    // perform the instruction locally as well
    ret = comm::controller::SendInstructionMessageToAgent(MSG_INSTR_FIN);
    if (ret != DBERR_OK) {
        log::log_error(ret, "Sending to local children failed");
        return ret;
    }

    return DBERR_OK;
}

static void hostTerminate() {
    // broadcast finalization instruction
    DB_STATUS ret = terminateAllWorkers();
    if (ret != DBERR_OK) {
        log::log_error(ret, "Worker termination broadcast failed");
    }
    
}

int main(int argc, char* argv[]) {
    DB_STATUS ret;
    char c;
    int provided;
    int rank, wsize;
    // init MPI
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
	MPI_Comm_size(MPI_COMM_WORLD, &wsize);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    g_world_size = wsize;
    g_node_rank = rank;
       
    // get parent process intercomm (must be null)
    MPI_Comm_get_parent(&proc::g_intercomm);
    if (proc::g_intercomm != MPI_COMM_NULL) {
        log::log_error(DBERR_PROC_INIT_FAILED, "Controllers must be parentless");
        goto EXIT_SAFELY;
    }
    g_parent_original_rank = PARENTLESS_RANK;

    log::log_task("Runs on cpu", sched_getcpu());
    // printf( "Controller %d cpu = %d\n", g_node_rank, sched_getcpu() );

    // all nodes create their agent process
    ret = proc::setupProcesses();
    if (ret != DBERR_OK) {
        log::log_error(ret, "Setup host process environment failed");
        hostTerminate();
        goto EXIT_SAFELY;
    }

    if (g_node_rank == g_host_rank) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            log::log_error(ret, "System setup failed");
            hostTerminate();
            goto EXIT_SAFELY;
        }

        // listen for controller replies or the local agent's replies/errors (TODO: maybe employ dedicated thread?)


        // terminate
        hostTerminate();

    } else {
        // worker controllers go directly to listening
        // printf("Controller %d listening...\n", g_node_rank);
        ret = comm::controller::listen();
        if (ret != DBERR_OK) {
            log::log_error(ret, "Controller failed while listening for messages");
            goto EXIT_SAFELY;
        }
    }

EXIT_SAFELY:
    // Wait for the children processes to finish
    MPI_Barrier(proc::g_intercomm);

    // Wait for all the controllers to finish
    MPI_Barrier(MPI_COMM_WORLD);

    // Finalize the MPI environment.
    MPI_Finalize();
    if (rank == g_host_rank) {
        log::log_success("System finalized successfully");
    }
    return 0;
}