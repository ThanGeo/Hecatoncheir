


#include "containers.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm.h"
#include "env/pack.h"
#include "env/actions.h"

int main(int argc, char* argv[]) {
    // initialize MPI environment
    DB_STATUS ret = configurer::initMPI(argc, argv);
    if (ret != DBERR_OK) {
        goto EXIT_SAFELY;
    }
    
    // print cpu
    // logger::log_task("Runs on cpu", sched_getcpu());

    // all nodes create their agent process
    ret = proc::setupProcesses();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Setup host process environment failed");
        actions::initTermination();
        goto EXIT_SAFELY;
    }

    if (g_node_rank == g_host_rank) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System setup failed");
            actions::initTermination();
            goto EXIT_SAFELY;
        }
        
        // perform the user-requested actions in order
        ret = actions::performActions();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Not all actions where performed successfully.");
            actions::initTermination();
            goto EXIT_SAFELY;
        }
        // terminate
        actions::initTermination();
    } else {
        // worker controllers go directly to listening
        // printf("Controller %d listening...\n", g_node_rank);
        ret = comm::controller::listen();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Interrupted");
            goto EXIT_SAFELY;
        }
    }

EXIT_SAFELY:
    // Wait for the children processes to finish
    // logger::log_task("Waiting for children...");
    MPI_Barrier(g_local_comm);

    // Wait for all the controllers to finish
    // logger::log_task("Waiting for siblings...");
    MPI_Barrier(g_global_comm);

    // Finalize the MPI environment.
    MPI_Finalize();
    if (g_node_rank == g_host_rank) {
        logger::log_success("System finalized successfully");
    }
    return 0;
}