#include "containers.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm_common.h"
#include "env/comm_host.h"
#include "env/comm_worker.h"
#include "env/send.h"
#include "env/pack.h"

static DB_STATUS terminateAllWorkers() {
    // broadcast finalization instruction
    DB_STATUS ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_FIN);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Broadcasting termination instruction failed");
        return ret;
    }

    return DBERR_OK;
}

static void terminate() {
    // broadcast finalization instruction
    if (g_node_rank == HOST_LOCAL_RANK) {
        DB_STATUS ret = terminateAllWorkers();
        // if (ret != DBERR_OK) {
        //     logger::log_error(ret, "Worker termination broadcast failed");
        // }
        // int send_ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_NACK, g_global_intra_comm);
    }

    // Wait for all the workers to finish
    MPI_Barrier(g_worker_comm);

    // syncrhonize with driver
    MPI_Barrier(g_global_intra_comm);

    // Finalize the MPI environment.
    MPI_Finalize();
    // if (g_node_rank == HOST_LOCAL_RANK) {
    //     logger::log_success("System finalized successfully");
    // }
    // logger::log_success("System finalized successfully");
}

int main(int argc, char* argv[]) {
    // initialize MPI environment
    int mpi_ret;
    DB_STATUS ret = configurer::initWorker(argc, argv);
    if (ret != DBERR_OK) {
        return ret;
    }
    
    // logger::log_task("Runs on cpu", sched_getcpu());
    // logger::log_task("My rank is", g_node_rank, "my world is:", g_world_size);

    MPI_Barrier(g_global_intra_comm);

    
    if (g_node_rank == HOST_LOCAL_RANK) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System setup failed");
            terminate();
            return ret;
        }
        // then listen from driver
        ret = comm::host::listen();
        if (ret != DBERR_OK) {
            terminate();
            return ret;
        }
    } else {
        // workers listen for messages from the host
        ret = comm::worker::listen();
        if (ret != DBERR_OK) {
            terminate();
            return ret;
        }
    }

    

    // return
    terminate();
    // logger::log_success("System finalized successfully");

    return 0;
}
