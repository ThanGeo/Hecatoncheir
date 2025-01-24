#include "containers.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm.h"
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
namespace controller 
{
    void terminate() {
        // broadcast finalization instruction
        if (g_node_rank == HOST_LOCAL_RANK) {
            DB_STATUS ret = terminateAllWorkers();
            // if (ret != DBERR_OK) {
            //     logger::log_error(ret, "Worker termination broadcast failed");
            // }
            int send_ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_NACK, g_global_intra_comm);
        }

        // Wait for the children processes to finish
        MPI_Barrier(g_agent_comm);

        // Wait for all the controllers to finish
        MPI_Barrier(g_controller_comm);

        // Finalize the MPI environment.
        MPI_Finalize();
        // if (g_node_rank == HOST_LOCAL_RANK) {
        //     logger::log_success("System finalized successfully");
        // }
    }
}

static DB_STATUS initAPRILCreation() {
    SerializedMsg<int> aprilMetadataMsg(MPI_INT);
    // pack the APRIL metadata
    DB_STATUS ret = pack::packAPRILMetadata(g_config.approximationMetadata.aprilConfig, aprilMetadataMsg);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to pack APRIL metadata");
        return ret;
    }
    // send to workers
    ret = comm::broadcast::broadcastMessage(aprilMetadataMsg, MSG_APRIL_CREATE);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast APRIL metadata");
        return ret;
    }
    // measure response time
    double startTime;
    startTime = mpi_timer::markTime();
    // wait for response by workers+agent that all is ok
    ret = comm::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }
    logger::log_success("APRIL creation finished in", mpi_timer::getElapsedTime(startTime), "seconds.");

    return ret;
}

/** @brief initializes (broadcasts) the load specific dataset action for the given dataset */
static DB_STATUS initLoadDataset(Dataset *dataset, DatasetIndex datasetIndex) {
    // send load instruction + dataset metadata
    DB_STATUS ret = DBERR_OK;
    // pack nicknames
    SerializedMsg<char> msg(MPI_CHAR);
    ret = pack::packDatasetLoadMsg(dataset, datasetIndex, msg);
    if (ret != DBERR_OK) {
        return ret;
    }

    // broadcast message
    ret = comm::broadcast::broadcastMessage(msg, MSG_LOAD_DATASET);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast query metadata");
        return ret;
    }

    // wait for responses by workers+agent that all is ok
    ret = comm::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }

    return ret;
}

static DB_STATUS initLoadAPRIL() {
    SerializedMsg<int> aprilMetadataMsg(MPI_INT);
    // pack the APRIL metadata
    DB_STATUS ret = pack::packAPRILMetadata(g_config.approximationMetadata.aprilConfig, aprilMetadataMsg);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to pack APRIL metadata");
        return ret;
    }
    // send to workers
    ret = comm::broadcast::broadcastMessage(aprilMetadataMsg, MSG_LOAD_APRIL);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast APRIL metadata");
        return ret;
    }
    // wait for responses by workers+agent that all is ok
    ret = comm::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }
    return ret;
}

int main(int argc, char* argv[]) {
    // initialize MPI environment parameters
    DB_STATUS ret = configurer::initMPIController(argc, argv);
    if (ret != DBERR_OK) {
        controller::terminate();
        return ret;
    }


    // all nodes create their agent process
    ret = proc::setupProcesses();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Setup host process environment failed");
        controller::terminate();
        return ret;
    }

    // syncrhonize with host controller
    MPI_Barrier(g_global_intra_comm);

    // logger::log_task("My global rank is", g_global_rank, "and my local rank is", g_node_rank);
    
    if (g_node_rank == HOST_LOCAL_RANK) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System setup failed");
            controller::terminate();
            return ret;
        }
        // notify driver that all is well
        ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Responding to driver failed");
            controller::terminate();
            return ret;
        }
        // listen for messages from the driver
        ret = comm::host::listen();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Interrupted");
            controller::terminate();
            return ret;
        }
    } else {
        // listen for messages from the host controller
        ret = comm::controller::listen();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Interrupted");
            controller::terminate();
            return ret;
        }
    }
    
    controller::terminate();
    // return success
    return 0;
}


