
#include "SpatialLib.h"

#include "def.h"
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

static void hostTerminate() {
    // broadcast finalization instruction
    DB_STATUS ret = terminateAllWorkers();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Worker termination broadcast failed");
    }
    
}

static DB_STATUS initAPRILCreation() {
    SerializedMsgT<int> aprilInfoMsg(MPI_INT);
    // pack the APRIL info
    DB_STATUS ret = pack::packAPRILInfo(g_config.approximationInfo.aprilConfig, aprilInfoMsg);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to pack APRIL info");
        return ret;
    }
    // send to workers
    ret = comm::broadcast::broadcastMessage(aprilInfoMsg, MSG_APRIL_CREATE);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast APRIL info");
        return ret;
    }
    // measure response time
    double startTime;
    startTime = mpi_timer::markTime();
    // wait for response by workers+agent that all is ok
    ret = comm::controller::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }
    logger::log_success("APRIL creation finished in", mpi_timer::getElapsedTime(startTime), "seconds.");

    return ret;
}

static DB_STATUS initQueryExecution() {
    SerializedMsgT<int> queryInfoMsg(MPI_INT);
    // pack the APRIL info
    DB_STATUS ret = pack::packQueryInfo(g_config.queryInfo, queryInfoMsg);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to pack query info");
        return ret;
    }
    // broadcast message
    ret = comm::broadcast::broadcastMessage(queryInfoMsg, MSG_QUERY_INIT);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast query info");
        return ret;
    }

    logger::log_task("Processing query", spatial_lib::mapping::queryTypeIntToStr(g_config.queryInfo.type),"on datasets", g_config.datasetInfo.getDatasetR()->nickname, "-", g_config.datasetInfo.getDatasetS()->nickname);
    // measure response time
    double startTime;
    startTime = mpi_timer::markTime();
    // wait for results by workers+agent
    ret = comm::controller::host::gatherResults();
    if (ret != DBERR_OK) {
        return ret;
    }
    logger::log_success("Query evaluted in", mpi_timer::getElapsedTime(startTime), "seconds.");

    return DBERR_OK;
}

static DB_STATUS initLoadDatasets() {
    // send load instruction + dataset info
    DB_STATUS ret = DBERR_OK;
    // pack nicknames
    SerializedMsgT<char> msg(MPI_CHAR);
    ret = pack::packDatasetsNicknames(msg);
    if (ret != DBERR_OK) {
        return ret;
    }

    // broadcast message
    ret = comm::broadcast::broadcastMessage(msg, MSG_LOAD_DATASETS);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast query info");
        return ret;
    }

    // wait for responses by workers+agent that all is ok
    ret = comm::controller::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }

    return ret;
}

static DB_STATUS initLoadAPRIL() {
    SerializedMsgT<int> aprilInfoMsg(MPI_INT);
    // pack the APRIL info
    DB_STATUS ret = pack::packAPRILInfo(g_config.approximationInfo.aprilConfig, aprilInfoMsg);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to pack APRIL info");
        return ret;
    }
    // send to workers
    ret = comm::broadcast::broadcastMessage(aprilInfoMsg, MSG_LOAD_APRIL);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast APRIL info");
        return ret;
    }
    // wait for responses by workers+agent that all is ok
    ret = comm::controller::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }
    return ret;
}

static DB_STATUS performActions() {
    DB_STATUS ret = DBERR_OK;
    // perform the user-requested actions in order
    for(int i=0;i <g_config.actions.size(); i++) {
        logger::log_task("*** Action:", actionIntToStr(g_config.actions.at(i).type));
        switch(g_config.actions.at(i).type) {
            case ACTION_PERFORM_PARTITIONING:
                for (auto &it: g_config.datasetInfo.datasets) {
                    ret = partitioning::partitionDataset(g_config.datasetInfo.getDatasetByNickname(it.second.nickname));
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                break;
            case ACTION_LOAD_DATASETS:
                /* instruct workers to load the datasets (MBRs)*/
                ret = initLoadDatasets();
                if (ret != DBERR_OK) {
                    return ret;
                }
                break;
            case ACTION_LOAD_APRIL:
                /* instruct workers to load the APRIL*/
                ret = initLoadAPRIL();
                if (ret != DBERR_OK) {
                    return ret;
                }
                break;
            case ACTION_CREATE_APRIL:
                // initialize APRIL creation
                ret = initAPRILCreation();
                if (ret != DBERR_OK) {
                    return ret;
                }
                break;
            case ACTION_QUERY:
                // send query info and begin evaluating
                ret = initQueryExecution();
                if (ret != DBERR_OK) {
                    return ret;
                }
                break;
            default:
                logger::log_error(DBERR_INVALID_PARAMETER, "Unknown action. Type:",g_config.actions.at(i).type);
                return ret;
        }
    }
    return DBERR_OK;
}

int main(int argc, char* argv[]) {
    // initialize MPI environment
    DB_STATUS ret = configure::initMPI(argc, argv);
    if (ret != DBERR_OK) {
        goto EXIT_SAFELY;
    }
    
    // print cpu
    // logger::log_task("Runs on cpu", sched_getcpu());

    // all nodes create their agent process
    ret = proc::setupProcesses();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Setup host process environment failed");
        hostTerminate();
        goto EXIT_SAFELY;
    }

    if (g_node_rank == g_host_rank) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System setup failed");
            hostTerminate();
            goto EXIT_SAFELY;
        }
        
        // perform the user-requested actions in order
        ret = performActions();
        if (ret != DBERR_OK) {
            hostTerminate();
            goto EXIT_SAFELY;
        }

        // terminate
        hostTerminate();
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
    MPI_Barrier(g_local_comm);

    // Wait for all the controllers to finish
    MPI_Barrier(g_global_comm);

    // Finalize the MPI environment.
    MPI_Finalize();
    if (g_node_rank == g_host_rank) {
        logger::log_success("System finalized successfully");
    }
    return 0;
}