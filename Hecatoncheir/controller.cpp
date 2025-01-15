#include "controller.h"

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
    void hostTerminate() {
        // broadcast finalization instruction
        DB_STATUS ret = terminateAllWorkers();
        // if (ret != DBERR_OK) {
        //     logger::log_error(ret, "Worker termination broadcast failed");
        // }

        // Wait for the children processes to finish
        MPI_Barrier(g_agent_comm);

        // Wait for all the controllers to finish
        MPI_Barrier(g_controller_comm);

        // Finalize the MPI environment.
        MPI_Finalize();
        // if (g_node_rank == HOST_RANK) {
        //     logger::log_success("System finalized successfully");
        // }
    }
}

static void printResults() {
    logger::log_success("MBR Results:", g_queryOutput.postMBRFilterCandidates);
    switch (g_config.queryMetadata.type) {
        case Q_RANGE:
        case Q_DISJOINT:
        case Q_INTERSECT:
        case Q_INSIDE:
        case Q_CONTAINS:
        case Q_COVERS:
        case Q_COVERED_BY:
        case Q_MEET:
        case Q_EQUAL:
            logger::log_success("Total Results:", g_queryOutput.queryResults);
            logger::log_success("       Accept:", g_queryOutput.trueHits / (double) g_queryOutput.postMBRFilterCandidates * 100, "%");
            logger::log_success("       Reject:", g_queryOutput.trueNegatives / (double) g_queryOutput.postMBRFilterCandidates * 100, "%");
            logger::log_success(" Inconclusive:", g_queryOutput.refinementCandidates / (double) g_queryOutput.postMBRFilterCandidates * 100, "%");
            break;
        case Q_FIND_RELATION:
            logger::log_success(" Inconclusive:", g_queryOutput.refinementCandidates / (double) g_queryOutput.postMBRFilterCandidates * 100, "%");
            logger::log_success("     Disjoint:", g_queryOutput.topologyRelationsResultMap[TR_DISJOINT]);
            logger::log_success("    Intersect:", g_queryOutput.topologyRelationsResultMap[TR_INTERSECT]);
            logger::log_success("       Inside:", g_queryOutput.topologyRelationsResultMap[TR_INSIDE]);
            logger::log_success("     Contains:", g_queryOutput.topologyRelationsResultMap[TR_CONTAINS]);
            logger::log_success("       Covers:", g_queryOutput.topologyRelationsResultMap[TR_COVERS]);
            logger::log_success("   Covered by:", g_queryOutput.topologyRelationsResultMap[TR_COVERED_BY]);
            logger::log_success("         Meet:", g_queryOutput.topologyRelationsResultMap[TR_MEET]);
            logger::log_success("        Equal:", g_queryOutput.topologyRelationsResultMap[TR_EQUAL]);
            break;
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
    ret = comm::controller::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }
    logger::log_success("APRIL creation finished in", mpi_timer::getElapsedTime(startTime), "seconds.");

    return ret;
}

static DB_STATUS initQueryExecution() {
    SerializedMsg<int> queryMetadataMsg(MPI_INT);
    // pack the APRIL metadata
    DB_STATUS ret = pack::packQueryMetadata(g_config.queryMetadata, queryMetadataMsg);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to pack query metadata");
        return ret;
    }
    // broadcast message
    ret = comm::broadcast::broadcastMessage(queryMetadataMsg, MSG_QUERY_INIT);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to broadcast query metadata");
        return ret;
    }
    // reset query outpyt
    g_queryOutput.reset();
    logger::log_task("Processing query '", mapping::queryTypeIntToStr(g_config.queryMetadata.type),"' on datasets", g_config.datasetMetadata.getDatasetR()->nickname, "-", g_config.datasetMetadata.getDatasetS()->nickname);
    // measure response time
    double startTime;
    startTime = mpi_timer::markTime();
    // wait for results by workers+agent
    ret = comm::controller::host::gatherResults();
    if (ret != DBERR_OK) {
        return ret;
    }
    logger::log_success("Query evaluated in", mpi_timer::getElapsedTime(startTime), "seconds.");

    // print results
    printResults();

    return DBERR_OK;
}

/** @brief initializes (broadcasts) the load specific dataset action for the given dataset */
static DB_STATUS initLoadDataset(Dataset *dataset, DatasetIndexE datasetIndex) {
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
    ret = comm::controller::host::gatherResponses();
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
    ret = comm::controller::host::gatherResponses();
    if (ret != DBERR_OK) {
        return ret;
    }
    return ret;
}

static DB_STATUS initPartitioning() {
    DB_STATUS ret = DBERR_OK;
    // claculate dataset metadata if not given
    for (auto &it: g_config.datasetMetadata.datasets) {
        Dataset* dataset = g_config.datasetMetadata.getDatasetByNickname(it.second.nickname);
        if (!dataset->dataspaceMetadata.boundsSet) {
            
            switch (dataset->fileType) {
                case FT_CSV:
                    ret = partitioning::csv::calculateDatasetMetadata(dataset);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to calculate CSV dataset metadata.");
                        return ret;
                    }
                    break;
                case FT_WKT:
                    ret = partitioning::wkt::calculateDatasetMetadata(dataset);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to calculate WKT dataset metadata.");
                        return ret;
                    }
                    break;
                case FT_BINARY:
                default:
                    logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported data file type:", dataset->fileType);
                    break;
            }

        }
    }
    // update the global dataspace
    g_config.datasetMetadata.updateDataspace();
    
    // perform partitioning for each dataset
    for (auto &it: g_config.datasetMetadata.datasets) {
        // first, issue the partitioning instruction
        ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_PARTITIONING_INIT);
        if (ret != DBERR_OK) {
            return ret;
        }
        
        // broadcast the dataset metadata to the nodes
        ret = comm::controller::host::broadcastDatasetMetadata(&it.second);
        if (ret != DBERR_OK) {
            return ret;
        }

        // partition
        ret = partitioning::partitionDataset(g_config.datasetMetadata.getDatasetByNickname(it.second.nickname));
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Partitioning dataset", it.second.nickname, "failed");
            return ret;
        }
    }
    return ret;
}

static DB_STATUS performActions() {
    DB_STATUS ret = DBERR_OK;
    // perform the user-requested actions in order
    for(int i=0;i <g_config.actions.size(); i++) {
        logger::log_task("*** Action:", mapping::actionIntToStr(g_config.actions.at(i).type),"***");
        switch(g_config.actions.at(i).type) {
            case ACTION_PERFORM_PARTITIONING:
                ret = initPartitioning();
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Partitioning action failed");
                    return ret;
                }
                break;
            case ACTION_LOAD_DATASET_R:
                ret = initLoadDataset(g_config.datasetMetadata.getDatasetR(), DATASET_R);
                if (ret != DBERR_OK) {
                    return ret;
                }
                break;
            case ACTION_LOAD_DATASET_S:
                ret = initLoadDataset(g_config.datasetMetadata.getDatasetS(), DATASET_S);
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
                // send query metadata and begin evaluating
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

// int main(int argc, char* argv[]) {
//     // initialize MPI environment
//     DB_STATUS ret = configurer::initMPI(argc, argv);
//     if (ret != DBERR_OK) {
//         return ret;
//     }
    
//     // print cpu
//     // logger::log_task("Runs on cpu", sched_getcpu());

//     // all nodes create their agent process
//     ret = proc::setupProcesses();
//     if (ret != DBERR_OK) {
//         logger::log_error(ret, "Setup host process environment failed");
//         hostTerminate();
//         return ret;
//     }

//     if (g_node_rank == HOST_RANK) {
//         // host controller has to handle setup/user input etc.
//         ret = setup::setupSystem(argc, argv);
//         if (ret != DBERR_OK) {
//             logger::log_error(ret, "System setup failed");
//             hostTerminate();
//             return ret;
//         }
        
//         // perform the user-requested actions in order
//         ret = performActions();
//         if (ret != DBERR_OK) {
//             hostTerminate();
//             return ret;
//         }

//         // terminate
//         hostTerminate();
//     } else {
//         // worker controllers go directly to listening
//         // printf("Controller %d listening...\n", g_node_rank);
//         ret = comm::controller::listen();
//         if (ret != DBERR_OK) {
//             logger::log_error(ret, "Interrupted");
//             return ret;
//         }
//     }

//     return ret;
// }

int main(int argc, char* argv[]) {
    // initialize MPI environment parameters
    DB_STATUS ret = configurer::initMPIController(argc, argv);
    if (ret != DBERR_OK) {
        return ret;
    }

    // // all nodes create their agent process
    ret = proc::setupProcesses();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Setup host process environment failed");
        controller::hostTerminate();
        return ret;
    }

    // logger::log_task("C. Hello! My rank is", g_node_rank, "and my known world has size", g_world_size);
    
    
    if (g_node_rank == HOST_RANK) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System setup failed");
            controller::hostTerminate();
            return ret;
        }
    } 

    // synchronize before moving on to listening
    // MPI_Barrier(g_controller_comm);
    // listen for messages
    ret = comm::controller::listen();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Interrupted");
        return ret;
    }
    
    // synchronize before returning
    logger::log_success("C. Terminating...");
    // return success
    return 0;
}


