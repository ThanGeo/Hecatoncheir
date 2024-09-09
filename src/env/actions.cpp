#include "env/actions.h"

namespace actions
{
    static DB_STATUS terminateAllWorkers() {
        // broadcast finalization instruction
        DB_STATUS ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_FIN);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Broadcasting termination instruction failed");
            return ret;
        }

        return DBERR_OK;
    }

    DB_STATUS initTermination() {
        // broadcast finalization instruction
        DB_STATUS ret = terminateAllWorkers();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Worker termination broadcast failed");
        }
        return ret;
    }

    static void printResults() {
        logger::log_success("MBR Results:", g_queryOutput.postMBRFilterCandidates);
        switch (g_config.queryInfo.type) {
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
        SerializedMsg<int> aprilInfoMsg(MPI_INT);
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

    static DB_STATUS initQueryResultGathering() {
        // reset query output
        g_queryOutput.reset();
        // measure response time
        double startTime;
        startTime = mpi_timer::markTime();
        // wait for results by workers+agent
        DB_STATUS ret = comm::controller::host::gatherResults();
        if (ret != DBERR_OK) {
            return ret;
        }
        logger::log_success("Query evaluated in", mpi_timer::getElapsedTime(startTime), "seconds.");

        // print results
        printResults();

        return ret;
    }

    static DB_STATUS initQueryExecution() {
        SerializedMsg<int> queryInfoMsg(MPI_INT);
        // pack the query info
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
        logger::log_task("Query '", mapping::queryTypeIntToStr(g_config.queryInfo.type),"' initialized.");
        return DBERR_OK;
    }

    static DB_STATUS initLoadDataset(Dataset *dataset, MsgType msgTag) {
        // send load instruction + dataset info
        DB_STATUS ret = DBERR_OK;
        // pack nicknames
        SerializedMsg<char> msg(MPI_CHAR);
        ret = pack::packDatasetNickname(msg, dataset);
        if (ret != DBERR_OK) {
            return ret;
        }

        // broadcast message
        ret = comm::broadcast::broadcastMessage(msg, msgTag);
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
        SerializedMsg<int> aprilInfoMsg(MPI_INT);
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

    static DB_STATUS initPartitioning() {
        DB_STATUS ret = DBERR_OK;
        for (auto &it: g_config.datasetInfo.datasets) {
            Dataset* dataset = g_config.datasetInfo.getDatasetByNickname(it.second.nickname);
            if (!dataset->dataspaceInfo.boundsSet) {
                ret = partitioning::calculateCSVDatasetDataspaceBounds(*dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Calculate CSV dataset dataspace bounds failed");
                    return ret;
                }
            }
        }
        // update the global dataspace
        g_config.datasetInfo.updateDataspace();
        // perform partitioning for each dataset
        for (auto &it: g_config.datasetInfo.datasets) {
            ret = partitioning::partitionDataset(g_config.datasetInfo.getDatasetByNickname(it.second.nickname), MSG_INSTR_DATA_PARTITIONING_INIT);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Partitioning dataset", it.second.nickname, "failed");
                return ret;
            }
        }
        return ret;
    }

    

    DB_STATUS performActions() {
        DB_STATUS ret = DBERR_OK;
        // perform the user-requested actions in order
        for(auto &action : g_config.actions) {
            logger::log_task("*** Action:", mapping::actionIntToStr(action.type));
            switch(action.type) {
                case ACTION_PERFORM_PARTITIONING:
                    ret = initPartitioning();
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Partitioning action failed");
                        return ret;
                    }
                    break;
                case ACTION_LOAD_DATASET_R:
                    /* instruct workers to load the dataset R (MBRs)*/
                    ret = initLoadDataset(g_config.datasetInfo.getDatasetR(), MSG_LOAD_DATASET_R);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case ACTION_LOAD_DATASET_S:
                    /* instruct workers to load the dataset S (MBRs)*/
                    ret = initLoadDataset(g_config.datasetInfo.getDatasetS(), MSG_LOAD_DATASET_S);
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
                case ACTION_QUERY_INIT:
                    // send query info and begin evaluating
                    ret = initQueryExecution();
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case ACTION_QUERY_GATHER_RESULTS:
                    // wait for the query results
                    ret = initQueryResultGathering();
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case ACTION_QUERY_PARTITIONING:
                    // parse and partition the queries (selection only)
                    ret = partitioning::partitionDataset(g_config.datasetInfo.getDatasetQ(), MSG_INSTR_QUERY_BATCHES_INIT);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_INVALID_PARAMETER, "Unknown action. Type:",action.type);
                    return ret;
            }
        }
        return DBERR_OK;
    }

}