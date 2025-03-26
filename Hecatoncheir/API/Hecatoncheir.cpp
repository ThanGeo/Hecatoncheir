#include "Hecatoncheir.h"

#include "def.h"
#include "utils.h"
#include "env/comm_def.h"
#include "env/send.h"
#include "env/recv.h"
#include "env/pack.h"
#include "env/comm.h"

#define HOST_CONTROLLER 1

static DB_STATUS probeBlocking(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
    int mpi_ret = MPI_Probe(sourceRank, tag, comm, &status);
    if(mpi_ret != MPI_SUCCESS) {
        logger::log_error(DBERR_COMM_PROBE_FAILED, "Blocking probe failed");
        return DBERR_COMM_PROBE_FAILED;
    }
    return DBERR_OK;
}

static DB_STATUS receiveResponse(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status) {
    // check tag validity
    if (sourceTag != MSG_ACK && sourceTag != MSG_NACK) {
        logger::log_error(DBERR_INVALID_PARAMETER, "Response tag must by either ACK or NACK. Tag:", sourceTag);
        return DBERR_INVALID_PARAMETER;
    }
    // receive response
    int mpi_ret = MPI_Recv(NULL, 0, MPI_CHAR, sourceRank, sourceTag, comm, &status);
    if (mpi_ret != MPI_SUCCESS) {
        logger::log_error(DBERR_COMM_RECV, "Failed to receive response with tag ", sourceTag);
        return DBERR_COMM_RECV;
    }
    return DBERR_OK;
}

static DB_STATUS receiveResult(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status, hec::QueryResult &finalResults) {
    SerializedMsg<char> msg(MPI_CHAR);
    // receive message
    DB_STATUS ret = comm::recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
    if (ret != DBERR_OK) {
        return ret;
    }
    // unpack
    finalResults.deserialize(msg.data, msg.count);

    return DBERR_OK;
}

static DB_STATUS receiveResultBatch(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status, std::unordered_map<int,hec::QueryResult> &finalResults) {
    SerializedMsg<char> msg(MPI_CHAR);
    // receive message
    DB_STATUS ret = comm::recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
    if (ret != DBERR_OK) {
        return ret;
    }
    // unpack
    ret = unpack::unpackBatchResults(msg, finalResults);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to unpack batch query results.");
        return ret;
    }
    return DBERR_OK;
}

static DB_STATUS waitForResponse() {
    MPI_Status status;
    // wait for response by the host controller
    DB_STATUS ret = probeBlocking(1, MPI_ANY_TAG, g_global_intra_comm, status);
    if (ret != DBERR_OK) {
        return ret;
    }
    // receive response
    ret = receiveResponse(HOST_CONTROLLER, status.MPI_TAG, g_global_intra_comm, status);
    if (ret != DBERR_OK) {
        return ret;
    }
    // check response, if its NACK then terminate
    if (status.MPI_TAG == MSG_NACK) {
        logger::log_error(DBERR_COMM_RECEIVED_NACK, "Received NACK by agent");
        return DBERR_COMM_RECEIVED_NACK;
    }
    return ret;
}    

static DB_STATUS waitForResult(hec::QueryResult &finalResults) {
    MPI_Status status;
    // wait for response by the host controller
    DB_STATUS ret = probeBlocking(1, MPI_ANY_TAG, g_global_intra_comm, status);
    if (ret != DBERR_OK) {
        return ret;
    }
    switch (status.MPI_TAG) {
        case MSG_NACK:
            // receive response
            ret = receiveResponse(HOST_CONTROLLER, status.MPI_TAG, g_global_intra_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            logger::log_error(DBERR_COMM_RECEIVED_NACK, "Query failed.");
            return DBERR_COMM_RECEIVED_NACK;
        case MSG_QUERY_RESULT:
            // receive result
            ret = receiveResult(HOST_CONTROLLER, status.MPI_TAG, g_global_intra_comm, status, finalResults);
            if (ret != DBERR_OK) {
                return ret;
            }
            break;
        default:
            break;
    }
    
    return ret;
}

static DB_STATUS waitForResult(std::unordered_map<int, hec::QueryResult> &finalResults) {
    MPI_Status status;
    // wait for response by the host controller
    DB_STATUS ret = probeBlocking(1, MPI_ANY_TAG, g_global_intra_comm, status);
    if (ret != DBERR_OK) {
        return ret;
    }
    switch (status.MPI_TAG) {
        case MSG_NACK:
            // receive response
            ret = receiveResponse(HOST_CONTROLLER, status.MPI_TAG, g_global_intra_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            logger::log_error(DBERR_COMM_RECEIVED_NACK, "Query failed.");
            return DBERR_COMM_RECEIVED_NACK;
        case MSG_QUERY_BATCH_RESULT:
            // receive result
            ret = receiveResultBatch(HOST_CONTROLLER, status.MPI_TAG, g_global_intra_comm, status, finalResults);
            if (ret != DBERR_OK) {
                return ret;
            }
            break;
        default:
            break;
    }
    
    return ret;
}

static DB_STATUS spawnControllers(int num_procs, const std::vector<std::string> &hosts) {
    DB_STATUS ret = DBERR_OK;
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        std::vector<char*> cmdsVec(num_procs, (char*) CONTROLLER_EXECUTABLE_PATH.c_str());
        char** cmds = cmdsVec.data();
        int* error_codes = (int*)malloc(num_procs * sizeof(int));
        MPI_Info* info = (MPI_Info*)malloc(num_procs * sizeof(MPI_Info));
        int* np = (int*)malloc(num_procs * sizeof(int));

        // set spawn info
        for (int i=0; i<num_procs; i++) {
            // num procs per spawn
            np[i] = 1;
            // Set the host for each process using MPI_Info_set
            MPI_Info_create(&info[i]);
            int mpi_set_result = MPI_Info_set(info[i], "host", hosts[i].c_str());
            if (mpi_set_result != MPI_SUCCESS) {
                logger::log_error(DBERR_MPI_INFO_FAILED, "Failed to set MPI_Info host for process " + std::to_string(i));
                return DBERR_MPI_INFO_FAILED;
            }
            // logger::log_task("Spawning at", hosts[i].c_str());
        }

        // spawn the controllers
        MPI_Comm_spawn_multiple(num_procs, cmds, NULL, np, info, 0, MPI_COMM_WORLD, &g_global_inter_comm, error_codes);
        for (int i = 0; i < num_procs; ++i) {
            if (error_codes[i] != MPI_SUCCESS) {
                logger::log_error(DBERR_MPI_INIT_FAILED, "Failed while spawning the controllers.");
                return DBERR_MPI_INIT_FAILED;
            }
        }

        // Free the MPI_Info object after use
        MPI_Info_free(info);

        // merge inter-comm to intra-comm
        MPI_Intercomm_merge(g_global_inter_comm, 0, &g_global_intra_comm);
        // spit intra-comm to groups
        MPI_Comm_split(g_global_intra_comm, MPI_UNDEFINED, 0, &g_controller_comm);
        // get driver rank
        MPI_Comm_rank(g_global_intra_comm, &rank);
        MPI_Comm_size(g_global_intra_comm, &size);
        g_global_rank = rank;
        g_world_size = size;
        // logger::log_success("Set my rank to", g_global_rank, "world size:", g_world_size);

        // syncrhonize with host controller
        MPI_Barrier(g_global_intra_comm);

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System initialization failed.");
            return ret;
        }

        // release inter-comm
        MPI_Comm_free(&g_global_inter_comm);
    }

    logger::log_success("System init complete.");
    return ret;
}

static DB_STATUS terminate() {
    // send the message
    int mpi_ret = MPI_Ssend(NULL, 0, MPI_CHAR, HOST_CONTROLLER, MSG_INSTR_FIN, g_global_intra_comm);
    if (mpi_ret != MPI_SUCCESS) {
        logger::log_error(DBERR_COMM_SEND, "Send message with tag", MSG_INSTR_FIN);
        return DBERR_COMM_SEND;
    }
    logger::log_success("System finalization complete.");
    return DBERR_OK;
}

namespace hec {
    int init(int numProcs, const std::vector<std::string> &hosts){
        DB_STATUS ret = DBERR_OK;
        // init MPI
        int provided;
        int mpi_ret = MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init MPI failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        // set process type
        g_proc_type = DRIVER;

        // spawn the controllers in the hosts
        ret = spawnControllers(numProcs, hosts);
        if (ret != DBERR_OK) {
            return -1;
        }
        // all ok
        return 0;
    }

    int finalize() {
        DB_STATUS ret = terminate();
        if (ret != DBERR_OK) {
            // finished with errors
            logger::log_error(ret, "Termination finished with errors.");
            return -1;
        }
        return 0;
    }

    DatasetID prepareDataset(std::string &filePath, std::string fileTypeStr, std::string dataTypeStr, bool persist) {
        if (filePath == "") {
            // empty path, empty dataset
            return -1;
        }
        // set metadata and serialize
        SerializedMsg<char> msg(MPI_CHAR);
        DatasetMetadata metadata;
        metadata.persist = persist;
        metadata.internalID = (DatasetIndex) -1;
        metadata.path = filePath;
        metadata.fileType = mapping::fileTypeTextToInt(fileTypeStr);
        metadata.dataType = mapping::dataTypeTextToInt(dataTypeStr);
        DB_STATUS ret = metadata.serialize(&msg.data, msg.count);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Metadata serialization failed.");
            return -1;
        }

        // send message to Host Controller to init the dataset's struct and get an ID for the dataset
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_PREPARE_DATASET, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset metadata message failed.");
            return -1;
        }

        // free
        free(msg.data);

        // wait for response with dataset internal ID
        MPI_Status status;
        SerializedMsg<int> msgFromHost;
        // probe
        ret = comm::probe(HOST_CONTROLLER, MSG_DATASET_INDEX, g_global_intra_comm, status);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Probing failed agent response.");
            return ret;
        }
        // receive
        ret = comm::recv::receiveMessage(status, msgFromHost.type, g_global_intra_comm, msgFromHost);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Receiving failed for agent response.");
            return ret;
        }
        // unpack
        std::vector<int> indexes;
        ret = unpack::unpackDatasetIndexes(msgFromHost, indexes);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Error unpacking dataste indexes.");
            return ret;
        }

        // free
        free(msgFromHost.data);

        // return the ID
        return indexes[0];
    }

    DatasetID prepareDataset(std::string &filePath, std::string fileTypeStr, std::string dataTypeStr, double xMin, double yMin, double xMax, double yMax, bool persist) {
        if (filePath == "") {
            // empty path, empty dataset
            return -1;
        }
        if (xMax < xMin) {
            // fix x values
            double temp = xMax;
            xMax = xMin;
            xMin = temp; 
        }
        if (yMax < yMin) {
            // fix y values
            double temp = yMax;
            yMax = yMin;
            yMin = temp; 
        }

        if (filePath == "") {
            // empty path, empty dataset
            return -1;
        }
        // set metadata and serialize
        SerializedMsg<char> msg(MPI_CHAR);
        DatasetMetadata metadata;
        metadata.persist = persist;
        metadata.internalID = (DatasetIndex) -1;
        metadata.path = filePath;
        metadata.fileType = mapping::fileTypeTextToInt(fileTypeStr);
        metadata.dataType = mapping::dataTypeTextToInt(dataTypeStr);
        metadata.dataspaceMetadata.boundsSet = true;
        metadata.dataspaceMetadata.set(xMin, yMin, xMax, yMax);
        DB_STATUS ret = metadata.serialize(&msg.data, msg.count);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Metadata serialization failed.");
            return -1;
        }

        // send message to Host Controller to init the dataset's struct and get an ID for the dataset
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_PREPARE_DATASET, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset metadata message failed.");
            return -1;
        }

        // free
        free(msg.data);

        // wait for response with dataset internal ID
        MPI_Status status;
        SerializedMsg<int> msgFromHost;
        // probe
        ret = comm::probe(HOST_CONTROLLER, MSG_DATASET_INDEX, g_global_intra_comm, status);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Probing failed agent response.");
            return ret;
        }
        // receive
        ret = comm::recv::receiveMessage(status, msgFromHost.type, g_global_intra_comm, msgFromHost);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Receiving failed for agent response.");
            return ret;
        }
        // unpack
        std::vector<int> indexes;
        ret = unpack::unpackDatasetIndexes(msgFromHost, indexes);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Error unpacking dataste indexes.");
            return ret;
        }

        // free
        free(msgFromHost.data);

        // return the ID
        return indexes[0];
    }

    int partition(std::vector<DatasetID> datasetIndexes) {
        SerializedMsg<int> msg(MPI_INT);
        DB_STATUS ret = pack::packIntegers(msg, datasetIndexes);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Packing dataset indexes failed.");
            return -1;
        }
        // send message to Host Controller to initiate the partitioning for the datasets indexes contained in the message
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_PARTITION_DATASET, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset partition message failed.");
            return -1;
        }

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Partitioning finished with errors.");
            return -1;
        }        
        logger::log_success("Partitioned datasets.");
        return 0;
    }

    int buildIndex(std::vector<DatasetID> datasetIndexes, IndexType indexType) {
        SerializedMsg<int> msg(MPI_INT);
        std::vector<int> indexTypeAndDatasets = {indexType};
        indexTypeAndDatasets.insert(indexTypeAndDatasets.end(), datasetIndexes.begin(), datasetIndexes.end());

        DB_STATUS ret = pack::packIntegers(msg, indexTypeAndDatasets);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Packing build index info failed.");
            return -1;
        }
        // send message to Host Controller to initiate the partitioning for the datasets indexes contained in the message
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_BUILD_INDEX, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending build index message failed.");
            return -1;
        }

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Building index finished with errors.");
            return -1;
        }        
        logger::log_success("Built index.");
        return 0;
    }

    int load(std::vector<DatasetID> datasetIndexes) {
        SerializedMsg<int> msg(MPI_INT);
        DB_STATUS ret = pack::packIntegers(msg, datasetIndexes);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Packing dataset indexes failed.");
            return -1;
        }
        // send message to Host Controller to initiate the loading for the datasets indexes contained in the message
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_LOAD_DATASET, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset load message failed.");
            return -1;
        }

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Loading finished with errors.");
            return -1;
        }        
        logger::log_success("Loaded datasets.");
        return 0;
    }

    hec::QueryResult query(Query* query) {
        DB_STATUS ret = DBERR_OK;
        hec::QueryResult finalResults(query->getQueryID(), query->getQueryType(), (hec::QueryResultType) query->getResultType());
        SerializedMsg<char> msg(MPI_CHAR);
        switch (query->getQueryType()) {
            case Q_RANGE:
            case Q_INTERSECTION_JOIN:
            case Q_INSIDE_JOIN:
            case Q_DISJOINT_JOIN:
            case Q_EQUAL_JOIN:
            case Q_MEET_JOIN:
            case Q_CONTAINS_JOIN:
            case Q_COVERS_JOIN:
            case Q_COVERED_BY_JOIN:
            case Q_FIND_RELATION_JOIN:
                // pack query info
                ret = pack::packQuery(query, msg);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to pack query.");
                    return finalResults;
                }
                // send the query to Host Controller
                ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_QUERY, g_global_intra_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Sending dataset load message failed.");
                    return finalResults;
                }
                // wait for result
                ret = waitForResult(finalResults);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Receiving query results failed.");
                    return finalResults;
                }
                break;
            default:
                // error
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query name:", query->getQueryType());
                return finalResults;
        }
        logger::log_success("Evaluated query!");
        return finalResults;
    }

    std::unordered_map<int, hec::QueryResult> query(std::vector<Query*> &queryBatch) {
        DB_STATUS ret = DBERR_OK;
        std::unordered_map<int, hec::QueryResult> finalResults;

        if (queryBatch.size() == 0) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Query batch is empty.");
            return finalResults;
        }

        SerializedMsg<char> msg(MPI_CHAR);
        // pack query info
        ret = pack::packQueryBatch(&queryBatch, msg);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Failed to pack query.");
            return finalResults;
        }
        // send the query to Host Controller
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_QUERY_BATCH, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset load message failed.");
            return finalResults;
        }
        // wait for result
        ret = waitForResult(finalResults);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Receiving query results failed.");
            return finalResults;
        }
        logger::log_success("Evaluated query batch!");
        return finalResults;
    }

    static int loadQueriesFromWKT(std::string &filePath, int datasetID, hec::QueryResultType resultType, std::vector<hec::Query*> &batchQueries) {
        std::ifstream fin(filePath);
        if (!fin.is_open()) {
            logger::log_error(DBERR_MISSING_FILE, "Failed to open query file from path:", filePath);
            return -1;
        }
        std::string line, token;
        int queryID = 0;

        while (getline(fin, line)) {
            // parse line to get only the first column (wkt geometry)
            std::stringstream ss(line);
            std::getline(ss, token, '\t');

            hec::RangeQuery* query = new hec::RangeQuery(datasetID, queryID, token, resultType);
            batchQueries.emplace_back(query);

            queryID++;
        }

        fin.close();

        return 0;
    }

    std::vector<hec::Query*> loadQueriesFromFile(std::string &filePath, std::string fileTypeStr, int datasetID, hec::QueryResultType resultType) {
        std::vector<hec::Query*> batchQueries;
        int ret = 0;
        hec::FileType fileType = mapping::fileTypeTextToInt(fileTypeStr);
        switch (fileType) {
            case FT_WKT:
                ret = loadQueriesFromWKT(filePath, datasetID, resultType, batchQueries);
                if (ret != 0) {
                    logger::log_error(DBERR_INVALID_FILETYPE, "Failed to parse the given file as WKT.");
                    return batchQueries;
                }
                break;
            case FT_CSV:
            default:
                logger::log_error(DBERR_INVALID_FILETYPE, "Unsupported file type for query file:", fileTypeStr, "Supported query types: WKT");
                break;

        }

        return batchQueries;
    }

}

