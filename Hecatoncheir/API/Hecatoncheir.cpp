#include "Hecatoncheir.h"

#include "fstream"

#include <regex>
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

static DB_STATUS receiveResult(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status, hec::QResultBase* finalResults) {
    SerializedMsg<char> msg(MPI_CHAR);
    // receive message
    DB_STATUS ret = comm::recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
    if (ret != DBERR_OK) {
        return ret;
    }
    // unpack
    finalResults->deserialize(msg.data, msg.count);
    // free memory
    msg.clear();
    return ret;
}

static DB_STATUS receiveResultBatch(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status, std::unordered_map<int,hec::QResultBase*> &finalResults) {
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
    // free memory
    msg.clear();
    return DBERR_OK;
}

static DB_STATUS waitForResponse() {
    MPI_Status status;
    // wait for response by the host controller
    DB_STATUS ret = probeBlocking(HOST_CONTROLLER, MPI_ANY_TAG, g_global_intra_comm, status);
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
        logger::log_error(DBERR_COMM_RECEIVED_NACK, "Received NACK by host controller");
        return DBERR_COMM_RECEIVED_NACK;
    }
    return ret;
}    

static DB_STATUS waitForResult(hec::QResultBase* finalResults) {
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

static DB_STATUS waitForResult(std::unordered_map<int, hec::QResultBase*> &finalResults) {
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
        for (int i=0; i<num_procs; i++) {
            MPI_Info_free(&info[i]);
        }
        free(info);
        free(np);
        free(error_codes);


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

static DB_STATUS spawnWorkers(int num_procs, const std::vector<std::string> &hosts) {
    DB_STATUS ret = DBERR_OK;
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        std::vector<char*> cmdsVec(num_procs, (char*) WORKER_EXECUTABLE_PATH.c_str());
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
        for (int i=0; i<num_procs; i++) {
            MPI_Info_free(&info[i]);
        }
        free(info);
        free(np);
        free(error_codes);


        // merge inter-comm to intra-comm
        MPI_Intercomm_merge(g_global_inter_comm, 0, &g_global_intra_comm);
        // spit intra-comm to groups
        MPI_Comm_split(g_global_intra_comm, MPI_UNDEFINED, 0, &g_worker_comm);
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
    DB_STATUS ret = comm::send::sendInstructionMessage(HOST_CONTROLLER, MSG_INSTR_FIN, g_global_intra_comm);
    if (ret != DBERR_OK) {
        logger::log_error(DBERR_COMM_SEND, "Send message with tag", MSG_INSTR_FIN);
        return DBERR_COMM_SEND;
    }
    // get ack for termination? @todo
    ret = waitForResponse();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Finalization failed with errors.");
        return ret;
    }        
    // syncrhonize with host controller
    MPI_Barrier(g_global_intra_comm);
    MPI_Finalize();
    logger::log_success("System finalization complete.");
    return DBERR_OK;
}

namespace hec {

    static DB_STATUS fixHosts(std::vector<std::string> &hosts) {
        for (auto &host : hosts) {
            if (host.find(':') == std::string::npos) {
                host += ":1";
            }
        }    
        return DBERR_OK;
    }

    static std::vector<std::string> getExactBinaryPIDs(const std::string& binaryPath) {
        std::vector<std::string> pids;
        std::string cmd = "ps -eo pid,args | grep \"" + binaryPath + "\" | grep -v grep";

        std::array<char, 256> buffer;
        std::string line;

        std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
        if (!pipe) {
            std::cerr << "popen() failed!" << std::endl;
            return pids;
        }

        while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
            line = buffer.data();

            std::istringstream iss(line);
            std::string pid;
            std::string cmd_part;

            // Extract PID
            iss >> pid;

            // Remainder is full command
            std::getline(iss, cmd_part);
            size_t start = cmd_part.find_first_not_of(" \t");
            if (start != std::string::npos) {
                cmd_part = cmd_part.substr(start);
            }

            // Check if the command starts exactly with the binary path
            if (cmd_part.rfind(binaryPath, 0) == 0) { // starts with
                pids.push_back(pid);
            }
        }

        return pids;
    }

    int init(int numProcs, std::vector<std::string> &hosts){
        DB_STATUS ret = DBERR_OK;
        // fix hosts
        ret = fixHosts(hosts);
        if (ret != DBERR_OK) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Invalid hosts, unable to fix.");
            return ret;
        }
        
        // init MPI
        int provided;
        int mpi_ret = MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &provided);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init MPI failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        // set process type
        g_proc_type = PT_DRIVER;

        // spawn the workers in the hosts
        ret = spawnWorkers(numProcs, hosts);
        if (ret != DBERR_OK) {
            return -1;
        }

        std::vector<std::string> all_pids;
        for (const std::string& proc : {"build/Hecatoncheir/worker", "build/driver/driver"}) {
            std::vector<std::string> pids = getExactBinaryPIDs(proc);
            for (const auto& pid : pids) {
                all_pids.push_back(pid);
                logger::log_success("Process", proc, "PID:", pid);
            }
        }

        // all ok
        return ret;
    }

    int finalize() {
        DB_STATUS ret = terminate();
        if (ret != DBERR_OK) {
            // finished with errors
            logger::log_error(ret, "Termination finished with errors.");
            return -1;
        }
        return ret;
    }

    DatasetID prepareDataset(std::string filePath, std::string fileTypeStr, std::string dataTypeStr, bool persist) {
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

        // free memory
        msg.clear();

        // wait for response with dataset internal ID
        MPI_Status status;
        SerializedMsg<char> msgFromHost;
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
        ret = unpack::unpackValues(msgFromHost, indexes);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Error unpacking dataset indexes.");
            return ret;
        }

        // free
        msgFromHost.clear();

        // return the ID
        if (indexes.size() != 1) {
            logger::log_error(DBERR_INVALID_PARAMETER, "The unpacked id list does not contain a single id.");
            return DBERR_INVALID_PARAMETER;
        }

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
        msg.clear();

        // wait for response with dataset internal ID
        MPI_Status status;
        SerializedMsg<char> msgFromHost;
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
        ret = unpack::unpackValues(msgFromHost, indexes);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Error unpacking dataste indexes.");
            return ret;
        }

        // free
        msgFromHost.clear();

        // return the ID
        return indexes[0];
    }

    int unloadDataset(DatasetID datasetID) {
        SerializedMsg<char> msg(MPI_CHAR);
        DB_STATUS ret = pack::packValues(msg, datasetID);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Packing dataset id failed.");
            return -1;
        }

        // send
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_UNLOAD_DATASET, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset metadata message failed.");
            return -1;
        }
        // free memory
        msg.clear();

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Partitioning finished with errors.");
            return -1;
        }        
        logger::log_success("Unloaded dataset.");
        return ret;
    }

    int partition(std::vector<DatasetID> datasetIndexes) {
        SerializedMsg<char> msg(MPI_CHAR);
        DB_STATUS ret = pack::packValues(msg, datasetIndexes);
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
        // free memory
        msg.clear();

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Partitioning finished with errors.");
            return -1;
        }        
        logger::log_success("Partitioned datasets.");
        return ret;
    }

    int buildIndex(std::vector<DatasetID> datasetIndexes, IndexType indexType) {
        SerializedMsg<char> msg(MPI_CHAR);
        std::vector<int> indexTypeAndDatasets = {indexType};
        indexTypeAndDatasets.insert(indexTypeAndDatasets.end(), datasetIndexes.begin(), datasetIndexes.end());

        DB_STATUS ret = pack::packValues(msg, indexTypeAndDatasets);
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
        // free memory
        msg.clear();

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Building index finished with errors.");
            return -1;
        }        
        logger::log_success("Built index.");
        return ret;
    }

    int load(std::vector<DatasetID> datasetIndexes) {
        SerializedMsg<char> msg(MPI_CHAR);
        DB_STATUS ret = pack::packValues(msg, datasetIndexes);
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
        // free memory
        msg.clear();
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

    hec::QResultBase* query(Query* query) {
        DB_STATUS ret = DBERR_OK;        
        SerializedMsg<char> msg(MPI_CHAR);
        hec::QResultBase* qResPtr;        
        switch (query->getQueryType()) {
            case Q_RANGE:
                // create query result object
                switch (query->getResultType()) {
                    case QR_COUNT:
                        qResPtr = new QResultCount(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    case QR_COLLECT:
                        qResPtr = new QResultCollect(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for RANGE query:", query->getResultType());
                        return nullptr;
                }
                break;
            case Q_INTERSECTION_JOIN:
            case Q_INSIDE_JOIN:
            case Q_DISJOINT_JOIN:
            case Q_EQUAL_JOIN:
            case Q_MEET_JOIN:
            case Q_CONTAINS_JOIN:
            case Q_COVERS_JOIN:
            case Q_COVERED_BY_JOIN:
            case Q_DISTANCE_JOIN:
                // create query result object
                switch (query->getResultType()) {
                    case QR_COUNT:
                        qResPtr = new QResultCount(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    case QR_COLLECT:
                        qResPtr = new QPairResultCollect(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for JOIN query:", query->getResultType());
                        return nullptr;
                }
                break;
            case Q_FIND_RELATION_JOIN:
                // create query result object
                switch (query->getResultType()) {
                    case QR_COUNT:
                        qResPtr = new QTopologyResultCount(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    case QR_COLLECT:
                        qResPtr = new QTopologyResultCollect(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for FIND RELATION query:", query->getResultType());
                        return nullptr;
                }
                break;
            case Q_KNN:
                switch (query->getResultType()) {
                    case QR_KNN:
                        qResPtr = new QResultkNN(query->getQueryID(), query->getK());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for KNN query:", query->getResultType());
                        return nullptr;
                }
                break;
            default:
                // error
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", query->getQueryType());
                return nullptr;
        }
        // pack query info
        int res = query->serialize(&msg.data, msg.count);
        if (res < 0) {
            logger::log_error(DBERR_SERIALIZE_FAILED, "Failed to serialize query.");
            return nullptr;
        }
        // send the query to Host Controller
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_QUERY, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset load message failed.");
            return nullptr;
        }
        // free memory
        msg.clear();
        // wait for result
        ret = waitForResult(qResPtr);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Receiving query results failed.");
            return qResPtr;
        }
        logger::log_success("Evaluated query!");
        return qResPtr;
    }

    std::unordered_map<int, hec::QResultBase*> query(std::vector<Query*> &queryBatch, hec::QueryType batchType) {
        DB_STATUS ret = DBERR_OK;
        std::unordered_map<int, hec::QResultBase*> finalResults;

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
        switch (batchType) {
            case Q_RANGE:
                ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_QUERY_BATCH_RANGE, g_global_intra_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Sending dataset load message failed.");
                    return finalResults;
                }
                break;
            case Q_KNN:
                ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_QUERY_BATCH_KNN, g_global_intra_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Sending dataset load message failed.");
                    return finalResults;
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported query type for batch processing:", batchType);
                return finalResults;
        }
        // free memory
        msg.clear();
        // wait for result
        ret = waitForResult(finalResults);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Receiving query results failed.");
            return finalResults;
        }
        logger::log_success("Evaluated query batch!");
        return finalResults;
    }

    namespace range_queries 
    {
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

                // logger::log_success("Loaded range query:", query->getWKT(), query->getShapeType());
    
                queryID++;
            }
    
            fin.close();
    
            return 0;
        }
    }

    std::vector<hec::Query*> loadRangeQueriesFromFile(std::string filePath, std::string fileTypeStr, int datasetID, hec::QueryResultType resultType){
        std::vector<hec::Query*> batchQueries;
        int ret = 0;
        hec::FileType fileType = mapping::fileTypeTextToInt(fileTypeStr);
        switch (fileType) {
            case FT_WKT:
                ret = range_queries::loadQueriesFromWKT(filePath, datasetID, resultType, batchQueries);
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

    namespace knn_queries 
    {
        static int loadQueriesFromWKT(std::string &filePath, int datasetID, int k, std::vector<hec::Query*> &batchQueries) {
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
    
                hec::KNNQuery* query = new hec::KNNQuery(datasetID, queryID, token, k);
                batchQueries.emplace_back(query);
    
                queryID++;
            }
    
            fin.close();
    
            return 0;
        }
    }

    std::vector<hec::Query*> loadKNNQueriesFromFile(std::string filePath, std::string fileTypeStr, int datasetID, int k) {
        std::vector<hec::Query*> batchQueries;
        int ret = 0;
        hec::FileType fileType = mapping::fileTypeTextToInt(fileTypeStr);
        switch (fileType) {
            case FT_WKT:
                ret = knn_queries::loadQueriesFromWKT(filePath, datasetID, k, batchQueries);
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

    namespace time {

        double getTime() {
            return MPI_Wtime();
        }
    }

}

