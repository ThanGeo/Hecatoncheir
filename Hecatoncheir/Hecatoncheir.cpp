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

static DB_STATUS spawnControllers(int num_procs, const std::vector<std::string> &hosts) {
    DB_STATUS ret = DBERR_OK;
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) {
        char *cmds[2] = { (char*) CONTROLLER_EXECUTABLE_PATH.c_str(), (char*) CONTROLLER_EXECUTABLE_PATH.c_str() };
        int* error_codes = (int*)malloc(num_procs * sizeof(int));
        MPI_Info* info = (MPI_Info*)malloc(num_procs * sizeof(MPI_Info));
        int* np = (int*)malloc(num_procs * sizeof(int));

        // set spawn info
        for (int i=0; i<num_procs; i++) {
            // num procs per spawn
            np[i] = 1;
            // Set the host for each process using MPI_Info_set
            MPI_Info_create(&info[i]);
            MPI_Info_set(info[i], "host", hosts[i].c_str());
        }

        // spawn the controllers
        MPI_Comm_spawn_multiple(num_procs, cmds, NULL, np, info, 0, MPI_COMM_WORLD, &g_global_inter_comm, error_codes);
        for (int i = 0; i < num_procs; ++i) {
            if (error_codes[i] != MPI_SUCCESS) {
                logger::log_error(DBERR_MPI_INIT_FAILED, "Failed while spawning the controllers.");
                return DBERR_MPI_INIT_FAILED;
            }
        }

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

static DB_STATUS initiateDataPartitioning(std::vector<hec::DatasetID> &datasets) {

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

    DatasetID prepareDataset(std::string &filePath, FileFormat fileType, SpatialDataType dataType) {
        if (filePath == "") {
            // empty path, empty dataset
            return -1;
        }
        // set metadata and serialize
        SerializedMsg<char> msg(MPI_CHAR);
        DatasetMetadata metadata;
        metadata.internalID = DATASET_R;
        metadata.path = filePath;
        metadata.fileType = (FileType) fileType;
        metadata.dataType = (DataType) dataType;
        DB_STATUS ret = metadata.serialize(&msg.data, msg.count);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Metadata serialization failed.");
            return -1;
        }

        // send message to Host Controller to init the dataset's struct and get an ID for the dataset
        ret = comm::send::sendMessage(msg, HOST_CONTROLLER, MSG_DATASET_METADATA, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Sending dataset metadata message failed.");
            return -1;
        }

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

        // return the ID
        return indexes[0];
    }

    DatasetID prepareDataset(std::string &filePath, FileFormat fileType, SpatialDataType dataType, double xMin, double yMin, double xMax, double yMax) {
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

        // communicate with the system to init the dataset's struct and get an ID

        // return the ID

        return 0;
    }

    int partitionDataset(DatasetID &dataset) {
        std::vector<DatasetID> vec = {dataset};
        DB_STATUS ret = initiateDataPartitioning(vec);
        if (ret != DBERR_OK) {
            return -1;
        }
        return 0;
    }

    int partitionDataset(DatasetID &datasetR, DatasetID &datasetS) {
        std::vector<DatasetID> vec = {datasetR, datasetS};
        DB_STATUS ret = initiateDataPartitioning(vec);
        if (ret != DBERR_OK) {
            return -1;
        }
        return 0;
    }
}

