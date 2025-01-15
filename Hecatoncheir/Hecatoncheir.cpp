#include "Hecatoncheir.h"

static DB_STATUS probeBlocking(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
    int mpi_ret = MPI_Probe(sourceRank, tag, comm, &status);
    if(mpi_ret != MPI_SUCCESS) {
        logger::log_error(DBERR_COMM_PROBE_FAILED, "Blocking probe failed");
        return DBERR_COMM_PROBE_FAILED;
    }
    return DBERR_OK;
}

DB_STATUS receiveResponse(int sourceRank, int sourceTag, MPI_Comm &comm, MPI_Status &status) {
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
    DB_STATUS ret = probeBlocking(HOST_GLOBAL_RANK, MPI_ANY_TAG, g_global_intra_comm, status);
    if (ret != DBERR_OK) {
        return ret;
    }
    // receive response
    ret = receiveResponse(HOST_GLOBAL_RANK, status.MPI_TAG, g_global_intra_comm, status);
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
    int rank;
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
        g_global_rank = rank;
        logger::log_success("Set my rank to", g_global_rank);

        // release inter-comm
        MPI_Comm_free(&g_global_inter_comm);

        // wait for ACK
        MPI_Status status;
        ret = waitForResponse();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System initialization failed.");
            return ret;
        }
    }

    logger::log_success("System init complete.");
    return ret;
}

namespace hecatoncheir {

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
        // controller::hostTerminate();
        return 0;
    }
}

