#include "Hecatoncheir.h"

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
        
        // sync controllers
        logger::log_task("Waiting for controllers...");
        MPI_Barrier(g_global_intra_comm);
    }

    printf("Driver: Controllers spawned and synchronized.\n");
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

