#include "def.h"

ConfigT g_config;

int g_world_size;
int g_node_rank;
int g_host_rank = HOST_RANK;
int g_parent_original_rank;

MPI_Comm g_global_comm = MPI_COMM_WORLD;
MPI_Comm g_local_comm;

std::string actionIntToStr(ActionTypeE action) {
    switch (action) {
        case ACTION_NONE:
            return "NONE";
        case ACTION_LOAD_DATASETS:
            return "LOAD DATASETS";
        case ACTION_PERFORM_PARTITIONING:
            return "PERFORM PARTITIONING";
        case ACTION_CREATE_APRIL:
            return "CREATE APRIL";
        case ACTION_PERFORM_VERIFICATION:
            return "PERFORM VERIFICATION";
        case ACTION_QUERY:
            return "QUERY";
        case ACTION_LOAD_APRIL:
            return "LOAD APRIL";
        default:
            return "";
    }
}