#include "def.h"

ConfigT g_config;

int g_world_size;
int g_node_rank;
int g_host_rank = HOST_RANK;
int g_parent_original_rank;

MPI_Comm g_global_comm = MPI_COMM_WORLD;
MPI_Comm g_local_comm;