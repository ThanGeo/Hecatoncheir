#include "def.h"
#include <omp.h>

int g_world_size;
int g_node_rank;
int g_global_rank;
int g_parent_original_rank;
PROCESS_TYPE g_proc_type;

int MAX_THREADS = omp_get_max_threads();
// int MAX_THREADS = 1;

MPI_Comm g_global_inter_comm;
MPI_Comm g_global_intra_comm;
MPI_Comm g_controller_comm;
MPI_Comm g_agent_comm;

std::string AGENT_EXECUTABLE_PATH = "build/Hecatoncheir/agent";
std::string CONTROLLER_EXECUTABLE_PATH = "build/Hecatoncheir/controller";
