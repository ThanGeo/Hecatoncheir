#include "def.h"
#include <omp.h>

int g_world_size;
int g_node_rank;
int g_global_rank;
PROCESS_TYPE g_proc_type;

int MAX_THREADS = omp_get_max_threads();
// int MAX_THREADS = 1;

MPI_Comm g_global_inter_comm;
MPI_Comm g_global_intra_comm;
MPI_Comm g_controller_comm;
MPI_Comm g_agent_comm;
MPI_Comm g_worker_comm;

std::string WORKER_EXECUTABLE_PATH = "build/Hecatoncheir/worker";
