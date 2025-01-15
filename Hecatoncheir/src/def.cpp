#include "def.h"

int g_world_size;
int g_node_rank;
int g_parent_original_rank;
PROCESS_TYPE g_proc_type;

MPI_Comm g_global_comm;
MPI_Comm g_controller_comm;
MPI_Comm g_agent_comm;

std::string AGENT_EXECUTABLE_PATH = "build/Hecatoncheir/agent";
std::string CONTROLLER_EXECUTABLE_PATH = "build/Hecatoncheir/controller";
