#ifndef D_PROC_H
#define D_PROC_H
#include <mpi.h>

#include "def.h"

typedef struct ErrorReport {
    int contents[2];
    int* workerRank = &contents[0];
    int* errorCode = &contents[1];
} ErrorReportT;

namespace proc
{
    extern MPI_Comm g_intercomm;
   
    /**
     * @brief sets up the processes in the host node
     */
    DB_STATUS setupProcesses();
    

}

#endif