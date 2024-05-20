#ifndef D_PROC_H
#define D_PROC_H

#include "def.h"

typedef struct ErrorReport {
    int contents[2];
    int* workerRank = &contents[0];
    int* errorCode = &contents[1];
} ErrorReportT;

namespace proc
{   
    /**
     * @brief sets up the processes in the host node
     */
    DB_STATUS setupProcesses();
    

}

#endif