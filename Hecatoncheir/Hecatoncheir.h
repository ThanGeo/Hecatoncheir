#ifndef HECATONCHEIR_H
#define HECATONCHEIR_H

#include "def.h"
#include "utils.h"
#include "env/comm_def.h"

namespace hecatoncheir {
    
    /** @brief Initialize function with parameters.
     * 
     * @param[in] argc: as given at program execution.
     * @param[in] argv: ad given at program execution. 
     * Must have specified -np <N> for nodes and --hostfile <hostfile> with the nodes' names 
     * 
     * @return 0 for success, error code otherwise.
     */
    int init(int numProcs, const std::vector<std::string> &hosts);
 

    /** @brief Terminate function */
    int finalize();
}


#endif