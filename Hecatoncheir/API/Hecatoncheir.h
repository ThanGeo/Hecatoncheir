/**
 * 
 * Hecatoncheir API header file.
 * (c) 2024 Thanasis Georgiadis
 * This code is licensed under MIT license (see LICENSE.txt for details)
 * 
 */

#ifndef HECATONCHEIR_H
#define HECATONCHEIR_H

#include "containers.h"

namespace hec {
    /** @brief Initialize function with parameters.
     * @param[in] numProcs: number of nodes to use.
     * @param[in] hosts: the hosts addresses and slots (right now, only 1 slot per node is supported)
     * @todo: make it context-encapsulated (see spark)
     */
    int init(int numProcs, const std::vector<std::string> &hosts);
 
    /** @brief Terminate Hecatoncheir
     * @details Instructs the host controller to terminate all activity. 
     * Terminates its local agent and instructs the other controllers to do the same for their agents.
     * 
     */
    int finalize();

    /**
     * @brief Prepares a dataset object for handling. Must always be called before partitionDataset() and/or loadDataset().
     * @return Assigned dataset id.
     * @details Internally, generates and assigns a Dataset object to Hecatoncheir's configuration.
     */
    DatasetID prepareDataset(std::string &filePath, std::string fileTypeStr, std::string dataTypeStr);

    /**
     * @brief Prepares a dataset object for handling, manually setting its bounding box. 
     * @param[in] xMin,yMin,xMax,yMax: set the bounding box of the dataset.
     * @return Assigned dataset id.
     * @details Internally, generates and assigns a Dataset object to Hecatoncheir's configuration.
     */
    DatasetID prepareDataset(std::string &filePath, std::string fileTypeStr, std::string dataTypeStr, double xMin, double yMin, double xMax, double yMax);


    /**
     * @brief Partition dataset(s) across the system. 
     */
    int partition(std::vector<DatasetID> indexes);

    /**
     * @brief Load dataset(s) across the system. 
     * @todo: make an Unload function as well.
     */
    int load(std::vector<DatasetID> indexes);

    /** @brief Run a query described by the passed object. */
    hec::QueryResult query(Query* query);

    /** @brief Run queries in batches. */
    std::vector<size_t> query(std::vector<Query> &queryBatch);

}


#endif