#ifndef HECATONCHEIR_H
#define HECATONCHEIR_H

#include <vector>
#include <string>

namespace hec {
    
    /** 
     * @brief Spatial data types supported by Hecatoncheir. 
     * 
     */
    enum DataType{
        POINT,
        LINESTRING,
        RECTANGLE,
        POLYGON,
    };

    /** @brief Input file types supported by Hecatoncheir.
     * @details 
     * - CSV assumes there is no header line in the beginning of the file.
     * - WKT assumes that the file is indeed a CSV, but the first column is a WKT with the shape (disregards the rest)
     */
    enum FileType {
        CSV,
        WKT,
    };

    /** @brief Initialize function with parameters.
     * @param[in] numProcs: number of nodes to use.
     * @param[in] hosts: the hosts addresses and slots (right now, only 1 slot per node is supported)
     */
    int init(int numProcs, const std::vector<std::string> &hosts);
 
    /** @brief Terminate Hecatoncheir
     * @details Instructs the host controller to terminate all activity. 
     * Terminates its local agent and instructs the other controllers to do the same for their agents.
     * 
     */
    int finalize();

    /**
     * @brief Load a dataset in memory.
     * @param[in] filePath: Full path to the dataset
     * @param[in] fileType: Type of the file at path.
     * @param[in] dataType: Spatial data type of the objects in the dataset. (non-compatible objects are ignored for now)
     */
    int loadDataset(std::string &filePath, FileType fileType, DataType dataType);

    /**
     * @brief Partition a dataset across the system. 
     * @param[in] filePath: Full path to the dataset
     * @param[in] fileType: Type of the file at path.
     * @param[in] dataType: Spatial data type of the objects in the dataset. (non-compatible objects are ignored for now)
     * @todo: Overload with options for size of grid etc.
     */
    int partitionDataset(std::string &filePath, FileType fileType, DataType dataType);
}


#endif