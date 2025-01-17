/**
 * 
 * Hecatoncheir API header file.
 * (c) 2024 Thanasis Georgiadis
 * This code is licensed under MIT license (see LICENSE.txt for details)
 * 
 */

#ifndef HECATONCHEIR_H
#define HECATONCHEIR_H

#include <vector>
#include <string>

namespace hec {
    
    /** 
     * @brief Spatial data types supported by Hecatoncheir. 
     * 
     */
    enum SpatialDataType{
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
    enum FileFormat {
        CSV,
        WKT,
    };

    typedef int DatasetID;
    
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
    int loadDataset(std::string &filePath, FileFormat fileType, SpatialDataType dataType);

    /**
     * @brief Partition a dataset(s) across the system. 
     * @todo: Overload with options for size of grid etc.
     */
    int partitionDataset(std::vector<DatasetID> indexes);

    /**
     * @brief Generates a hec::Dataset metadata object with the dataset's specifications as set by the user
     * The bounding box of the dataset is calculated from the data.
     */
    DatasetID prepareDataset(std::string &filePath, FileFormat fileType, SpatialDataType dataType);

    /**
     * @brief Generates a hec::Dataset metadata object with the dataset's specifications as set by the user
     * @param[in] xMin,yMin,xMax,yMax: set the bounding box of the dataset.
     */
    DatasetID prepareDataset(std::string &filePath, FileFormat fileType, SpatialDataType dataType, double xMin, double yMin, double xMax, double yMax);



}


#endif