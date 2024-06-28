#include "env/partitioning.h"
#include <bitset>

namespace partitioning
{

    void printPartitionAssignment() {
        for (int j=0; j<g_config.partitioningInfo.partitionsPerDimension; j++) {
            for (int i=0; i<g_config.partitioningInfo.partitionsPerDimension; i++) {
                printf("%d ", g_config.partitioningInfo.getNodeRankForPartitionID(g_config.partitioningInfo.getCellID(i,j)));
            }
            printf("\n");
        }
    }
    
    DB_STATUS getPartitionsForMBR(double xMin, double yMin, double xMax, double yMax, std::vector<int> &partitionIDs, std::vector<spatial_lib::TwoLayerClassE> &twoLayerClasses){
        int minPartitionX = (xMin - g_config.datasetInfo.dataspaceInfo.xMinGlobal) / (g_config.datasetInfo.dataspaceInfo.xExtent / g_config.partitioningInfo.partitionsPerDimension);
        int minPartitionY = (yMin - g_config.datasetInfo.dataspaceInfo.yMinGlobal) / (g_config.datasetInfo.dataspaceInfo.yExtent / g_config.partitioningInfo.partitionsPerDimension);
        int maxPartitionX = (xMax - g_config.datasetInfo.dataspaceInfo.xMinGlobal) / (g_config.datasetInfo.dataspaceInfo.xExtent / g_config.partitioningInfo.partitionsPerDimension);
        int maxPartitionY = (yMax - g_config.datasetInfo.dataspaceInfo.yMinGlobal) / (g_config.datasetInfo.dataspaceInfo.yExtent / g_config.partitioningInfo.partitionsPerDimension);
        
        int startPartitionID = g_config.partitioningInfo.getCellID(minPartitionX, minPartitionY);
        int lastPartitionID = g_config.partitioningInfo.getCellID(maxPartitionX, maxPartitionY);
        if (startPartitionID < 0 || startPartitionID > g_config.partitioningInfo.partitionsPerDimension*g_config.partitioningInfo.partitionsPerDimension -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Start partition ID calculated wrong");
            return DBERR_INVALID_PARTITION;
        }
        if (lastPartitionID < 0 || lastPartitionID > g_config.partitioningInfo.partitionsPerDimension*g_config.partitioningInfo.partitionsPerDimension -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Last partition ID calculated wrong");
            return DBERR_INVALID_PARTITION;
        }

        if (startPartitionID == lastPartitionID) {
            // class A only
            twoLayerClasses.emplace_back(spatial_lib::CLASS_A);
            partitionIDs.emplace_back(startPartitionID);
            return DBERR_OK;
        }
        // class A
        twoLayerClasses.emplace_back(spatial_lib::CLASS_A);
        partitionIDs.emplace_back(startPartitionID);
        // loop overlapping partitions
        for (int i=minPartitionX; i<=maxPartitionX; i++) {
            if (i != minPartitionX) {
                int partitionID = g_config.partitioningInfo.getCellID(i, minPartitionY);
                if (partitionID < 0 || partitionID > g_config.partitioningInfo.partitionsPerDimension*g_config.partitioningInfo.partitionsPerDimension -1) {
                    logger::log_error(DBERR_INVALID_PARTITION, "Partition ID calculated wrong");
                    return DBERR_INVALID_PARTITION;
                }
                partitionIDs.emplace_back(partitionID);
                // class C
                twoLayerClasses.emplace_back(spatial_lib::CLASS_C);
            }
            for (int j=minPartitionY+1; j<=maxPartitionY; j++) {
                if (i == minPartitionX) {
                    // class B
                    twoLayerClasses.emplace_back(spatial_lib::CLASS_B);
                } else {
                    // class D
                    twoLayerClasses.emplace_back(spatial_lib::CLASS_D);
                }
                int partitionID = g_config.partitioningInfo.getCellID(i, j);
                if (partitionID < 0 || partitionID > g_config.partitioningInfo.partitionsPerDimension*g_config.partitioningInfo.partitionsPerDimension -1) {
                    logger::log_error(DBERR_INVALID_PARTITION, "Partition ID calculated wrong");
                    return DBERR_INVALID_PARTITION;
                }
                partitionIDs.emplace_back(partitionID);
            }
        }
        return DBERR_OK;
    }

    static DB_STATUS initializeBatchMap(std::unordered_map<int,GeometryBatchT> &batchMap) {
        // initialize batches
        for (int i=0; i<g_world_size; i++) {
            GeometryBatchT batch;
            batch.destRank = i;
            if (i > 0) {
                batch.comm = &g_global_comm;
            } else {
                batch.comm = &g_local_comm;
            }
            batch.tag = MSG_BATCH_POLYGON;
            batch.maxObjectCount = g_config.partitioningInfo.batchSize;
            batchMap.insert(std::make_pair(i,batch));
        }
        return DBERR_OK;
    }

    /**
     * @brief Assigns a geometry to the appropriate batches based on overlapping partition IDs.
     * If the batch is full after the insertion, it is sent and cleared before returning
     * 
     * @param geometry 
     * @param geoXmin 
     * @param geoYmin 
     * @param geoXmax 
     * @param geoYmax 
     * @param batchMap 
     * @return DB_STATUS 
     */
    static DB_STATUS assignGeometryToBatches(GeometryT &geometry, double geoXmin, double geoYmin, double geoXmax, double geoYmax, std::unordered_map<int,GeometryBatchT> &batchMap, int &batchesSent) {
        // find partition IDs and the class of the geometry in each partition
        std::vector<int> partitionIDs;
        std::vector<spatial_lib::TwoLayerClassE> twoLayerClasses;
        DB_STATUS ret = partitioning::getPartitionsForMBR(geoXmin, geoYmin, geoXmax, geoYmax, partitionIDs, twoLayerClasses);
        if (ret != DBERR_OK) {
            return ret;
        }
        // set partitions to object
        geometry.setPartitions(partitionIDs, twoLayerClasses);
        // find which nodes need to receive this geometry
        std::vector<bool> bitVector(g_world_size, false);
        // get receiving worker ranks
        for (auto partitionIT = partitionIDs.begin(); partitionIT != partitionIDs.end(); partitionIT++) {
            // get node rank
            int nodeRank = g_config.partitioningInfo.getNodeRankForPartitionID(*partitionIT);
            bitVector[nodeRank] = true;
        }
        // add to nodes' batches
        for (int nodeRank=0; nodeRank<bitVector.size(); nodeRank++) {
            if (bitVector.at(nodeRank)) {
                // if it has been marked
                // add geometry to batch
                auto it = batchMap.find(nodeRank);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", nodeRank);
                    return DBERR_INVALID_PARAMETER;
                }
                GeometryBatchT *batch = &it->second;

                // make a copy of the geometry, to adjust the partitions specifically for this node
                // remove any partitions that are irrelevant to this noderank
                GeometryT geometryCopy = geometry;
                for(auto it = geometryCopy.partitions.begin(); it != geometryCopy.partitions.end();) {
                    int assignedNodeRank = g_config.partitioningInfo.getNodeRankForPartitionID(*it);
                    if (assignedNodeRank != nodeRank) {
                        it = geometryCopy.partitions.erase(it);
                        it = geometryCopy.partitions.erase(it);
                        geometryCopy.partitionCount--;
                    } else {
                        it += 2;
                    }
                }
                // if (geometryCopy.recID == 17095) {
                //     logger::log_task("  After:", geometryCopy.partitionCount);
                //     for(int j=0; j<geometryCopy.partitions.size(); j+=2) {
                //         logger::log_task("id:", geometryCopy.partitions.at(j), "class:", geometryCopy.partitions.at(j+1));
                //     }
                // }

                // add moddified geometry to this batch
                batch->addGeometryToBatch(geometryCopy);
                // if batch is full, send and reset
                if (batch->objectCount >= batch->maxObjectCount) {
                    ret = comm::controller::serializeAndSendGeometryBatch(batch);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    // reset
                    batch->clear();
                    // count batch 
                    batchesSent += 1;
                }

            }
        }
        return ret;
    }

    static DB_STATUS loadCSVDatasetAndPartition(std::string &datasetPath) {
        DB_STATUS ret = DBERR_OK;
        // initialize batches
        std::unordered_map<int,GeometryBatchT> batchMap;
        ret = initializeBatchMap(batchMap);
        if (ret != DBERR_OK) {
            return ret;
        }
        // open file
        std::ifstream fin(datasetPath);
        if (!fin.is_open()) {
            logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", datasetPath);
            return DBERR_MISSING_FILE;
        }
        std::string line;
        // read total objects (first line of file)
        std::getline(fin, line);
        fin.close();
        int polygonCount = stoi(line);
        logger::log_task("Partitioning", polygonCount, "objects...");
        // spawn as many threads as possible
        int availableProcessors = omp_get_num_procs();
        // count how many batches have been sent in total
        int batchesSent = 0;
        
        // spawn all available threads (processors)
        // todo: maybe this is not optimal
        #pragma omp parallel firstprivate(batchMap, line, polygonCount) num_threads(availableProcessors) reduction(+:batchesSent)
        {
            int recID;
            int partitionID;
            int vertexCount;
            double x,y;
            double xMin, yMin, xMax, yMax;
            DB_STATUS local_ret = DBERR_OK;
            int tid = omp_get_thread_num();
            int totalThreads = omp_get_num_threads();
            // calculate which lines this thread will handle
            int linesPerThread = (polygonCount / totalThreads);
            int fromLine = 1 + (tid * linesPerThread);          // first line is polygon count
            int toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
            if (tid == totalThreads - 1) {
                toLine = polygonCount;
            }
            // open file
            std::ifstream fin(datasetPath);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", datasetPath);
                #pragma omp cancel parallel
                ret = DBERR_MISSING_FILE;
            }
            // jump to start line
            for (int i=0; i<fromLine; i++) {
                fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
            }
            // loop
            int currentLine = fromLine;
            std::string token;
            while (true) {
                // next polygon
                std::getline(fin, line);                
                std::stringstream ss(line);
                xMin = std::numeric_limits<int>::max();
                yMin = std::numeric_limits<int>::max();
                xMax = -std::numeric_limits<int>::max();
                yMax = -std::numeric_limits<int>::max();
                // recID
                std::getline(ss, token, ',');
                recID = std::stoi(token);
                // Read the coords x,y
                vertexCount = 0;
                std::vector<double> coords;
                while (std::getline(ss, token, ',')) {
                    std::stringstream coordStream(token);
                    // Split the x and y values
                    std::getline(coordStream, token, ' ');
                    x = std::stof(token);
                    std::getline(coordStream, token, ' ');
                    y = std::stof(token);
                    // store
                    coords.emplace_back(x);
                    coords.emplace_back(y);
                    // MBR
                    xMin = std::min(xMin, x);
                    yMin = std::min(yMin, y);
                    xMax = std::max(xMax, x);
                    yMax = std::max(yMax, y);
                    vertexCount += 1;
                }
                // create serializable object
                GeometryT geometry(recID, vertexCount, coords);
                // assign to appropriate batches
                local_ret = assignGeometryToBatches(geometry, xMin, yMin, xMax, yMax, batchMap, batchesSent);
                if (ret != DBERR_OK) {
                    #pragma omp cancel parallel
                    ret = local_ret;
                }
                currentLine += 1;
                if (currentLine >= toLine) {
                    // the last line for this thread has been read
                    break;
                }
            }
            // send any remaining non-empty batches
            for (int i=0; i<g_world_size; i++) {
                // fetch batch
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    #pragma omp cancel parallel
                    ret = DBERR_INVALID_PARAMETER;
                }
                GeometryBatchT *batch = &it->second;
                if (batch->objectCount > 0) {
                    local_ret = comm::controller::serializeAndSendGeometryBatch(batch);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                    // empty the batch
                    batch->clear();
                    // count the batch
                    batchesSent += 1;
                }
            }
            // close file
            fin.close();
        }
        
        // send an empty pack to each worker to signal the end of the partitioning for this dataset
        for (int i=0; i<g_world_size; i++) {
            // fetch batch (it is guaranteed to be empty)
            auto it = batchMap.find(i);
            if (it == batchMap.end()) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                return DBERR_INVALID_PARAMETER;
            }
            GeometryBatchT *batch = &it->second;
            ret = comm::controller::serializeAndSendGeometryBatch(batch);
            if (ret != DBERR_OK) {
                return ret;
            }
        }

        logger::log_success("Sent", batchesSent, "non-empty batches.");

        return ret;
    }

    static DB_STATUS performPartitioningCSV(spatial_lib::DatasetT *dataset) {
        DB_STATUS ret = DBERR_OK;  

        // first, issue the partitioning instruction
        ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_PARTITIONING_INIT);
        if (ret != DBERR_OK) {
            return ret;
        }
        
        // then, broadcast the dataset info message
        ret = comm::controller::broadcastDatasetInfo(dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
       
        // finally, load data and partition to workers
        ret = loadCSVDatasetAndPartition(dataset->path);
        if (ret != DBERR_OK) {
            return ret;
        }

        return ret;
    }

    DB_STATUS partitionDataset(spatial_lib::DatasetT *dataset) {
        double startTime;
        DB_STATUS ret;
        // time
        startTime = mpi_timer::markTime();
        switch (dataset->fileType) {
            // perform the partitioning
            case spatial_lib::FT_CSV:
                // csv dataset
                ret = performPartitioningCSV(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                break;
            case spatial_lib::FT_BINARY:
            case spatial_lib::FT_WKT:
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported file type:", dataset->fileType);
                break;
        }

        // wait for response by workers+agent that all is ok
        ret = comm::controller::host::gatherResponses();
        if (ret != DBERR_OK) {
            return ret;
        }
        logger::log_success("Partitioning for dataset", dataset->nickname, "finished in", mpi_timer::getElapsedTime(startTime), "seconds.");
                
        return DBERR_OK;
    }
    
}