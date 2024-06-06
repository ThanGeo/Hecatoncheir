#include "env/partitioning.h"

namespace partitioning
{
    // load space
    double coordLoadSpace[1000000];

    void printPartitionAssignment() {
        for (int j=0; j<g_config.partitioningInfo.partitionsPerDimension; j++) {
            for (int i=0; i<g_config.partitioningInfo.partitionsPerDimension; i++) {
                printf("%d ", g_config.partitioningInfo.getNodeRankForPartitionID(g_config.partitioningInfo.getCellID(i,j)));
            }
            printf("\n");
        }
    }
    
    DB_STATUS getPartitionsForMBR(double xMin, double yMin, double xMax, double yMax, std::vector<int> &partitionIDs){
        int minPartitionX = (xMin - g_config.datasetInfo.dataspaceInfo.xMinGlobal) / (g_config.datasetInfo.dataspaceInfo.xExtent / g_config.partitioningInfo.partitionsPerDimension);
        int minPartitionY = (yMin - g_config.datasetInfo.dataspaceInfo.yMinGlobal) / (g_config.datasetInfo.dataspaceInfo.yExtent / g_config.partitioningInfo.partitionsPerDimension);
        int maxPartitionX = (xMax - g_config.datasetInfo.dataspaceInfo.xMinGlobal) / (g_config.datasetInfo.dataspaceInfo.xExtent / g_config.partitioningInfo.partitionsPerDimension);
        int maxPartitionY = (yMax - g_config.datasetInfo.dataspaceInfo.yMinGlobal) / (g_config.datasetInfo.dataspaceInfo.yExtent / g_config.partitioningInfo.partitionsPerDimension);
        for (int i=minPartitionX; i<=maxPartitionX; i++) {
            for (int j=minPartitionY; j<=maxPartitionY; j++) {
                int partitionID = g_config.partitioningInfo.getCellID(i, j);
                if (partitionID < 0) {
                    logger::log_error(DBERR_INVALID_PARTITION, "Partition id calculated to be less than zero");
                    return DBERR_INVALID_PARTITION;
                }
                if (partitionID > g_config.partitioningInfo.partitionsPerDimension*g_config.partitioningInfo.partitionsPerDimension -1) {
                    logger::log_error(DBERR_INVALID_PARTITION, "Partition id calculated to be larger than numberOfPartitions^2 -1");
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
    static DB_STATUS assignGeometryToBatches(GeometryT &geometry, double geoXmin, double geoYmin, double geoXmax, double geoYmax, std::unordered_map<int,GeometryBatchT> &batchMap) {
        // find partition IDs
        std::vector<int> partitionIDs;
        DB_STATUS ret = partitioning::getPartitionsForMBR(geoXmin, geoYmin, geoXmax, geoYmax, partitionIDs);
        if (ret != DBERR_OK) {
            return ret;
        }

        // add to buffers for the batch
        for (auto partitionIT = partitionIDs.begin(); partitionIT != partitionIDs.end(); partitionIT++) {
            // set partitionID
            geometry.setPartitionID(*partitionIT);
            // get node rank responsible
            int nodeRank = g_config.partitioningInfo.getNodeRankForPartitionID(*partitionIT);
            // add geometry to batch
            auto it = batchMap.find(nodeRank);
            if (it == batchMap.end()) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", nodeRank);
                return DBERR_INVALID_PARAMETER;
            }
            GeometryBatchT *batch = &it->second;
            batch->addGeometryToBatch(geometry);
            // if batch is full, send and reset
            if (batch->objectCount >= batch->maxObjectCount) {
                ret = comm::controller::serializeAndSendGeometryBatch(batch);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // reset
                batch->clear();
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
        // todo calculate how many threads to spawn based on total object count
        int polygonCount;
        // open file
        std::ifstream fin(datasetPath);
        if (!fin.is_open()) {
            logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", datasetPath);
            return DBERR_MISSING_FILE;
        }
        std::string line;
        // read total objects (first line)
        std::getline(fin, line);
        fin.close();
        polygonCount = stoi(line);
        int availableProcessors = omp_get_num_procs();
        
        // spawn all available threads (processors)
        #pragma omp parallel firstprivate(batchMap, line, polygonCount) num_threads(availableProcessors)
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
                int vertexNum = 0;
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
                    vertexNum += 1;
                }
                // create serializable object
                GeometryT geometry(recID, 0, vertexCount, coords);
                // assign to appropriate batches
                local_ret = assignGeometryToBatches(geometry, xMin, yMin, xMax, yMax, batchMap);
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

        return ret;
    }

    static DB_STATUS loadBinaryDatasetAndPartition(std::string &datasetPath) {
        int polygonCount;
        int recID;
        int partitionID;
        int vertexCount;
        double x,y;
        double xMin, yMin, xMax, yMax;
        DB_STATUS ret = DBERR_OK;

        // open dataset file stream
        FILE* pFile = fopen(datasetPath.c_str(), "rb");
        if (pFile == NULL) {
            logger::log_error(DBERR_MISSING_FILE, "Couldnt open binary dataset file:", datasetPath);
            return DBERR_MISSING_FILE;
        }

        //first read the total polygon count of the dataset
        fread(&polygonCount, sizeof(int), 1, pFile);

        // initialize batches
        std::unordered_map<int,GeometryBatchT> batchMap;
        ret = initializeBatchMap(batchMap);
        if (ret != DBERR_OK) {
            return ret;
        }

        //read polygons
        int intBuf[2];
        for(int i=0; i<polygonCount; i++){
            //read the polygon id and vertex count
            fread(&intBuf, sizeof(int), 2, pFile);
            recID = intBuf[0];
            vertexCount = intBuf[1];

            xMin = std::numeric_limits<int>::max();
            yMin = std::numeric_limits<int>::max();
            xMax = -std::numeric_limits<int>::max();
            yMax = -std::numeric_limits<int>::max();

            // read the points
            std::vector<double> coords(vertexCount*2);
            fread(&coordLoadSpace, sizeof(double), vertexCount*2, pFile);
            // compute MBR and store
            for (int j=0; j<vertexCount; j+=2) {
                xMin = std::min(xMin, coordLoadSpace[j]);
                yMin = std::min(yMin, coordLoadSpace[j+1]);
                xMax = std::max(xMax, coordLoadSpace[j]);
                yMax = std::max(yMax, coordLoadSpace[j+1]);

                coords[j] = coordLoadSpace[j];
                coords[j+1] = coordLoadSpace[j+1];
            }

            // create serializable object
            GeometryT geometry(recID, 0, vertexCount, coords);

            // assign to appropriate batches
            ret = assignGeometryToBatches(geometry, xMin, yMin, xMax, yMax, batchMap);
            if (ret != DBERR_OK) {
                goto CLOSE_AND_RETURN;
            }
        }

        // send any remaining batches
        for (int i=0; i<g_world_size; i++) {
            // fetch batch
            auto it = batchMap.find(i);
            if (it == batchMap.end()) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                ret = DBERR_INVALID_PARAMETER;
                goto CLOSE_AND_RETURN;
            }
            GeometryBatchT *batch = &it->second;
            ret = comm::controller::serializeAndSendGeometryBatch(batch);
            if (ret != DBERR_OK) {
                goto CLOSE_AND_RETURN;
            }
            // send also an empty pack to signify the end of the partitioning for this dataset
            if (batch->objectCount > 0) {     
                // reset and send empty (final message)
                batch->clear();
                ret = comm::controller::serializeAndSendGeometryBatch(batch);
                if (ret != DBERR_OK) {
                    goto CLOSE_AND_RETURN;
                }
            }
        }

CLOSE_AND_RETURN:
        fclose(pFile);
        return ret;
    }

    static DB_STATUS performPartitioningBinary(spatial_lib::DatasetT *dataset) {
        DB_STATUS ret = DBERR_OK;  

        // first, broadcast the dataset info message
        ret = comm::controller::broadcastDatasetInfo(dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
       
        // load data and partition
        ret = loadBinaryDatasetAndPartition(dataset->path);
        if (ret != DBERR_OK) {
            return ret;
        }

        return ret;
    }

    static DB_STATUS performPartitioningCSV(spatial_lib::DatasetT *dataset) {
         DB_STATUS ret = DBERR_OK;  

        // first, broadcast the dataset info message
        ret = comm::controller::broadcastDatasetInfo(dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
       
        // load data and partition
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
            case spatial_lib::FT_BINARY:
                // binary dataset
                ret = performPartitioningBinary(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                break;
            case spatial_lib::FT_CSV:
                // csv dataset
                ret = performPartitioningCSV(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                break;
            case spatial_lib::FT_WKT:
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported file type:", dataset->fileType);
                break;
        }

        // wait for response by workers+agent that all is ok
        ret = comm::controller::gatherResponses();
        if (ret != DBERR_OK) {
            return ret;
        }
        logger::log_success("Partitioning for dataset", dataset->nickname, "finished in", mpi_timer::getElapsedTime(startTime), "seconds.");
                
        return DBERR_OK;
    }
    
}