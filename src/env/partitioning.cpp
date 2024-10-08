#include "env/partitioning.h"
#include <bitset>

namespace partitioning
{
    void printPartitionAssignment() {
        for (int j=0; j<g_config.partitioningMethod->getDistributionPPD(); j++) {
            for (int i=0; i<g_config.partitioningMethod->getDistributionPPD(); i++) {
                printf("%d ", g_config.partitioningMethod->getNodeRankForPartitionID(g_config.partitioningMethod->getDistributionGridPartitionID(i,j)));
            }
            printf("\n");
        }
    }
    
    /**
    @brief Calculates the intersecting partitions in the distribution grid for the given MBR.
     * @param[in] xMin, yMin, xMax, yMax MBR
     * @param[out] partitionIDs The partition IDs that intersect with the MBR.
     * @param[out] twoLayerClasses The MBR's two-layer index classification for each individual intersecting partition.
     */
    static DB_STATUS getPartitionsForMBR(MBR &mbr, std::vector<int> &partitionIDs){
        int minPartitionX = (mbr.pMin.x - g_config.datasetMetadata.dataspaceMetadata.xMinGlobal) / (g_config.datasetMetadata.dataspaceMetadata.xExtent / g_config.partitioningMethod->getDistributionPPD());
        int minPartitionY = (mbr.pMin.y - g_config.datasetMetadata.dataspaceMetadata.yMinGlobal) / (g_config.datasetMetadata.dataspaceMetadata.yExtent / g_config.partitioningMethod->getDistributionPPD());
        int maxPartitionX = (mbr.pMax.x - g_config.datasetMetadata.dataspaceMetadata.xMinGlobal) / (g_config.datasetMetadata.dataspaceMetadata.xExtent / g_config.partitioningMethod->getDistributionPPD());
        int maxPartitionY = (mbr.pMax.y - g_config.datasetMetadata.dataspaceMetadata.yMinGlobal) / (g_config.datasetMetadata.dataspaceMetadata.yExtent / g_config.partitioningMethod->getDistributionPPD());
        
        int startPartitionID = g_config.partitioningMethod->getDistributionGridPartitionID(minPartitionX, minPartitionY);
        int lastPartitionID = g_config.partitioningMethod->getDistributionGridPartitionID(maxPartitionX, maxPartitionY);
        if (startPartitionID < 0 || startPartitionID > g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD() -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Start partition ID calculated wrong");
            return DBERR_INVALID_PARTITION;
        }
        if (lastPartitionID < 0 || lastPartitionID > g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD() -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Last partition ID calculated wrong: MBR(", mbr.pMin.x, mbr.pMin.y, mbr.pMax.x, mbr.pMax.y, ")");
            return DBERR_INVALID_PARTITION;
        }
        // create the distribution grid partition list for the object
        for (int i=minPartitionX; i<=maxPartitionX; i++) {
            for (int j=minPartitionY; j<=maxPartitionY; j++) {
                int partitionID = g_config.partitioningMethod->getDistributionGridPartitionID(i, j);
                partitionIDs.emplace_back(partitionID);
            }
        }
        return DBERR_OK;
    }

    static DB_STATUS initializeBatchMap(std::unordered_map<int,Batch> &batchMap, DataTypeE dataType) {
        // initialize batches
        for (int i=0; i<g_world_size; i++) {
            Batch batch;
            batch.dataType = dataType;
            switch (dataType) {
                case DT_POINT:
                    batch.tag = MSG_BATCH_POINT;
                    break;
                case DT_LINESTRING:
                    batch.tag = MSG_BATCH_LINESTRING;
                    break;
                case DT_POLYGON:
                    batch.tag = MSG_BATCH_POLYGON;
                    break;
                case DT_RECTANGLE:
                default:
                    logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype for batch:", dataType);
                    return DBERR_INVALID_DATATYPE;
            }
            batch.destRank = i;
            if (i > 0) {
                batch.comm = &g_global_comm;
            } else {
                batch.comm = &g_local_comm;
            }
            batch.maxObjectCount = g_config.partitioningMethod->getBatchSize();
            batchMap[i] = batch;
        }
        return DBERR_OK;
    }

    /** @brief Assigns a geometry to the appropriate batches based on overlapping partition IDs.
     * If the batch is full after the insertion, it is sent and cleared before returning.
     */
    static DB_STATUS assignObjectToBatches(Shape &object, std::unordered_map<int,Batch> &batchMap, int &batchesSent) {
        // find partition IDs and the class of the geometry in each distribution partition
        std::vector<int> partitionIDs;
        DB_STATUS ret = partitioning::getPartitionsForMBR(object.mbr, partitionIDs);
        if (ret != DBERR_OK) {
            return ret;
        }
        // set partitions to object
        object.initPartitions(partitionIDs);
        // find which nodes need to receive this geometry
        std::vector<bool> bitVector(g_world_size, false);
        // get receiving worker ranks
        for (auto partitionIT = partitionIDs.begin(); partitionIT != partitionIDs.end(); partitionIT++) {
            // get node rank from distribution grid
            int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(*partitionIT);
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
                Batch *batch = &it->second;
                // make a copy of the geometry, to adjust the partitions specifically for this node
                // remove any partitions that are irrelevant to this noderank
                Shape objectCopy = object;
                std::vector<int> partitionsCopy = objectCopy.getPartitions();
                for (auto it = partitionsCopy.begin(); it != partitionsCopy.end();) {
                    int assignedNodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(*it);
                    if (assignedNodeRank != nodeRank) {
                        it = partitionsCopy.erase(it);
                        it = partitionsCopy.erase(it);
                    } else {
                        it += 2;
                    }
                }

                // set new partitions to the copy
                objectCopy.setPartitions(partitionsCopy, partitionsCopy.size() / 2);

                // printf("Object %ld has %d partitions:\n ", object.recID, object.getPartitionCount());
                // std::vector<int>* partitionRef = object.getPartitionsRef();
                // for (int i=0; i<object.getPartitionCount(); i++) {
                //     printf("(%d,%s),", object.getPartitionID(i), mapping::twoLayerClassIntToStr(object.getPartitionClass(i)).c_str());
                // }
                // printf("\n");

                // add moddified geometry to this batch
                // logger::log_task("Added geometry", objectCopy.recID, " to batch");
                batch->addObjectToBatch(objectCopy);
                // printf("Added geometry %ld to batch\n", geometryCopy.recID);
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

    namespace csv
    {
        DB_STATUS calculateDatasetMetadata(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // open file
            std::ifstream fin(dataset->path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // read total objects (first line of file)
            std::getline(fin, line);
            fin.close();
            size_t objectCount = (size_t) std::stoull(line);
            // dataset global bounds
            double global_xMin = std::numeric_limits<int>::max();
            double global_yMin = std::numeric_limits<int>::max();
            double global_xMax = -std::numeric_limits<int>::max();
            double global_yMax = -std::numeric_limits<int>::max();
            // spawn all available threads (processors)
            #pragma omp parallel firstprivate(line, objectCount) reduction(min:global_xMin) reduction(min:global_yMin) reduction(max:global_xMax)  reduction(max:global_yMax)
            {
                size_t recID;
                double x,y;
                DB_STATUS local_ret = DBERR_OK;
                int tid = omp_get_thread_num();
                int totalThreads = omp_get_num_threads();
                // calculate which lines this thread will handle
                size_t linesPerThread = (objectCount / totalThreads);
                size_t fromLine = 1 + (tid * linesPerThread);          // first line is object count
                size_t toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
                if (tid == totalThreads - 1) {
                    toLine = objectCount+1;
                }
                // open file
                std::ifstream fin(dataset->path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                for (size_t i=0; i<fromLine; i++) {
                    fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                }
                // loop
                size_t currentLine = fromLine;
                std::string token;
                while (true) {
                    // next object
                    std::getline(fin, line);                
                    std::stringstream ss(line);
                    // recID
                    std::getline(ss, token, ',');
                    // recID = (size_t) std::stoull(token);
                    recID = currentLine;
                    // Read the coords x,y
                    while (std::getline(ss, token, ',')) {
                        std::stringstream coordStream(token);
                        // Split the x and y values
                        std::getline(coordStream, token, ' ');
                        x = std::stof(token);
                        std::getline(coordStream, token, ' ');
                        y = std::stof(token);
                        // bounds
                        global_xMin = std::min(global_xMin, x);
                        global_yMin = std::min(global_yMin, y);
                        global_xMax = std::max(global_xMax, x);
                        global_yMax = std::max(global_yMax, y);
                    }
                    currentLine += 1;
                    if (currentLine >= toLine) {
                        // the last line for this thread has been read
                        break;
                    }
                }
                // close file
                fin.close();
            }
            if (ret != DBERR_OK) {
                return ret;
            }
            // set extent
            dataset->dataspaceMetadata.set(global_xMin, global_yMin, global_xMax, global_yMax);
            return ret;
        }

        static DB_STATUS loadDatasetAndPartition(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // initialize batches
            std::unordered_map<int,Batch> batchMap;
            ret = initializeBatchMap(batchMap, dataset->dataType);
            if (ret != DBERR_OK) {
                return ret;
            }
            // open file
            std::ifstream fin(dataset->path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // read total objects (first line of file)
            std::getline(fin, line);
            fin.close();
            size_t objectCount = (size_t) std::stoull(line);
            // count how many batches have been sent in total
            int batchesSent = 0;
            #pragma omp parallel firstprivate(batchMap, line, objectCount) reduction(+:batchesSent)
            {
                int partitionID;
                double x,y;
                double xMin, yMin, xMax, yMax;
                DB_STATUS local_ret = DBERR_OK;
                int tid = omp_get_thread_num();
                int totalThreads = omp_get_num_threads();
                // calculate which lines this thread will handle
                size_t linesPerThread = (objectCount / totalThreads);
                size_t fromLine = 1 + (tid * linesPerThread);          // first line is object count
                size_t toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
                if (tid == totalThreads - 1) {
                    toLine = objectCount+1;
                }
                // logger::log_task("will handle lines", fromLine, "to", toLine);
                // open file
                std::ifstream fin(dataset->path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                for (size_t i=0; i<fromLine; i++) {
                    fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                }

                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->dataType, object);
                if (local_ret != DBERR_OK) {
                    #pragma omp cancel parallel
                    ret = local_ret;
                } else {
                    // shape created
                    // loop
                    size_t currentLine = fromLine;
                    std::string token;
                    while (true) {
                        // reset shape object
                        object.reset();
                        // next object
                        std::getline(fin, line);                
                        std::stringstream ss(line);
                        xMin = std::numeric_limits<int>::max();
                        yMin = std::numeric_limits<int>::max();
                        xMax = -std::numeric_limits<int>::max();
                        yMax = -std::numeric_limits<int>::max();
                        // recID
                        std::getline(ss, token, ',');
                        // object.recID = (size_t) std::stoull(token);
                        object.recID = currentLine;
                        while (std::getline(ss, token, ',')) {
                            std::stringstream coordStream(token);
                            // Split the x and y values
                            std::getline(coordStream, token, ' ');
                            x = std::stof(token);
                            std::getline(coordStream, token, ' ');
                            y = std::stof(token);
                            // add point
                            object.addPoint(x, y);
                            // MBR
                            xMin = std::min(xMin, x);
                            yMin = std::min(yMin, y);
                            xMax = std::max(xMax, x);
                            yMax = std::max(yMax, y);
                        }
                        object.correctGeometry();
                        object.setMBR(xMin, yMin, xMax, yMax);
                        // assign to appropriate batches
                        local_ret = assignObjectToBatches(object, batchMap, batchesSent);
                        if (ret != DBERR_OK) {
                            #pragma omp cancel parallel
                            ret = local_ret;
                            break;
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
                        Batch *batch = &it->second;
                        if (batch->objectCount > 0) {
                            local_ret = comm::controller::serializeAndSendGeometryBatch(batch);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel parallel
                                ret = local_ret;
                                break;
                            }
                            // empty the batch
                            batch->clear();
                            // count the batch
                            batchesSent += 1;
                        }
                    }
                    // close file
                    fin.close();
                } // end of loop
            } // end of parallel region
            // check if operation finished successfully
            if (ret != DBERR_OK) {
                return ret;
            }
            // send an empty pack to each worker to signal the end of the partitioning for this dataset
            for (int i=0; i<g_world_size; i++) {
                // fetch batch (it is guaranteed to be empty)
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    return DBERR_INVALID_PARAMETER;
                }
                Batch *batch = &it->second;
                ret = comm::controller::serializeAndSendGeometryBatch(batch);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
            // logger::log_success("Sent", batchesSent, "non-empty batches.");
            return ret;
        }

    }

    namespace wkt
    {
        DB_STATUS calculateDatasetMetadata(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // open file
            std::ifstream fin(dataset->path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // count total objects (lines)
            size_t totalObjects = 0;
            ret = storage::countLinesInFile(dataset->path, totalObjects);
            if (ret != DBERR_OK) {
                return ret;
            }
            // dataset global bounds
            double global_xMin = std::numeric_limits<int>::max();
            double global_yMin = std::numeric_limits<int>::max();
            double global_xMax = -std::numeric_limits<int>::max();
            double global_yMax = -std::numeric_limits<int>::max();
            // spawn all available threads (processors)
            #pragma omp parallel firstprivate(line, totalObjects) reduction(min:global_xMin) reduction(min:global_yMin) reduction(max:global_xMax)  reduction(max:global_yMax)
            {
                DB_STATUS local_ret = DBERR_OK;
                int tid = omp_get_thread_num();
                int totalThreads = omp_get_num_threads();
                // calculate which lines this thread will handle
                size_t linesPerThread = (totalObjects / totalThreads);
                size_t fromLine = 1 + (tid * linesPerThread);          // first line is object count
                size_t toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
                if (tid == totalThreads - 1) {
                    toLine = totalObjects+1;
                }
                // open file
                std::ifstream fin(dataset->path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                for (size_t i=0; i<fromLine; i++) {
                    fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                }
                
                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->dataType, object);
                if (local_ret != DBERR_OK) {
                    // error creating shape
                    #pragma omp cancel parallel
                    ret = local_ret;
                } else {
                    // loop
                    size_t currentLine = fromLine;
                    std::string token;
                    while (true) {
                        object.reset();
                        // next object
                        std::getline(fin, line);   
                        // parse line to get only the first column (wkt geometry)
                        std::stringstream ss(line);
                        std::getline(ss, token, '\t');
                        // set object from the WKT
                        local_ret = object.setFromWKT(token);
                        if (local_ret == DBERR_INVALID_GEOMETRY) {
                            // this line is not the appropriate geometry type, so just ignore
                        } else if (local_ret != DBERR_OK) {
                            // some other error occured, interrupt
                            #pragma omp cancel parallel
                            ret = local_ret;
                        } else {
                            // set the MBR
                            object.setMBR();
                            // get global bounds
                            global_xMin = std::min(global_xMin, object.mbr.pMin.x);
                            global_yMin = std::min(global_yMin, object.mbr.pMin.y);
                            global_xMax = std::max(global_xMax, object.mbr.pMax.x);
                            global_yMax = std::max(global_yMax, object.mbr.pMax.y);
                        }
                        // next line
                        currentLine += 1;
                        if (currentLine >= toLine) {
                            // the last line for this thread has been read
                            break;
                        }
                    }
                }
                // close file
                fin.close();
            }
            if (ret != DBERR_OK) {
                return ret;
            }
            // set total objects (valid + invalid)
            dataset->totalObjects = totalObjects;
            // set extent
            dataset->dataspaceMetadata.set(global_xMin, global_yMin, global_xMax, global_yMax);
            return ret;
        }

        static DB_STATUS loadDatasetAndPartition(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // initialize batches
            std::unordered_map<int,Batch> batchMap;
            ret = initializeBatchMap(batchMap, dataset->dataType);
            if (ret != DBERR_OK) {
                return ret;
            }
            // open file
            std::ifstream fin(dataset->path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // get total objects (lines)
            size_t totalObjects = dataset->totalObjects;
            size_t totalValidObjects = 0;
            // count how many batches have been sent in total
            int batchesSent = 0;
            #pragma omp parallel firstprivate(batchMap, line, totalObjects) reduction(+:batchesSent) reduction(+:totalValidObjects)
            {
                int partitionID;
                double x,y;
                double xMin, yMin, xMax, yMax;
                DB_STATUS local_ret = DBERR_OK;
                int tid = omp_get_thread_num();
                int totalThreads = omp_get_num_threads();
                // calculate which lines this thread will handle
                size_t linesPerThread = (totalObjects / totalThreads);
                size_t fromLine = 1 + (tid * linesPerThread);          // first line is object count
                size_t toLine = 1 + ((tid + 1) * linesPerThread);    // exclusive
                if (tid == totalThreads - 1) {
                    toLine = totalObjects+1;
                }
                // logger::log_task("will handle lines", fromLine, "to", toLine);
                // open file
                std::ifstream fin(dataset->path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                for (size_t i=0; i<fromLine; i++) {
                    fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                }

                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->dataType, object);
                if (local_ret != DBERR_OK) {
                    // error creating shape
                    #pragma omp cancel parallel
                    ret = local_ret;
                } else {
                    // shape created
                    // loop
                    size_t currentLine = fromLine;
                    std::string token;
                    while (true) {
                        // reset shape object
                        object.reset();
                        // next object
                        std::getline(fin, line);
                        // parse line to get only the first column (wkt geometry)
                        std::stringstream ss(line);
                        std::getline(ss, token, '\t');
                        // set rec ID
                        object.recID = currentLine;
                        // set object from the WKT
                        local_ret = object.setFromWKT(token);
                        if (local_ret == DBERR_INVALID_GEOMETRY) {
                            // this line is not the appropriate geometry type, so just ignore
                        } else if (local_ret != DBERR_OK) {
                            // some other error occured, interrupt
                            #pragma omp cancel parallel
                            ret = local_ret;
                        } else {
                            // valid object
                            totalValidObjects += 1;
                            // set the MBR
                            object.setMBR();
                            // assign to appropriate batches
                            local_ret = assignObjectToBatches(object, batchMap, batchesSent);
                            if (ret != DBERR_OK) {
                                #pragma omp cancel parallel
                                ret = local_ret;
                                break;
                            }
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
                        Batch *batch = &it->second;
                        if (batch->objectCount > 0) {
                            local_ret = comm::controller::serializeAndSendGeometryBatch(batch);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel parallel
                                ret = local_ret;
                                break;
                            }
                            // empty the batch
                            batch->clear();
                            // count the batch
                            batchesSent += 1;
                        }
                    }
                    // close file
                    fin.close();
                } // end of loop
            } // end of parallel region
            // check if operation finished successfully
            if (ret != DBERR_OK) {
                return ret;
            }
            logger::log_success("Partitioned", totalValidObjects, "valid objects");
            // send an empty pack to each worker to signal the end of the partitioning for this dataset
            for (int i=0; i<g_world_size; i++) {
                // fetch batch (it is guaranteed to be empty)
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    return DBERR_INVALID_PARAMETER;
                }
                Batch *batch = &it->second;
                ret = comm::controller::serializeAndSendGeometryBatch(batch);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
            // logger::log_success("Sent", batchesSent, "non-empty batches.");
            return ret;
        }
    }

    DB_STATUS partitionDataset(Dataset *dataset) {
        double startTime;
        DB_STATUS ret;
        // time
        startTime = mpi_timer::markTime();
        switch (dataset->fileType) {
            // perform the partitioning
            case FT_CSV:
                // csv dataset
                ret = csv::loadDatasetAndPartition(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                break;
            case FT_WKT:
                // wkt dataset
                ret = wkt::loadDatasetAndPartition(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                break;
            case FT_BINARY:
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported data file type:", dataset->fileType);
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

    static TwoLayerClassE getTwoLayerClassForMBRandPartition(double mbrXmin, double mbrYmin, double partitionXmin, double partitionYmin) {
        if (mbrXmin < partitionXmin) {
            // before in X axis
            if (mbrYmin < partitionYmin) {
                // before in Y axis, class D
                return CLASS_D;
            } else {
                // after in Y axis, class C
                return CLASS_C;
            }
        } else {
            // after in X axis
            if (mbrYmin < partitionYmin) {
                // before in Y axis, class B
                return CLASS_B;
            } else {
                // after in Y axis, class A
                return CLASS_A;
            }
        }
    }

    namespace two_grid
    {
        /** @brief The agent repartitions the geometry using a finer grid as defined by the two-grid partitioning
         *  and sets the geometry's class in each partition. */
        static DB_STATUS setPartitionClassesForObject(Shape &object){
            // calculate the object's MBR
            const std::vector<bg_point_xy>* pointsRef = object.getReferenceToPoints();
            double xMin = std::numeric_limits<int>::max();
            double yMin = std::numeric_limits<int>::max();
            double xMax = -std::numeric_limits<int>::max();
            double yMax = -std::numeric_limits<int>::max();
            for (auto &it: *pointsRef) {
                xMin = std::min(xMin, it.x());
                yMin = std::min(yMin, it.y());
                xMax = std::max(xMax, it.x());
                yMax = std::max(yMax, it.y());
            }             
            // if (geometry.recID == 10582) {
            //     printf("MBR: (%f,%f),(%f,%f)\n", xMin, yMin, xMax, yMax);
            //     printf("%d partitions in dist grid\n", geometry.partitionCount);
            // }
            // vector for the new partitions
            std::vector<int> newPartitions;
            // for each distribution partition
            for (int i=0; i < object.getPartitionCount(); i++) {
                // get the partition ID, no two layer class set yet
                int distPartitionID = object.getPartitionID(i);
                // get the partition's indices
                int distPartitionIndexX, distPartitionIndexY;
                g_config.partitioningMethod->getDistributionGridPartitionIndices(distPartitionID, distPartitionIndexX, distPartitionIndexY);
                // get the global partition's extents
                // printf("Distribution grid partition ID: %d (%d,%d)\n", distPartitionID, distPartitionIndexX, distPartitionIndexY);
                // calculate the partition's bounds in the distribution grid
                double distPartitionXmin = g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal + (distPartitionIndexX * g_config.partitioningMethod->getDistPartionExtentX());
                double distPartitionYmin = g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal + (distPartitionIndexY * g_config.partitioningMethod->getDistPartionExtentY());
                double distPartitionXmax = g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal + ((distPartitionIndexX+1) * g_config.partitioningMethod->getDistPartionExtentX());
                double distPartitionYmax = g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal + ((distPartitionIndexY+1) * g_config.partitioningMethod->getDistPartionExtentY());
                // printf("Distribution grid partition bounds: (%f,%f),(%f,%f),(%f,%f),(%f,%f)\n", distPartitionXmin, distPartitionYmin, distPartitionXmax, distPartitionYmin, distPartitionXmax, distPartitionYmax, distPartitionXmin, distPartitionYmax);
                TwoLayerClassE objectClassInDistributionPartition = getTwoLayerClassForMBRandPartition(xMin, yMin, distPartitionXmin, distPartitionYmin);
                // printf("Object class in parent partition: %s\n", mapping::twoLayerClassIntToStr(objectClassInDistributionPartition).c_str());
                // get the indices of the start/end part partitions
                int localPartitionXstart = (xMin - distPartitionXmin) / (g_config.partitioningMethod->getDistPartionExtentX() / g_config.partitioningMethod->getPartitioningPPD());
                int localPartitionYstart = (yMin - distPartitionYmin) / (g_config.partitioningMethod->getDistPartionExtentY() / g_config.partitioningMethod->getPartitioningPPD());
                int localPartitionXend = (xMax - distPartitionXmin) / (g_config.partitioningMethod->getDistPartionExtentX() / g_config.partitioningMethod->getPartitioningPPD());
                int localPartitionYend = (yMax - distPartitionYmin) / (g_config.partitioningMethod->getDistPartionExtentY() / g_config.partitioningMethod->getPartitioningPPD());
                
                // printf("Local partition start/end: (%d,%d),(%d,%d)\n", localPartitionXstart, localPartitionYstart, localPartitionXend, localPartitionYend);
                // adjust (todo: check whether any of these happen, discard the rest)
                if (localPartitionXstart < 0) {
                    localPartitionXstart = 0;
                }
                else if (localPartitionXstart > g_config.partitioningMethod->getPartitioningPPD()-1) {
                    localPartitionXstart = g_config.partitioningMethod->getPartitioningPPD()-1;
                }
                if (localPartitionYstart < 0) {
                    localPartitionYstart = 0;
                }
                else if (localPartitionYstart > g_config.partitioningMethod->getPartitioningPPD()-1) {
                    localPartitionYstart = g_config.partitioningMethod->getPartitioningPPD()-1;
                }
                if (localPartitionXend < 0) {
                    localPartitionXend = 0;
                }
                else if (localPartitionXend > g_config.partitioningMethod->getPartitioningPPD()-1) {
                    localPartitionXend = g_config.partitioningMethod->getPartitioningPPD()-1;
                }
                if (localPartitionYend < 0) {
                    localPartitionYend = 0;
                }
                else if (localPartitionYend > g_config.partitioningMethod->getPartitioningPPD()-1) {
                    localPartitionYend = g_config.partitioningMethod->getPartitioningPPD()-1;
                }

                int totalNewPartitions = (localPartitionXend - localPartitionXstart + 1) * (localPartitionYend - localPartitionYstart + 1);
                // vector for the new partitions
                newPartitions.reserve(newPartitions.size() + totalNewPartitions*2);
                // first partition inherits the class of the parent partition
                int partitionID = g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, localPartitionXstart, localPartitionYstart);
                newPartitions.emplace_back(partitionID);
                newPartitions.emplace_back(objectClassInDistributionPartition);
                // for each local partition, get the object's class
                for (int indexI=localPartitionXstart; indexI <= localPartitionXend; indexI++) {
                    if (indexI != localPartitionXstart) {
                        // next partitions on X axis
                        // add partition ID
                        int partitionID = g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, indexI, localPartitionYstart);
                        newPartitions.emplace_back(partitionID);
                        if (objectClassInDistributionPartition == CLASS_A || objectClassInDistributionPartition == CLASS_C) {
                            // if in parent partition it is A or C 
                            // then in the next partition on the X axis would be C as well
                            newPartitions.emplace_back(CLASS_C);
                        } else {
                            // if in parent partition it is B or D 
                            // then in the next partition on the X axis would be D 
                            newPartitions.emplace_back(CLASS_D);
                        }
                    }
                    for (int indexJ=localPartitionYstart+1; indexJ <= localPartitionYend; indexJ++) {
                        // next partitions on Y axis
                        // add partition ID
                        int partitionID = g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, indexI, indexJ);
                        newPartitions.emplace_back(partitionID);
                        if (indexI == localPartitionXstart) {
                            if (objectClassInDistributionPartition == CLASS_B || objectClassInDistributionPartition == CLASS_A) {
                                // if it is the first column and the object is class A or B in the dist grid
                                // then this is class B as well
                                newPartitions.emplace_back(CLASS_B);
                            } else {
                                // otherwise it is D
                                newPartitions.emplace_back(CLASS_D);
                            }
                        } else {
                            // D always
                            newPartitions.emplace_back(CLASS_D);
                        }
                    }
                }
            }
            // replace into object
            object.setPartitions(newPartitions, newPartitions.size() / 2);
            return DBERR_OK;
        }
    }

    namespace round_robin
    {
        /** @brief The agent sets the geometry's class in its partitions, as set by the round robin partitioning. */
        static DB_STATUS setPartitionClassesForObject(Shape &object){
            // calculate the object's MBR
            const std::vector<bg_point_xy>* pointsRef = object.getReferenceToPoints();
            double xMin = std::numeric_limits<int>::max();
            double yMin = std::numeric_limits<int>::max();
            double xMax = -std::numeric_limits<int>::max();
            double yMax = -std::numeric_limits<int>::max();
            for (auto &it: *pointsRef) {
                xMin = std::min(xMin, it.x());
                yMin = std::min(yMin, it.y());
                xMax = std::max(xMax, it.x());
                yMax = std::max(yMax, it.y());
            }          
            // for each distribution partition
            for (int i=0; i < object.getPartitionCount(); i++) {
                // get the partition ID, no two layer class set yet
                int distPartitionID = object.getPartitionID(i);
                // get the partition's indices
                int distPartitionIndexX, distPartitionIndexY;
                g_config.partitioningMethod->getDistributionGridPartitionIndices(distPartitionID, distPartitionIndexX, distPartitionIndexY);
                    // calculate the partition's bottom left point coordinates in the distribution grid to get the object's class
                double distPartitionXmin = g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal + (distPartitionIndexX * g_config.partitioningMethod->getDistPartionExtentX());
                double distPartitionYmin = g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal + (distPartitionIndexY * g_config.partitioningMethod->getDistPartionExtentY());
                // get the object's class
                TwoLayerClassE objectClass = getTwoLayerClassForMBRandPartition(xMin, yMin, distPartitionXmin, distPartitionYmin);
                // fill in the partition's two layer class data
                object.setPartitionClass(i, objectClass);
            }
            return DBERR_OK;
        }
    }

    DB_STATUS calculateTwoLayerClasses(Batch &batch) {
        DB_STATUS ret = DBERR_OK;
        // classify based on partitioning method
        switch (g_config.partitioningMethod->type) {
            case PARTITIONING_ROUND_ROBIN:
                #pragma omp parallel
                {
                    int tid = omp_get_thread_num();
                    DB_STATUS local_ret = DBERR_OK;
                    #pragma omp for
                    for (int i=0; i<batch.objectCount; i++) {
                        // determine the two layer classes for each partition of the object in the fine grid
                        local_ret = round_robin::setPartitionClassesForObject(batch.objects[i]);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "Setting partition classes for object with ID", batch.objects[i].recID, "failed.");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
                break;
            case PARTITIONING_TWO_GRID:
                #pragma omp parallel //num_threads(1)
                {
                    int tid = omp_get_thread_num();
                    DB_STATUS local_ret = DBERR_OK;
                    #pragma omp for
                    for (int i=0; i<batch.objectCount; i++) {
                    // for (int i=0; i<batch.objects.size(); i++) {
                        // determine the two layer classes for each partition of the object in the fine grid
                        local_ret = two_grid::setPartitionClassesForObject(batch.objects[i]);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "Setting partition classes for object with ID", batch.objects[i].recID, "failed.");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // if (batch.geometries[i].recID == 10582) {
                        //     printf("Object %ld has %d partitions:\n", batch.geometries[i].recID, batch.geometries[i].partitionCount);
                        //     for (int j=0; j < batch.geometries[i].partitionCount; j++) {
                        //         printf("    partition: %d class %s\n", batch.geometries[i].partitions[2*j], mapping::twoLayerClassIntToStr((TwoLayerClassE) batch.geometries[i].partitions[2*j+1]).c_str());
                        //     }   
                        // }
                    }
                }
                break;
            default:
                logger::log_error(DBERR_INVALID_PARAMETER, "Unknown partitioning method type:", g_config.partitioningMethod->type);
                return DBERR_INVALID_PARAMETER;
        }

        // check if parallel classification compleleted successfully
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Two layer classification for batch failed.");
        }
        return ret;
    }
        
}