#include "env/partitioning.h"
#include <bitset>
#include <fcntl.h>
#include <sys/mman.h>

namespace partitioning
{
    // void printPartitionAssignment() {
    //     for (int j=0; j<g_config.partitioningMethod->getDistributionPPD(); j++) {
    //         for (int i=0; i<g_config.partitioningMethod->getDistributionPPD(); i++) {
    //             printf("%d ", g_config.partitioningMethod->getNodeRankForPartitionID(g_config.partitioningMethod->getDistributionGridPartitionID(i,j)));
    //         }
    //         printf("\n");
    //     }
    // }
    
    DB_STATUS getPartitionsForMBR(MBR &mbr, std::vector<int> &partitionIDs){
        // logger::log_task("Calculating partition for MBR", mbr.pMin.x, mbr.pMin.y, mbr.pMax.x, mbr.pMax.y);
        // g_config.datasetOptions.dataspaceMetadata.print();
        // logger::log_task("DistpartitionExtentX:", g_config.partitioningMethod->getDistPartionExtentX());
        // logger::log_task("DistpartitionExtentY:", g_config.partitioningMethod->getDistPartionExtentY());
        // logger::log_task("DistributionPPD:", g_config.partitioningMethod->getDistributionPPD());
        // return DBERR_NULL_PTR_EXCEPTION;

        int minPartitionX = (mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getDistPartionExtentX();
        int minPartitionY = (mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getDistPartionExtentY();
        int maxPartitionX = (mbr.pMax.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getDistPartionExtentX();
        int maxPartitionY = (mbr.pMax.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getDistPartionExtentY();
        
        int startPartitionID = g_config.partitioningMethod->getPartitionID(minPartitionX, minPartitionY, g_config.partitioningMethod->getDistributionPPD());
        int lastPartitionID = g_config.partitioningMethod->getPartitionID(maxPartitionX, maxPartitionY, g_config.partitioningMethod->getDistributionPPD());
        if (startPartitionID < 0 || startPartitionID > (g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD()) -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Start partition ID calculated wrong:", startPartitionID);
            return DBERR_INVALID_PARTITION;
        }
        if (lastPartitionID < 0 || lastPartitionID > g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD() -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Last partition ID calculated wrong: MBR(", mbr.pMin.x, mbr.pMin.y, mbr.pMax.x, mbr.pMax.y, ")");
            return DBERR_INVALID_PARTITION;
        }
        // create the distribution grid partition list for the object
        for (int i=minPartitionX; i<=maxPartitionX; i++) {
            for (int j=minPartitionY; j<=maxPartitionY; j++) {
                int partitionID = g_config.partitioningMethod->getPartitionID(i, j, g_config.partitioningMethod->getDistributionPPD());
                partitionIDs.emplace_back(partitionID);
            }
        }
        return DBERR_OK;
    }

    /** @brief returns the list of partitions in the coarse grid that intersect the given MBR AND correspond to the invoking node. */
    DB_STATUS getPartitionsForMBRAndNode(MBR &mbr, std::vector<int> &partitionIDs){
        int minPartitionX = (mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getDistPartionExtentX();
        int minPartitionY = (mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getDistPartionExtentY();
        int maxPartitionX = (mbr.pMax.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getDistPartionExtentX();
        int maxPartitionY = (mbr.pMax.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getDistPartionExtentY();
        
        int startPartitionID = g_config.partitioningMethod->getPartitionID(minPartitionX, minPartitionY, g_config.partitioningMethod->getDistributionPPD());
        int lastPartitionID = g_config.partitioningMethod->getPartitionID(maxPartitionX, maxPartitionY, g_config.partitioningMethod->getDistributionPPD());
        if (startPartitionID < 0 || startPartitionID > (g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD()) -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Start partition ID calculated wrong:", startPartitionID);
            return DBERR_INVALID_PARTITION;
        }
        if (lastPartitionID < 0 || lastPartitionID > g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD() -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Last partition ID calculated wrong: MBR(", mbr.pMin.x, mbr.pMin.y, mbr.pMax.x, mbr.pMax.y, ")");
            return DBERR_INVALID_PARTITION;
        }
        // create the distribution grid partition list for the object
        for (int i=minPartitionX; i<=maxPartitionX; i++) {
            for (int j=minPartitionY; j<=maxPartitionY; j++) {
                int partitionID = g_config.partitioningMethod->getPartitionID(i, j, g_config.partitioningMethod->getDistributionPPD());
                // check if this partition is assigned to this worker
                int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(partitionID);
                if (nodeRank == g_parent_original_rank) {
                    partitionIDs.emplace_back(partitionID);
                }
            }
        }
        return DBERR_OK;
    }

    static DB_STATUS initializeBatchMap(std::unordered_map<int,Batch> &batchMap, DataType dataType) {
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
                case DT_BOX:
                default:
                    logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype for batch:", dataType);
                    return DBERR_INVALID_DATATYPE;
            }
            batch.destRank = i;
            if (i > 0) {
                batch.comm = &g_controller_comm;
            } else {
                batch.comm = &g_agent_comm;
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
                // add moddified geometry to this batch
                it->second.addObjectToBatch(object);

                // if batch is full, send and reset
                if (it->second.objectCount >= it->second.maxObjectCount) {
                    ret = comm::controller::serializeAndSendGeometryBatch(&it->second);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    // reset
                    it->second.clear();
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
            std::ifstream fin(dataset->metadata.path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
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
                // calculate which lines this thread will handle
                size_t linesPerThread = ceil(objectCount / MAX_THREADS);
                size_t fromLine = (tid * linesPerThread);
                size_t toLine = std::min(fromLine + linesPerThread, objectCount+1);
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
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
            dataset->metadata.dataspaceMetadata.set(global_xMin, global_yMin, global_xMax, global_yMax);
            return ret;
        }

        static DB_STATUS loadDatasetAndPartition(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // initialize batches
            std::unordered_map<int,Batch> batchMap;
            ret = initializeBatchMap(batchMap, dataset->metadata.dataType);
            if (ret != DBERR_OK) {
                return ret;
            }
            // open file
            std::ifstream fin(dataset->metadata.path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
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
                // calculate which lines this thread will handle
                size_t linesPerThread = ceil(objectCount / MAX_THREADS);
                size_t fromLine = (tid * linesPerThread);
                size_t toLine = std::min(fromLine + linesPerThread, objectCount+1);
                // logger::log_task("will handle lines", fromLine, "to", toLine);
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                for (size_t i=0; i<fromLine; i++) {
                    fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                }

                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
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
                        if (local_ret != DBERR_OK) {
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
            std::ifstream fin(dataset->metadata.path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // count total objects (lines)
            size_t totalObjects = 0;
            ret = storage::reader::getDatasetLineCountWithSystemCall(dataset, totalObjects);
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
                // calculate which lines this thread will handle
                size_t linesPerThread = ceil(totalObjects / MAX_THREADS);
                size_t fromLine = (tid * linesPerThread);
                size_t toLine = std::min(fromLine + linesPerThread, totalObjects+1);
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                for (size_t i=0; i<fromLine; i++) {
                    fin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
                }
                
                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
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
            dataset->metadata.dataspaceMetadata.set(global_xMin, global_yMin, global_xMax, global_yMax);
            return ret;
        }

        static DB_STATUS loadDatasetAndPartition(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // initialize batches
            // std::unordered_map<int,Batch> batchMap;
            std::unordered_map<int,Batch> batchMap;
            ret = initializeBatchMap(batchMap, dataset->metadata.dataType);
            if (ret != DBERR_OK) {
                return ret;
            }
            // open file
            std::ifstream fin(dataset->metadata.path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // get total objects (lines)
            size_t totalObjects = dataset->totalObjects;
            size_t totalValidObjects = 0;
            // count how many batches have been sent in total
            int batchesSent = 0;
            // logger::log_task("Spawning", MAX_THREADS, "threads that each will store", g_world_size, "batches of", g_config.partitioningMethod->batchSize, "objects of arbitrary size");
            #pragma omp parallel firstprivate(batchMap, line, totalObjects) reduction(+:batchesSent) reduction(+:totalValidObjects)
            {
                int partitionID;
                double x,y;
                double xMin, yMin, xMax, yMax;
                DB_STATUS local_ret = DBERR_OK;
                int tid = omp_get_thread_num();
                // calculate which lines this thread will handle
                size_t linesPerThread = (totalObjects + MAX_THREADS - 1) / MAX_THREADS;
                size_t fromLine = (tid * linesPerThread);
                size_t toLine = std::min(fromLine + linesPerThread, totalObjects);
                // logger::log_task("will handle lines", fromLine, "to", toLine);
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                std::string token;
                size_t currentLine = 0;
                while (currentLine < fromLine && std::getline(fin, line)) {
                    currentLine++;
                }

                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
                if (local_ret != DBERR_OK) {
                    // error creating shape
                    #pragma omp cancel parallel
                    ret = local_ret;
                } else {
                    // shape created
                    // loop
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
                            break;
                        } else {
                            // valid object
                            totalValidObjects += 1;
                            // set the MBR
                            object.setMBR();
                            // assign to appropriate batches
                            local_ret = assignObjectToBatches(object, batchMap, batchesSent);
                            if (local_ret != DBERR_OK) {
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
            // logger::log_success("Partitioned", totalValidObjects, "valid objects");
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

        static DB_STATUS loadDatasetAndPartitionMMAP(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;

            // Initialize batches
            std::unordered_map<int, Batch> batchMap;
            ret = initializeBatchMap(batchMap, dataset->metadata.dataType);
            if (ret != DBERR_OK) {
                return ret;
            }

            // Open the file
            int fd = open(dataset->metadata.path.c_str(), O_RDONLY);
            if (fd == -1) {
                logger::log_error(DBERR_OPEN_FILE_FAILED, "Failed to open dataset path:", dataset->metadata.path);
                return DBERR_OPEN_FILE_FAILED;
            }

            // Get file size
            off_t fileSize = lseek(fd, 0, SEEK_END);
            if (fileSize == -1) {
                logger::log_error(DBERR_OPEN_FILE_FAILED, "Failed to determine file size.");
                return DBERR_DISK_READ_FAILED;
            }

            // Memory-map the file
            char* mappedData = (char*) mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
            if (mappedData == MAP_FAILED) {
                close(fd);
                logger::log_error(DBERR_MMAP_FAILED, "mmap failed.");
                return DBERR_MMAP_FAILED;
            }

            // Close the file descriptor
            close(fd);

            // Set chunk size per thread
            size_t chunkSize = fileSize / MAX_THREADS;
            size_t totalValidObjects = 0;
            int batchesSent = 0;
            size_t lineCounter = 0;

            #pragma omp parallel firstprivate(batchMap) reduction(+:batchesSent) reduction(+:totalValidObjects)
            {
                int threadID = omp_get_thread_num();
                size_t start = threadID * chunkSize;
                size_t end = (threadID == MAX_THREADS - 1) ? fileSize : (threadID + 1) * chunkSize;

                // Ensure we start at the beginning of a new line
                while (start > 0 && mappedData[start] != '\n') start++;

                // Local processing variables
                DB_STATUS local_ret = DBERR_OK;
                std::string token;
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
                if (local_ret != DBERR_OK) {
                    #pragma omp cancel parallel
                    ret = local_ret;
                }

                // Read & process lines in this chunk
                while (start < end) {
                    size_t lineStart = start;
                    while (start < fileSize && mappedData[start] != '\n') start++;

                    if (start < fileSize) { 
                        std::string line(mappedData + lineStart, start - lineStart);
                        start++;  // Move to next line

                        object.reset();
                        std::stringstream ss(line);
                        std::getline(ss, token, '\t'); // Extract WKT

                        object.recID = lineStart; // Use file offset as ID
                        local_ret = object.setFromWKT(token);
                        if (local_ret == DBERR_OK) {
                            totalValidObjects++;
                            object.setMBR();
                            local_ret = assignObjectToBatches(object, batchMap, batchesSent);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel parallel
                            }
                        }
                    }
                }

                // Send remaining non-empty batches
                for (int i = 0; i < g_world_size; i++) {
                    auto it = batchMap.find(i);
                    if (it == batchMap.end()) {
                        logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                        #pragma omp cancel parallel
                        ret = DBERR_INVALID_PARAMETER;
                    }
                    Batch* batch = &it->second;
                    if (batch->objectCount > 0) {
                        local_ret = comm::controller::serializeAndSendGeometryBatch(batch);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel parallel
                            ret = local_ret;
                            break;
                        }
                        batch->clear();
                        batchesSent++;
                    }
                }
            } // End of parallel region

            if (ret != DBERR_OK) {
                return ret;
            }

            // Send termination signal to workers
            for (int i = 0; i < g_world_size; i++) {
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    return DBERR_INVALID_PARAMETER;
                }
                Batch* batch = &it->second;
                ret = comm::controller::serializeAndSendGeometryBatch(batch);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }

            // Unmap memory
            if (munmap(mappedData, fileSize) == -1) {
                logger::log_error(DBERR_MMAP_FAILED, "munmap failed.");
                return DBERR_MMAP_FAILED;
            }

            return ret;
        }

        static DB_STATUS loadDatasetAndPartitionNoBatches(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            // open file
            std::ifstream fin(dataset->metadata.path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                return DBERR_MISSING_FILE;
            }
            std::string line;
            // get total objects (lines)
            size_t totalObjects = dataset->totalObjects;
            size_t totalValidObjects = 0;
            // count how many batches have been sent in total
            #pragma omp parallel firstprivate(line, totalObjects) reduction(+:totalValidObjects)
            {
                int partitionID;
                double x,y;
                double xMin, yMin, xMax, yMax;
                DB_STATUS local_ret = DBERR_OK;
                int tid = omp_get_thread_num();
                // calculate which lines this thread will handle
                size_t linesPerThread = (totalObjects + MAX_THREADS - 1) / MAX_THREADS;
                size_t fromLine = (tid * linesPerThread);
                size_t toLine = std::min(fromLine + linesPerThread, totalObjects);
                // logger::log_task("will handle lines", fromLine, "to", toLine);
                // open file
                std::ifstream fin(dataset->metadata.path);
                if (!fin.is_open()) {
                    logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset->metadata.path);
                    #pragma omp cancel parallel
                    ret = DBERR_MISSING_FILE;
                }
                // jump to start line
                std::string token;
                size_t currentLine = 0;
                while (currentLine < fromLine && std::getline(fin, line)) {
                    currentLine++;
                }

                // create empty object based on data type
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
                if (local_ret != DBERR_OK) {
                    // error creating shape
                    #pragma omp cancel parallel
                    ret = local_ret;
                } else {
                    // shape created
                    // loop
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
                            break;
                        } else {
                            // valid object
                            totalValidObjects += 1;
                            // set the MBR
                            object.setMBR();
                            
                            // find partitions
                            std::vector<int> partitionIDs;
                            local_ret = partitioning::getPartitionsForMBR(object.mbr, partitionIDs);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel parallel
                                ret = local_ret;
                                break;
                            }

                            // find which nodes need to receive this geometry
                            std::vector<bool> bitVector(g_world_size, false);
                            // get receiving worker ranks
                            for (auto partitionIT = partitionIDs.begin(); partitionIT != partitionIDs.end(); partitionIT++) {
                                // get node rank from distribution grid
                                int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(*partitionIT);
                                bitVector[nodeRank] = true;
                            }
                            // to AGENT
                            if (bitVector.at(AGENT_RANK)) {
                                // if it has been marked, serialize and send
                                local_ret = comm::controller::serializeAndSendGeometry(&object, AGENT_RANK, g_agent_comm);
                                if (local_ret != DBERR_OK) {
                                    #pragma omp cancel parallel
                                    ret = local_ret;
                                    break;
                                }
                            }
                            // to WORKERS
                            for (int nodeRank=1; nodeRank<bitVector.size(); nodeRank++) {
                                if (bitVector.at(nodeRank)) {
                                    // if it has been marked, serialize and send
                                    local_ret = comm::controller::serializeAndSendGeometry(&object, nodeRank, g_controller_comm);
                                    if (local_ret != DBERR_OK) {
                                        #pragma omp cancel parallel
                                        ret = local_ret;
                                        break;
                                    }
                                }
                            }
                        }
                        currentLine += 1;
                        if (currentLine >= toLine) {
                            // the last line for this thread has been read
                            break;
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
            // logger::log_success("Partitioned", totalValidObjects, "valid objects");
            // send an empty shape to each worker to signal the end of the partitioning for this dataset
            
            ret = comm::controller::serializeAndSendGeometry(nullptr, AGENT_RANK, g_agent_comm);
            if (ret != DBERR_OK) {
                return ret;
            }
            for (int i=1; i<g_world_size; i++) {
                ret = comm::controller::serializeAndSendGeometry(nullptr, i, g_controller_comm);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
            // logger::log_success("Sent", batchesSent, "non-empty batches.");
            return ret;
        }

        /** @brief Uses mmap to load and partition the given dataset locally. Does not perform any communications. Dataset must exist on this machine. */
        static DB_STATUS loadDatasetAndPartitionMMAPlocally(Dataset *dataset) {
            DB_STATUS ret = DBERR_OK;

            // Open the file
            int fd = open(dataset->metadata.path.c_str(), O_RDONLY);
            if (fd == -1) {
                logger::log_error(DBERR_OPEN_FILE_FAILED, "Failed to open dataset path:", dataset->metadata.path);
                return DBERR_OPEN_FILE_FAILED;
            }

            // Get file size
            off_t fileSize = lseek(fd, 0, SEEK_END);
            if (fileSize == -1) {
                logger::log_error(DBERR_OPEN_FILE_FAILED, "Failed to determine file size.");
                return DBERR_DISK_READ_FAILED;
            }

            // Memory-map the file
            char* mappedData = (char*) mmap(NULL, fileSize, PROT_READ, MAP_PRIVATE, fd, 0);
            if (mappedData == MAP_FAILED) {
                close(fd);
                logger::log_error(DBERR_MMAP_FAILED, "mmap failed.");
                return DBERR_MMAP_FAILED;
            }

            // Close the file descriptor
            close(fd);

            // Set chunk size per thread
            size_t chunkSize = fileSize / MAX_THREADS;
            size_t lineCounter = 0;

            // Thread-local storage for reduction
            std::vector<std::vector<Shape>> threadLocalVectors(MAX_THREADS);

            #pragma omp parallel
            {
                int threadID = omp_get_thread_num();
                size_t start = threadID * chunkSize;
                size_t end = (threadID == MAX_THREADS - 1) ? fileSize : (threadID + 1) * chunkSize;

                // Ensure we start at the beginning of a new line
                while (start > 0 && mappedData[start] != '\n') start++;

                // Local processing variables
                DB_STATUS local_ret = DBERR_OK;
                std::string token;
                Shape object;
                local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
                if (local_ret != DBERR_OK) {
                    #pragma omp cancel parallel
                    ret = local_ret;
                }

                // thread map
                std::vector<Shape> &localVector = threadLocalVectors[threadID];

                // Read & process lines in this chunk
                while (start < end) {
                    size_t lineStart = start;
                    while (start < fileSize && mappedData[start] != '\n') start++;

                    if (start < fileSize) { 
                        std::string line(mappedData + lineStart, start - lineStart);
                        start++;  // Move to next line

                        object.reset();
                        std::stringstream ss(line);
                        std::getline(ss, token, '\t'); // Extract WKT

                        object.recID = lineStart; // Use file offset as ID
                        local_ret = object.setFromWKT(token);
                        if (local_ret == DBERR_OK) {
                            object.setMBR();
                            // get partitions
                            std::vector<int> partitionIDs;
                            local_ret = partitioning::getPartitionsForMBRAndNode(object.mbr, partitionIDs);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel parallel
                            }
                            if (!partitionIDs.empty()) {
                                // object is assigned to this node
                                // dataset->storeObject(object);
                                localVector.emplace_back(object);
                            }

                        }
                    }
                }

                
            } // End of parallel region
            if (ret != DBERR_OK) {
                return ret;
            }

            // merging of all thread-local vectors
            dataset->totalObjects = 0;
            for (int i = 0; i < MAX_THREADS; ++i) {
                dataset->totalObjects += threadLocalVectors[i].size();
            }
            dataset->objects.reserve(dataset->totalObjects);
            for (int i = 0; i < MAX_THREADS; ++i) {
                for (auto &it: threadLocalVectors[i]){
                    dataset->storeObject(it);
                }
                threadLocalVectors[i].clear();
            }

            // set sizes and capacity
            dataset->objects.shrink_to_fit();

            // Unmap memory
            if (munmap(mappedData, fileSize) == -1) {
                logger::log_error(DBERR_MMAP_FAILED, "munmap failed.");
                return DBERR_MMAP_FAILED;
            }

            return ret;
        }
    } // wkt namespace
    
    DB_STATUS partitionDatasetLocally(Dataset *dataset) {
        if (dataset == nullptr) {
            logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Dataset pointer is null");
            return DBERR_NULL_PTR_EXCEPTION;
        }
        DB_STATUS ret;
        // time
        switch (dataset->metadata.fileType) {
            // perform the partitioning
            case hec::FT_CSV:
                // csv dataset
                return DBERR_FEATURE_UNSUPPORTED;
            case hec::FT_WKT:
                // wkt dataset
                ret = wkt::loadDatasetAndPartitionMMAPlocally(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning locally failed for dataset", dataset->metadata.internalID);
                    return ret;
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported data file type:", dataset->metadata.fileType);
                break;
        }

        // logger::log_success("Partitioning for dataset", dataset->metadata.internalID, "finished in", mpi_timer::getElapsedTime(startTime), "seconds.");                
        return DBERR_OK;
    }

    DB_STATUS partitionDataset(Dataset *dataset) {
        if (dataset == nullptr) {
            logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Dataset pointer is null");
            return DBERR_NULL_PTR_EXCEPTION;
        }
        DB_STATUS ret;
        // time
        switch (dataset->metadata.fileType) {
            // perform the partitioning
            case hec::FT_CSV:
                // csv dataset
                ret = csv::loadDatasetAndPartition(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->metadata.internalID);
                    return ret;
                }
                break;
            case hec::FT_WKT:
                // wkt dataset
                // ret = wkt::loadDatasetAndPartition(dataset);
                // ret = wkt::loadDatasetAndPartitionNoBatches(dataset);
                ret = wkt::loadDatasetAndPartitionMMAP(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->metadata.internalID);
                    return ret;
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported data file type:", dataset->metadata.fileType);
                break;
        }

        // logger::log_success("Partitioning for dataset", dataset->metadata.internalID, "finished in", mpi_timer::getElapsedTime(startTime), "seconds.");                
        return DBERR_OK;
    }

    static TwoLayerClass getTwoLayerClassForMBRandPartition(double mbrXmin, double mbrYmin, double partitionXmin, double partitionYmin) {
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
         *  and sets the geometry's class in each partition. 
         * @todo: revisit this method. Too many extra computations? why compute the mbr again?
         * */
        // static DB_STATUS setPartitionClassesForObject(Shape &object){
        //     // vector for the new partitions
        //     std::vector<int> newPartitions;
        //     // for each distribution partition
        //     for (int i=0; i < object.getPartitionCount(); i++) {
        //         // get the partition ID, no two layer class set yet
        //         int distPartitionID = object.getPartitionID(i);
        //         // get the partition's indices
        //         int distPartitionIndexX, distPartitionIndexY;
        //         g_config.partitioningMethod->getDistributionGridPartitionIndices(distPartitionID, distPartitionIndexX, distPartitionIndexY);
        //         // get the global partition's extents
        //         // printf("Distribution grid partition ID: %d (%d,%d)\n", distPartitionID, distPartitionIndexX, distPartitionIndexY);
        //         // calculate the partition's bounds in the distribution grid
        //         double distPartitionXmin = g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal + (distPartitionIndexX * g_config.partitioningMethod->getDistPartionExtentX());
        //         double distPartitionYmin = g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal + (distPartitionIndexY * g_config.partitioningMethod->getDistPartionExtentY());
        //         double distPartitionXmax = g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal + ((distPartitionIndexX+1) * g_config.partitioningMethod->getDistPartionExtentX());
        //         double distPartitionYmax = g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal + ((distPartitionIndexY+1) * g_config.partitioningMethod->getDistPartionExtentY());
        //         // printf("Distribution grid partition bounds: (%f,%f),(%f,%f),(%f,%f),(%f,%f)\n", distPartitionXmin, distPartitionYmin, distPartitionXmax, distPartitionYmin, distPartitionXmax, distPartitionYmax, distPartitionXmin, distPartitionYmax);
        //         TwoLayerClass objectClassInDistributionPartition = getTwoLayerClassForMBRandPartition(object.mbr.pMin.x, object.mbr.pMin.y, distPartitionXmin, distPartitionYmin);
        //         // printf("Object class in parent partition: %s\n", mapping::twoLayerClassIntToStr(objectClassInDistributionPartition).c_str());
        //         // get the indices of the start/end part partitions
        //         int localPartitionXstart = (object.mbr.pMin.x - distPartitionXmin) / (g_config.partitioningMethod->getDistPartionExtentX() / g_config.partitioningMethod->getPartitioningPPD());
        //         int localPartitionYstart = (object.mbr.pMin.y - distPartitionYmin) / (g_config.partitioningMethod->getDistPartionExtentY() / g_config.partitioningMethod->getPartitioningPPD());
        //         int localPartitionXend = (object.mbr.pMax.x - distPartitionXmin) / (g_config.partitioningMethod->getDistPartionExtentX() / g_config.partitioningMethod->getPartitioningPPD());
        //         int localPartitionYend = (object.mbr.pMax.y - distPartitionYmin) / (g_config.partitioningMethod->getDistPartionExtentY() / g_config.partitioningMethod->getPartitioningPPD());
                
        //         // printf("Local partition start/end: (%d,%d),(%d,%d)\n", localPartitionXstart, localPartitionYstart, localPartitionXend, localPartitionYend);
        //         // adjust (todo: check whether any of these happen, discard the rest)
        //         if (localPartitionXstart < 0) {
        //             localPartitionXstart = 0;
        //         }
        //         else if (localPartitionXstart > g_config.partitioningMethod->getPartitioningPPD()-1) {
        //             localPartitionXstart = g_config.partitioningMethod->getPartitioningPPD()-1;
        //         }
        //         if (localPartitionYstart < 0) {
        //             localPartitionYstart = 0;
        //         }
        //         else if (localPartitionYstart > g_config.partitioningMethod->getPartitioningPPD()-1) {
        //             localPartitionYstart = g_config.partitioningMethod->getPartitioningPPD()-1;
        //         }
        //         if (localPartitionXend < 0) {
        //             localPartitionXend = 0;
        //         }
        //         else if (localPartitionXend > g_config.partitioningMethod->getPartitioningPPD()-1) {
        //             localPartitionXend = g_config.partitioningMethod->getPartitioningPPD()-1;
        //         }
        //         if (localPartitionYend < 0) {
        //             localPartitionYend = 0;
        //         }
        //         else if (localPartitionYend > g_config.partitioningMethod->getPartitioningPPD()-1) {
        //             localPartitionYend = g_config.partitioningMethod->getPartitioningPPD()-1;
        //         }

        //         int totalNewPartitions = (localPartitionXend - localPartitionXstart + 1) * (localPartitionYend - localPartitionYstart + 1);
        //         // vector for the new partitions
        //         newPartitions.reserve(newPartitions.size() + totalNewPartitions*2);
        //         // first partition inherits the class of the parent partition
        //         int partitionID = g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, localPartitionXstart, localPartitionYstart);
        //         newPartitions.emplace_back(partitionID);
        //         newPartitions.emplace_back(objectClassInDistributionPartition);
        //         // for each local partition, get the object's class
        //         for (int indexI=localPartitionXstart; indexI <= localPartitionXend; indexI++) {
        //             if (indexI != localPartitionXstart) {
        //                 // next partitions on X axis
        //                 // add partition ID
        //                 int partitionID = g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, indexI, localPartitionYstart);
        //                 newPartitions.emplace_back(partitionID);
        //                 if (objectClassInDistributionPartition == CLASS_A || objectClassInDistributionPartition == CLASS_C) {
        //                     // if in parent partition it is A or C 
        //                     // then in the next partition on the X axis would be C as well
        //                     newPartitions.emplace_back(CLASS_C);
        //                 } else {
        //                     // if in parent partition it is B or D 
        //                     // then in the next partition on the X axis would be D 
        //                     newPartitions.emplace_back(CLASS_D);
        //                 }
        //             }
        //             for (int indexJ=localPartitionYstart+1; indexJ <= localPartitionYend; indexJ++) {
        //                 // next partitions on Y axis
        //                 // add partition ID
        //                 int partitionID = g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, indexI, indexJ);
        //                 newPartitions.emplace_back(partitionID);
        //                 if (indexI == localPartitionXstart) {
        //                     if (objectClassInDistributionPartition == CLASS_B || objectClassInDistributionPartition == CLASS_A) {
        //                         // if it is the first column and the object is class A or B in the dist grid
        //                         // then this is class B as well
        //                         newPartitions.emplace_back(CLASS_B);
        //                     } else {
        //                         // otherwise it is D
        //                         newPartitions.emplace_back(CLASS_D);
        //                     }
        //                 } else {
        //                     // D always
        //                     newPartitions.emplace_back(CLASS_D);
        //                 }
        //             }
        //         }
        //     }
        //     // replace into object
        //     object.setPartitions(newPartitions, newPartitions.size() / 2);
        //     return DBERR_OK;
        // }
    }

    namespace round_robin
    {
        /** @brief The agent sets the geometry's class in its partitions, as set by the round robin partitioning. */
        // static DB_STATUS setPartitionClassesForObject(Shape &object){
        //     // calculate the object's MBR
        //     const std::vector<bg_point_xy>* pointsRef = object.getReferenceToPoints();
        //     double xMin = std::numeric_limits<int>::max();
        //     double yMin = std::numeric_limits<int>::max();
        //     double xMax = -std::numeric_limits<int>::max();
        //     double yMax = -std::numeric_limits<int>::max();
        //     for (auto &it: *pointsRef) {
        //         xMin = std::min(xMin, it.x());
        //         yMin = std::min(yMin, it.y());
        //         xMax = std::max(xMax, it.x());
        //         yMax = std::max(yMax, it.y());
        //     }          
        //     // for each distribution partition
        //     for (int i=0; i < object.getPartitionCount(); i++) {
        //         // get the partition ID, no two layer class set yet
        //         int distPartitionID = object.getPartitionID(i);
        //         // get the partition's indices
        //         int distPartitionIndexX, distPartitionIndexY;
        //         g_config.partitioningMethod->getDistributionGridPartitionIndices(distPartitionID, distPartitionIndexX, distPartitionIndexY);
        //             // calculate the partition's bottom left point coordinates in the distribution grid to get the object's class
        //         double distPartitionXmin = g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal + (distPartitionIndexX * g_config.partitioningMethod->getDistPartionExtentX());
        //         double distPartitionYmin = g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal + (distPartitionIndexY * g_config.partitioningMethod->getDistPartionExtentY());
        //         // get the object's class
        //         TwoLayerClass objectClass = getTwoLayerClassForMBRandPartition(xMin, yMin, distPartitionXmin, distPartitionYmin);
        //         // fill in the partition's two layer class data
        //         object.setPartitionClass(i, objectClass);
        //     }
        //     return DBERR_OK;
        // }
    }

    // DB_STATUS calculateTwoLayerClasses(Batch &batch) {
    //     DB_STATUS ret = DBERR_OK;
    //     // classify based on partitioning method
    //     switch (g_config.partitioningMethod->type) {
    //         case PARTITIONING_ROUND_ROBIN:
    //             #pragma omp parallel
    //             {
    //                 int tid = omp_get_thread_num();
    //                 DB_STATUS local_ret = DBERR_OK;
    //                 #pragma omp for
    //                 for (int i=0; i<batch.objectCount; i++) {
    //                     // determine the two layer classes for each partition of the object in the fine grid
    //                     local_ret = round_robin::setPartitionClassesForObject(batch.objects[i]);
    //                     if (local_ret != DBERR_OK) {
    //                         logger::log_error(ret, "Setting partition classes for object with ID", batch.objects[i].recID, "failed.");
    //                         #pragma omp cancel for
    //                         ret = local_ret;
    //                     }
    //                 }
    //             }
    //             break;
    //         case PARTITIONING_TWO_GRID:
    //             #pragma omp parallel
    //             {
    //                 int tid = omp_get_thread_num();
    //                 DB_STATUS local_ret = DBERR_OK;
    //                 #pragma omp for
    //                 for (int i=0; i<batch.objectCount; i++) {
    //                 // for (int i=0; i<batch.objects.size(); i++) {
    //                     // determine the two layer classes for each partition of the object in the fine grid
    //                     local_ret = two_grid::setPartitionClassesForObject(batch.objects[i]);
    //                     if (local_ret != DBERR_OK) {
    //                         logger::log_error(ret, "Setting partition classes for object with ID", batch.objects[i].recID, "failed.");
    //                         #pragma omp cancel for
    //                         ret = local_ret;
    //                     }
    //                 }
    //             }
    //             break;
    //         default:
    //             logger::log_error(DBERR_INVALID_PARAMETER, "Unknown partitioning method type:", g_config.partitioningMethod->type);
    //             return DBERR_INVALID_PARAMETER;
    //     }

    //     // check if parallel classification compleleted successfully
    //     if (ret != DBERR_OK) {
    //         logger::log_error(ret, "Two layer classification for batch failed.");
    //     }
    //     return ret;
    // }

    // DB_STATUS addBatchToTwoLayerIndex(Dataset* dataset, Batch &batch) {
    //     DB_STATUS ret = DBERR_OK;
    //     // classify based on partitioning method
    //     switch (g_config.partitioningMethod->type) {
    //         case PARTITIONING_TWO_GRID:
    //             // loop objects in batch
    //             for (int i=0; i<batch.objectCount; i++) {
    //                 // determine the two layer classes for each partition of the object in the fine grid
    //                 ret = two_grid::setPartitionClassesForObject(batch.objects[i]);
    //                 if (ret != DBERR_OK) {
    //                     logger::log_error(ret, "Setting partition classes for object with ID", batch.objects[i].recID, "failed.");
    //                     return ret;
    //                 }

    //                 // add to dataset
    //                 ret = dataset->addObject(batch.objects[i]);
    //                 if (ret != DBERR_OK) {
    //                     return ret;
    //                 }
                    
    //                 // if APRIL is enabled
    //                 if (g_config.queryMetadata.IntermediateFilter && (dataset->metadata.dataType == DT_POLYGON || dataset->metadata.dataType == DT_LINESTRING)) {
    //                     // create APRIL for the object 
    //                     AprilData aprilData;
    //                     ret = APRIL::generation::memory::createAPRILforObject(&batch.objects[i], dataset->metadata.dataType, dataset->aprilConfig, aprilData);
    //                     if (ret != DBERR_OK) {
    //                         logger::log_error(ret, "APRIL creation failed for object with id:", batch.objects[i].recID);
    //                         return ret;
    //                     }
    //                     // store APRIL in the object
    //                     // logger::log_task("Generated april for object:", batch.objects[i].recID , ":", aprilData.numIntervalsALL, aprilData.numIntervalsFULL);
    //                     // dataset->addAprilData(0, batch.objects[i].recID, aprilData);
    //                     ret = dataset->setAprilDataForSectionAndObjectID(0, batch.objects[i].recID, aprilData);
    //                     if (ret != DBERR_OK) {
    //                         return ret;
    //                     }
    //                 }
                    
    //             }
    //             break;
    //         default:
    //             logger::log_error(DBERR_INVALID_PARAMETER, "Unknown partitioning method type:", g_config.partitioningMethod->type);
    //             return DBERR_INVALID_PARAMETER;
    //     }

    //     // check if parallel classification compleleted successfully
    //     if (ret != DBERR_OK) {
    //         logger::log_error(ret, "Two layer classification for batch failed.");
    //     }
    //     return ret;
    // }
}