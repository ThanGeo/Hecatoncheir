#include "env/partitioning.h"
#include "env/comm_worker.h"
#include "storage/read.h"

#include <fstream>
#include <bitset>
#include <fcntl.h>
#include <sys/mman.h>
#include <atomic>
#include <omp.h>

namespace partitioning
{
    DB_STATUS getPartitionsForMBR(MBR &mbr, std::vector<int> &partitionIDs){
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

    DB_STATUS initializeBatchMap(std::unordered_map<int,Batch> &batchMap, DataType dataType, DatasetIndex datasetID) {
        // initialize batches
        for (int i=1; i<g_world_size; i++) {
            Batch batch;
            batch.dataType = dataType;
            batch.datasetID = datasetID;
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
            batch.comm = &g_worker_comm;
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
        for (int nodeRank=1; nodeRank<bitVector.size(); nodeRank++) {
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
                    ret = comm::worker::serializeAndSendGeometryBatch(&it->second);
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
            #pragma omp parallel num_threads(MAX_THREADS) firstprivate(line, objectCount) reduction(min:global_xMin) reduction(min:global_yMin) reduction(max:global_xMax)  reduction(max:global_yMax)
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
            ret = initializeBatchMap(batchMap, dataset->metadata.dataType, dataset->metadata.internalID);
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
            #pragma omp parallel num_threads(MAX_THREADS) firstprivate(batchMap, line, objectCount) reduction(+:batchesSent)
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
                    for (int i=1; i<g_world_size; i++) {
                        // fetch batch
                        auto it = batchMap.find(i);
                        if (it == batchMap.end()) {
                            logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                            #pragma omp cancel parallel
                            ret = DBERR_INVALID_PARAMETER;
                        }
                        Batch *batch = &it->second;
                        if (batch->objectCount > 0) {
                            // local_ret = comm::controller::serializeAndSendGeometryBatch(batch);
                            /** @todo */
                            #pragma omp cancel parallel
                            ret = DBERR_FEATURE_UNSUPPORTED;
                            break;
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
            for (int i=1; i<g_world_size; i++) {
                // fetch batch (it is guaranteed to be empty)
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    return DBERR_INVALID_PARAMETER;
                }
                Batch *batch = &it->second;
                // ret = comm::controller::serializeAndSendGeometryBatch(batch);
                /** @todo */
                return DBERR_FEATURE_UNSUPPORTED;
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
            #pragma omp parallel num_threads(MAX_THREADS) firstprivate(line, totalObjects) reduction(min:global_xMin) reduction(min:global_yMin) reduction(max:global_xMax)  reduction(max:global_yMax)
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
            ret = initializeBatchMap(batchMap, dataset->metadata.dataType, dataset->metadata.internalID);
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
            #pragma omp parallel num_threads(MAX_THREADS) firstprivate(batchMap, line, totalObjects) reduction(+:batchesSent) reduction(+:totalValidObjects)
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

                // read lines
                while (currentLine < toLine) {
                    // read next object
                    std::getline(fin, line);
                    if (line.empty()) {
                        break;
                    }
                    // parse line to get only the first column (wkt geometry)
                    std::stringstream ss(line);
                    std::getline(ss, token, '\t');
                    
                    // create empty object based on data type
                    Shape object;
                    local_ret = shape_factory::createEmpty(dataset->metadata.dataType, object);
                    if (local_ret != DBERR_OK) {
                        // error creating shape
                        #pragma omp cancel parallel
                        ret = local_ret;
                        break;
                    } 
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
                } // end of loop

                // send any remaining non-empty batches
                for (int i=1; i<g_world_size; i++) {
                    // fetch batch
                    auto it = batchMap.find(i);
                    if (it == batchMap.end()) {
                        logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                        #pragma omp cancel parallel
                        ret = DBERR_INVALID_PARAMETER;
                    }
                    Batch *batch = &it->second;
                    if (batch->objectCount > 0) {
                        local_ret = comm::worker::serializeAndSendGeometryBatch(batch);
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
            } // end of parallel region
            // check if operation finished successfully
            if (ret != DBERR_OK) {
                return ret;
            }
            // set total objects value
            dataset->totalObjects = dataset->objects.size();

            // logger::log_success("Partitioned", totalValidObjects, "valid objects");
            // send an empty pack to each worker to signal the end of the partitioning for this dataset
            for (int i=1; i<g_world_size; i++) {
                // fetch batch (it is guaranteed to be empty)
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    return DBERR_INVALID_PARAMETER;
                }
                Batch *batch = &it->second;
                ret = comm::worker::serializeAndSendGeometryBatch(batch);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // empty the batch
                batch->clear();
            }
            // logger::log_success("Sent", batchesSent, "non-empty batches.");
            return ret;
        }

        static DB_STATUS loadDatasetAndPartitionMMAP(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;

            // Initialize batches
            std::unordered_map<int, Batch> batchMap;
            ret = initializeBatchMap(batchMap, dataset->metadata.dataType, dataset->metadata.internalID);
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
            // size_t lineCounter = 0;
            std::atomic<size_t> lineCounter(0);

            #pragma omp parallel num_threads(MAX_THREADS) firstprivate(batchMap) reduction(+:batchesSent) reduction(+:totalValidObjects)
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

                        // object.recID = lineStart; // Use file offset as ID
                        object.recID = lineCounter.fetch_add(1, std::memory_order_relaxed);

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
                for (int i = 1; i < g_world_size; i++) {
                    auto it = batchMap.find(i);
                    if (it == batchMap.end()) {
                        logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                        #pragma omp cancel parallel
                        ret = DBERR_INVALID_PARAMETER;
                    }
                    Batch* batch = &it->second;
                    if (batch->objectCount > 0) {
                        // local_ret = comm::controller::serializeAndSendGeometryBatch(batch);
                        /** @todo */
                        #pragma omp cancel parallel
                        ret = DBERR_FEATURE_UNSUPPORTED;
                        break;
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
            for (int i = 1; i < g_world_size; i++) {
                auto it = batchMap.find(i);
                if (it == batchMap.end()) {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Error fetching batch for node", i);
                    return DBERR_INVALID_PARAMETER;
                }
                Batch* batch = &it->second;
                // ret = comm::controller::serializeAndSendGeometryBatch(batch);
                /** @todo */
                return DBERR_FEATURE_UNSUPPORTED;
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

    } // wkt namespace
    
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
                ret = wkt::loadDatasetAndPartition(dataset);
                // ret = wkt::loadDatasetAndPartitionMMAP(dataset);
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
}