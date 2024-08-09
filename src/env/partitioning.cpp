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
    static DB_STATUS getPartitionsForMBR(double xMin, double yMin, double xMax, double yMax, std::vector<int> &partitionIDs){
        int minPartitionX = (xMin - g_config.datasetInfo.dataspaceInfo.xMinGlobal) / (g_config.datasetInfo.dataspaceInfo.xExtent / g_config.partitioningMethod->getDistributionPPD());
        int minPartitionY = (yMin - g_config.datasetInfo.dataspaceInfo.yMinGlobal) / (g_config.datasetInfo.dataspaceInfo.yExtent / g_config.partitioningMethod->getDistributionPPD());
        int maxPartitionX = (xMax - g_config.datasetInfo.dataspaceInfo.xMinGlobal) / (g_config.datasetInfo.dataspaceInfo.xExtent / g_config.partitioningMethod->getDistributionPPD());
        int maxPartitionY = (yMax - g_config.datasetInfo.dataspaceInfo.yMinGlobal) / (g_config.datasetInfo.dataspaceInfo.yExtent / g_config.partitioningMethod->getDistributionPPD());
        
        int startPartitionID = g_config.partitioningMethod->getDistributionGridPartitionID(minPartitionX, minPartitionY);
        int lastPartitionID = g_config.partitioningMethod->getDistributionGridPartitionID(maxPartitionX, maxPartitionY);
        if (startPartitionID < 0 || startPartitionID > g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD() -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Start partition ID calculated wrong");
            return DBERR_INVALID_PARTITION;
        }
        if (lastPartitionID < 0 || lastPartitionID > g_config.partitioningMethod->getDistributionPPD() * g_config.partitioningMethod->getDistributionPPD() -1) {
            logger::log_error(DBERR_INVALID_PARTITION, "Last partition ID calculated wrong: MBR(", xMin, yMin, xMax, yMax, ")");
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

    static DB_STATUS initializeBatchMap(std::unordered_map<int,GeometryBatch> &batchMap) {
        // initialize batches
        for (int i=0; i<g_world_size; i++) {
            GeometryBatch batch;
            batch.destRank = i;
            if (i > 0) {
                batch.comm = &g_global_comm;
            } else {
                batch.comm = &g_local_comm;
            }
            batch.tag = MSG_BATCH_POLYGON;
            batch.maxObjectCount = g_config.partitioningMethod->getBatchSize();
            batchMap.insert(std::make_pair(i,batch));
        }
        return DBERR_OK;
    }

    /** @brief Assigns a geometry to the appropriate batches based on overlapping partition IDs.
     * If the batch is full after the insertion, it is sent and cleared before returning.
     */
    static DB_STATUS assignGeometryToBatches(Geometry &geometry, double geoXmin, double geoYmin, double geoXmax, double geoYmax, std::unordered_map<int,GeometryBatch> &batchMap, int &batchesSent) {
        // printf("Handling geometry with id %ld\n", geometry.recID);
        // find partition IDs and the class of the geometry in each distribution partition
        std::vector<int> partitionIDs;
        DB_STATUS ret = partitioning::getPartitionsForMBR(geoXmin, geoYmin, geoXmax, geoYmax, partitionIDs);
        if (ret != DBERR_OK) {
            return ret;
        }
        // set partitions to object
        geometry.setPartitions(partitionIDs);
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
                GeometryBatch *batch = &it->second;

                // make a copy of the geometry, to adjust the partitions specifically for this node
                // remove any partitions that are irrelevant to this noderank
                Geometry geometryCopy = geometry;
                for(auto it = geometryCopy.partitions.begin(); it != geometryCopy.partitions.end();) {
                    int assignedNodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(*it);
                    if (assignedNodeRank != nodeRank) {
                        it = geometryCopy.partitions.erase(it);
                        it = geometryCopy.partitions.erase(it);
                        geometryCopy.partitionCount--;
                    } else {
                        it += 2;
                    }
                }
                // add moddified geometry to this batch
                batch->addGeometryToBatch(geometryCopy);
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

    DB_STATUS calculateCSVDatasetDataspaceBounds(Dataset &dataset) {
        DB_STATUS ret = DBERR_OK;
        // open file
        std::ifstream fin(dataset.path);
        if (!fin.is_open()) {
            logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset.path);
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
            std::ifstream fin(dataset.path);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", dataset.path);
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
                recID = (size_t) std::stoull(token);
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
        dataset.dataspaceInfo.set(global_xMin, global_yMin, global_xMax, global_yMax);
        return ret;
    }

    static DB_STATUS loadCSVDatasetAndPartition(std::string &datasetPath) {
        DB_STATUS ret = DBERR_OK;
        // initialize batches
        std::unordered_map<int,GeometryBatch> batchMap;
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
        size_t objectCount = (size_t) std::stoull(line);
        // count how many batches have been sent in total
        int batchesSent = 0;
        #pragma omp parallel firstprivate(batchMap, line, objectCount) reduction(+:batchesSent)
        {
            size_t recID;
            int partitionID;
            int vertexCount;
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
            std::ifstream fin(datasetPath);
            if (!fin.is_open()) {
                logger::log_error(DBERR_MISSING_FILE, "Failed to open dataset path:", datasetPath);
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
                // printf("Parsing line %ld/%ld\n", currentLine, toLine);
                // next object
                std::getline(fin, line);                
                std::stringstream ss(line);
                xMin = std::numeric_limits<int>::max();
                yMin = std::numeric_limits<int>::max();
                xMax = -std::numeric_limits<int>::max();
                yMax = -std::numeric_limits<int>::max();
                // recID
                std::getline(ss, token, ',');
                recID = (size_t) std::stoull(token);
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
                Geometry geometry(recID, vertexCount, coords);
                // assign to appropriate batches
                local_ret = assignGeometryToBatches(geometry, xMin, yMin, xMax, yMax, batchMap, batchesSent);
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
                GeometryBatch *batch = &it->second;
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
        }
        // check if it finished successfully
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
            GeometryBatch *batch = &it->second;
            ret = comm::controller::serializeAndSendGeometryBatch(batch);
            if (ret != DBERR_OK) {
                return ret;
            }
        }
        // logger::log_success("Sent", batchesSent, "non-empty batches.");
        return ret;
    }

    static DB_STATUS performPartitioningCSV(Dataset &dataset) {
        DB_STATUS ret = DBERR_OK;  

        // first, issue the partitioning instruction
        ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_PARTITIONING_INIT);
        if (ret != DBERR_OK) {
            return ret;
        }
        
        // broadcast the dataset info to the nodes
        ret = comm::controller::host::broadcastDatasetInfo(&dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
       
        // finally, load data and partition to workers
        ret = loadCSVDatasetAndPartition(dataset.path);
        if (ret != DBERR_OK) {
            return ret;
        }

        return ret;
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
                ret = performPartitioningCSV(*dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                break;
            case FT_BINARY:
            case FT_WKT:
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
         *  and sets the geometry's class in each partition. */
        static DB_STATUS setPartitionClassesForGeometry(Geometry &geometry){
            // get object's MBR
            double xMin = std::numeric_limits<int>::max();
            double yMin = std::numeric_limits<int>::max();
            double xMax = -std::numeric_limits<int>::max();
            double yMax = -std::numeric_limits<int>::max();
            for (int i=0; i<geometry.coords.size(); i+= 2) {
                xMin = std::min(xMin, geometry.coords[i]);
                yMin = std::min(yMin, geometry.coords[i+1]);
                xMax = std::max(xMax, geometry.coords[i]);
                yMax = std::max(yMax, geometry.coords[i+1]);
            }             
            // vector for the new partitions
            std::vector<int> newPartitions;
            // for each distribution partition
            for (int i=0; i < geometry.partitionCount; i++) {
                // get the partition ID, no two layer class set yet
                int distPartitionID = geometry.partitions[i*2];
                // get the partition's indices
                int distPartitionIndexX, distPartitionIndexY;
                g_config.partitioningMethod->getDistributionGridPartitionIndices(distPartitionID, distPartitionIndexX, distPartitionIndexY);
                // get the global partition's extents
                // printf("Distribution grid partition ID: %d (%d,%d)\n", distPartitionID, distPartitionIndexX, distPartitionIndexY);
                // calculate the partition's bounds in the distribution grid
                double distPartitionXmin = g_config.partitioningMethod->distGridDataspaceInfo.xMinGlobal + (distPartitionIndexX * g_config.partitioningMethod->getDistPartionExtentX());
                double distPartitionYmin = g_config.partitioningMethod->distGridDataspaceInfo.yMinGlobal + (distPartitionIndexY * g_config.partitioningMethod->getDistPartionExtentY());
                double distPartitionXmax = g_config.partitioningMethod->distGridDataspaceInfo.xMinGlobal + ((distPartitionIndexX+1) * g_config.partitioningMethod->getDistPartionExtentX());
                double distPartitionYmax = g_config.partitioningMethod->distGridDataspaceInfo.yMinGlobal + ((distPartitionIndexY+1) * g_config.partitioningMethod->getDistPartionExtentY());
                // printf("Distribution grid partition bounds: (%f,%f),(%f,%f),(%f,%f),(%f,%f)\n", distPartitionXmin, distPartitionYmin, distPartitionXmax, distPartitionYmin, distPartitionXmax, distPartitionYmax, distPartitionXmin, distPartitionYmax);
                TwoLayerClass objectClassInDistributionPartition = getTwoLayerClassForMBRandPartition(xMin, yMin, distPartitionXmin, distPartitionYmin);
                // printf("Object class in parent partition: %s\n", mapping::twoLayerClassIntToStr(objectClassInDistributionPartition).c_str());
                // get the indices of the start/end part partitions
                int localPartitionXstart = (xMin - distPartitionXmin) / g_config.partitioningMethod->getPartPartionExtentX();
                int localPartitionYstart = (yMin - distPartitionYmin) / g_config.partitioningMethod->getPartPartionExtentY();
                int localPartitionXend = (xMax - distPartitionXmin) / g_config.partitioningMethod->getPartPartionExtentX();
                int localPartitionYend = (yMax - distPartitionYmin) / g_config.partitioningMethod->getPartPartionExtentY();
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
                // printf("After adjustment: Local partition start/end: (%d,%d),(%d,%d)\n", localPartitionXstart, localPartitionYstart, localPartitionXend, localPartitionYend);
                int totalNewPartitions = (localPartitionXend - localPartitionXstart + 1) * (localPartitionYend - localPartitionYstart + 1);
                // vector for the new partitions
                newPartitions.reserve(newPartitions.size() + totalNewPartitions*2);
                // first partition inherits the class of the parent partition
                newPartitions.emplace_back(g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, localPartitionXstart, localPartitionYstart));
                newPartitions.emplace_back(objectClassInDistributionPartition);
                // for each local partition, get the object's class
                for (int indexI=localPartitionXstart; indexI <= localPartitionXend; indexI++) {
                    if (indexI != localPartitionXstart) {
                        // next partitions on X axis
                        // add partition ID
                        newPartitions.emplace_back(g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, indexI, localPartitionYstart));
                        if (objectClassInDistributionPartition == CLASS_B) {
                            // if in parent partition it is B, 
                            // then in the next partition on the X axis would be D 
                            // (as the object would be before this partition in both axes)
                            newPartitions.emplace_back(CLASS_D);
                            // printf("Instant object class: %s\n", mapping::twoLayerClassIntToStr(CLASS_D).c_str());
                        } else {
                            // otherwise its C
                            newPartitions.emplace_back(CLASS_C);
                            // printf("Instant object class: %s\n", mapping::twoLayerClassIntToStr(CLASS_C).c_str());
                        }
                        // validity prints
                        // double currentPartitionXmin = distPartitionXmin + (indexI * g_config.partitioningMethod->getPartPartionExtentX());
                        // double currentPartitionYmin = distPartitionYmin + (localPartitionYstart * g_config.partitioningMethod->getPartPartionExtentY());
                        // double currentPartitionXmax = distPartitionXmin + ((indexI+1) * g_config.partitioningMethod->getPartPartionExtentX());//debug only, remove after
                        // double currentPartitionYmax = distPartitionYmin + ((localPartitionYstart+1) * g_config.partitioningMethod->getPartPartionExtentY());//debug only, remove after
                        // TwoLayerClass objectClassInPartition = getTwoLayerClassForMBRandPartition(xMin, yMin, currentPartitionXmin, currentPartitionYmin);
                        // printf("Local partition: (%d,%d)\n", indexI, localPartitionYstart);
                        // printf("Local partition bounds: (%f,%f),(%f,%f),(%f,%f),(%f,%f)\n", currentPartitionXmin, currentPartitionYmin, currentPartitionXmax, currentPartitionYmin, currentPartitionXmax, currentPartitionYmax, currentPartitionXmin, currentPartitionYmax);
                        // printf("Calculated object class: %s\n", mapping::twoLayerClassIntToStr(objectClassInPartition).c_str());
                    }
                    for (int indexJ=localPartitionYstart+1; indexJ <= localPartitionYend; indexJ++) {
                        // next partitions on Y axis
                        // add partition ID
                        newPartitions.emplace_back(g_config.partitioningMethod->getPartitioningGridPartitionID(distPartitionIndexX, distPartitionIndexY, indexI, indexJ));
                        if (indexI == localPartitionXstart) {
                            if (objectClassInDistributionPartition == CLASS_C) {
                                // if in parent partition it is C, 
                                // then in the next partition on the Y axis would be D 
                                // (as the object would be before this partition in both axes)
                                newPartitions.emplace_back(CLASS_D);
                                // printf("Instant object class: %s\n", mapping::twoLayerClassIntToStr(CLASS_D).c_str());
                            } else {
                                // otherwise its B
                                newPartitions.emplace_back(CLASS_B);
                                // printf("Instant object class: %s\n", mapping::twoLayerClassIntToStr(CLASS_B).c_str());
                            }
                        } else {
                            // D always
                            newPartitions.emplace_back(CLASS_D);
                            // printf("Instant object class: %s\n", mapping::twoLayerClassIntToStr(CLASS_D).c_str());
                        }
                    }
                }
            }
            // replace into object
            geometry.partitions = newPartitions;
            geometry.partitionCount = newPartitions.size() / 2;
            
            return DBERR_OK;
        }
    }

    namespace round_robin
    {
        /** @brief The agent sets the geometry's class in its partitions, as set by the round robin partitioning. */
        static DB_STATUS setPartitionClassesForGeometry(Geometry &geometry){
            // get object's MBR
            double xMin = std::numeric_limits<int>::max();
            double yMin = std::numeric_limits<int>::max();
            double xMax = -std::numeric_limits<int>::max();
            double yMax = -std::numeric_limits<int>::max();
            for (int i=0; i<geometry.coords.size(); i+= 2) {
                xMin = std::min(xMin, geometry.coords[i]);
                yMin = std::min(yMin, geometry.coords[i+1]);
                xMax = std::max(xMax, geometry.coords[i]);
                yMax = std::max(yMax, geometry.coords[i+1]);
            }
            // for each distribution partition
            for (int i=0; i < geometry.partitionCount; i++) {
                // get the partition ID, no two layer class set yet
                int distPartitionID = geometry.partitions[i*2];
                // get the partition's indices
                int distPartitionIndexX, distPartitionIndexY;
                g_config.partitioningMethod->getDistributionGridPartitionIndices(distPartitionID, distPartitionIndexX, distPartitionIndexY);
                    // calculate the partition's bottom left point coordinates in the distribution grid to get the object's class
                double distPartitionXmin = g_config.partitioningMethod->distGridDataspaceInfo.xMinGlobal + (distPartitionIndexX * g_config.partitioningMethod->getDistPartionExtentX());
                double distPartitionYmin = g_config.partitioningMethod->distGridDataspaceInfo.yMinGlobal + (distPartitionIndexY * g_config.partitioningMethod->getDistPartionExtentY());
                // get the object's class
                TwoLayerClass objectClass = getTwoLayerClassForMBRandPartition(xMin, yMin, distPartitionXmin, distPartitionYmin);
                // fill in the partition's two layer class data
                geometry.partitions.at((i*2)+1) = objectClass;
            }
            return DBERR_OK;
        }
    }

    DB_STATUS calculateTwoLayerClasses(GeometryBatch &batch) {
        DB_STATUS ret = DBERR_OK;

        /** @todo parallelize */

        switch (g_config.partitioningMethod->type) {
            case PARTITIONING_ROUND_ROBIN:
                for (auto &it : batch.geometries) {
                    // determine the two layer classes for each partition of the object in the fine grid
                    ret = round_robin::setPartitionClassesForGeometry(it);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Setting partition classes for object with ID", it.recID, "failed.");
                        return ret;
                    }
                }
                break;
            case PARTITIONING_TWO_GRID:
                for (auto &it : batch.geometries) {
                    // determine the two layer classes for each partition of the object in the fine grid
                    ret = two_grid::setPartitionClassesForGeometry(it);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Setting partition classes for object with ID", it.recID, "failed.");
                        return ret;
                    }
                }
                break;
            default:
                logger::log_error(DBERR_INVALID_PARAMETER, "Unknown partitioning method type:", g_config.partitioningMethod->type);
                return DBERR_INVALID_PARAMETER;
        }

        return ret;
    }
        
}