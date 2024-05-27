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

        // logger::log_task("MBR:", xMin, yMin, xMax, yMax);
        // logger::log_task("Partitions min/max:", minPartitionX, minPartitionY, maxPartitionX, maxPartitionY);

        for (int i=minPartitionX; i<=maxPartitionX; i++) {
            for (int j=minPartitionY; j<=maxPartitionY; j++) {
                int partitionID = g_config.partitioningInfo.getCellID(i, j);
                
                // logger::log_task("Partition ID", partitionID);
                
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

    static DB_STATUS loadDatasetAndPartitionBinary(std::string &datasetPath, int polygonCount) {
        int recID;
        int partitionID;
        int vertexCount;
        double x,y;
        double xMin, yMin, xMax, yMax;
        DB_STATUS ret = DBERR_OK;

        // open dataset file stream
        std::ifstream fin(datasetPath, std::fstream::in | std::ios_base::binary);

        //first read the total polygon count of the dataset
        fin.read((char*) &polygonCount, sizeof(int));

        // init batches per node
        std::unordered_map<int,GeometryBatchT> batchMap;
        for (int i=0; i<g_world_size; i++) {
            GeometryBatchT batch;
            batch.destRank = i;
            batch.maxObjectCount = g_config.partitioningInfo.batchSize;
            batchMap.insert(std::make_pair(i,batch));
        }

        //read polygons
        for(int i=0; i<polygonCount; i++){
            //read the polygon id
            fin.read((char*) &recID, sizeof(int)); 
            //read the vertex count
            fin.read((char*) &vertexCount, sizeof(int));
            xMin = std::numeric_limits<int>::max();
            yMin = std::numeric_limits<int>::max();
            xMax = -std::numeric_limits<int>::max();
            yMax = -std::numeric_limits<int>::max();
            // loop points
            std::vector<double> coords;
            coords.reserve(vertexCount);
            for(int j=0; j<vertexCount; j++){
                fin.read((char*) &x, sizeof(double));
                fin.read((char*) &y, sizeof(double));

                // store coordinates temporarily
                coords.emplace_back(x);
                coords.emplace_back(y);

                // compute MBR
                xMin = std::min(xMin, x);
                yMin = std::min(yMin, y);
                xMax = std::max(xMax, x);
                yMax = std::max(yMax, y);
            }

            // find partition IDs
            std::vector<int> partitionIDs;
            ret = partitioning::getPartitionsForMBR(xMin, yMin, xMax, yMax, partitionIDs);
            if (ret != DBERR_OK) {
                goto CLOSE_AND_RETURN;
            }

            // create serializable object
            GeometryT geometry(recID, 0, vertexCount, coords);

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
                    ret = DBERR_INVALID_PARAMETER;
                    goto CLOSE_AND_RETURN;
                }
                GeometryBatchT *batch = &it->second;
                batch->addGeometryToBatch(geometry);

                // if batch is full, send and reset
                if (batch->objectCount >= batch->maxObjectCount) {
                    // send
                    if (batch->destRank != HOST_RANK) {
                        // if the destination is a worker node, send to that node's controller
                        ret = comm::controller::serializeAndSendGeometryBatch(*batch, batch->destRank, MSG_BATCH_POLYGON, g_global_comm);
                        if (ret != DBERR_OK) {
                            goto CLOSE_AND_RETURN;
                        }
                    } else {
                        // else, send to local agent
                        ret = comm::controller::serializeAndSendGeometryBatch(*batch, AGENT_RANK, MSG_BATCH_POLYGON, g_local_comm);
                        if (ret != DBERR_OK) {
                            goto CLOSE_AND_RETURN;
                        }
                        
                    }

                    // reset
                    batch->clear();
                }

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
            if (batch->destRank != HOST_RANK) {
                // if the destination is a worker node, send to that node's controller
                ret = comm::controller::serializeAndSendGeometryBatch(*batch, batch->destRank, MSG_BATCH_POLYGON, g_global_comm);
                if (ret != DBERR_OK) {
                    goto CLOSE_AND_RETURN;
                }
                // send also an empty pack to signify the end of the partitioning for this dataset
                if (batch->objectCount > 0) {
                    // reset and send empty (final) message
                    batch->clear();
                    ret = comm::controller::serializeAndSendGeometryBatch(*batch, batch->destRank, MSG_BATCH_POLYGON, g_global_comm);
                    if (ret != DBERR_OK) {
                        goto CLOSE_AND_RETURN;
                    }
                }
            } else {
                // else, send to local agent
                ret = comm::controller::serializeAndSendGeometryBatch(*batch, AGENT_RANK, MSG_BATCH_POLYGON, g_local_comm);
                if (ret != DBERR_OK) {
                    goto CLOSE_AND_RETURN;
                }
                // send also an empty pack to signify the end of the partitioning for this dataset
                if (batch->objectCount > 0) {     
                    // reset and send empty (final message)
                    batch->clear();
                    ret = comm::controller::serializeAndSendGeometryBatch(*batch, AGENT_RANK, MSG_BATCH_POLYGON, g_local_comm);
                    if (ret != DBERR_OK) {
                        goto CLOSE_AND_RETURN;
                    }
                }
            }
        }

CLOSE_AND_RETURN:
        fin.close();
        return ret;
    }

    static DB_STATUS loadDatasetAndPartitionBinaryOptimized(std::string &datasetPath, int polygonCount) {
        int recID;
        int partitionID;
        int vertexCount;
        double x,y;
        double xMin, yMin, xMax, yMax;
        DB_STATUS ret = DBERR_OK;

        // open dataset file stream
        FILE* pFile = fopen(datasetPath.c_str(), "rb");

        //first read the total polygon count of the dataset
        fread(&polygonCount, sizeof(int), 1, pFile);

        // init batches per node
        std::unordered_map<int,GeometryBatchT> batchMap;
        for (int i=0; i<g_world_size; i++) {
            GeometryBatchT batch;
            batch.destRank = i;
            batch.maxObjectCount = g_config.partitioningInfo.batchSize;
            batchMap.insert(std::make_pair(i,batch));
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

            // find partition IDs
            std::vector<int> partitionIDs;
            ret = partitioning::getPartitionsForMBR(xMin, yMin, xMax, yMax, partitionIDs);
            if (ret != DBERR_OK) {
                goto CLOSE_AND_RETURN;
            }

            // create serializable object
            GeometryT geometry(recID, 0, vertexCount, coords);

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
                    ret = DBERR_INVALID_PARAMETER;
                    goto CLOSE_AND_RETURN;
                }
                GeometryBatchT *batch = &it->second;
                batch->addGeometryToBatch(geometry);
                // if batch is full, send and reset
                if (batch->objectCount >= batch->maxObjectCount) {
                    // send
                    if (batch->destRank != HOST_RANK) {
                        // if the destination is a worker node, send to that node's controller
                        ret = comm::controller::serializeAndSendGeometryBatch(*batch, batch->destRank, MSG_BATCH_POLYGON, g_global_comm);
                        if (ret != DBERR_OK) {
                            goto CLOSE_AND_RETURN;
                        }
                    } else {
                        // else, send to local agent
                        ret = comm::controller::serializeAndSendGeometryBatch(*batch, AGENT_RANK, MSG_BATCH_POLYGON, g_local_comm);
                        if (ret != DBERR_OK) {
                            goto CLOSE_AND_RETURN;
                        }
                        
                    }
                    // reset
                    batch->clear();
                }

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
            if (batch->destRank != HOST_RANK) {
                // if the destination is a worker node, send to that node's controller
                ret = comm::controller::serializeAndSendGeometryBatch(*batch, batch->destRank, MSG_BATCH_POLYGON, g_global_comm);
                if (ret != DBERR_OK) {
                    goto CLOSE_AND_RETURN;
                }
                // send also an empty pack to signify the end of the partitioning for this dataset
                if (batch->objectCount > 0) {
                    // reset and send empty (final) message
                    batch->clear();
                    ret = comm::controller::serializeAndSendGeometryBatch(*batch, batch->destRank, MSG_BATCH_POLYGON, g_global_comm);
                    if (ret != DBERR_OK) {
                        goto CLOSE_AND_RETURN;
                    }
                }
            } else {
                // else, send to local agent
                ret = comm::controller::serializeAndSendGeometryBatch(*batch, AGENT_RANK, MSG_BATCH_POLYGON, g_local_comm);
                if (ret != DBERR_OK) {
                    goto CLOSE_AND_RETURN;
                }
                // send also an empty pack to signify the end of the partitioning for this dataset
                if (batch->objectCount > 0) {     
                    // reset and send empty (final message)
                    batch->clear();
                    ret = comm::controller::serializeAndSendGeometryBatch(*batch, AGENT_RANK, MSG_BATCH_POLYGON, g_local_comm);
                    if (ret != DBERR_OK) {
                        goto CLOSE_AND_RETURN;
                    }
                }
            }
        }

CLOSE_AND_RETURN:
        fclose(pFile);
        return ret;
    }

    static DB_STATUS performPartitioningBinary(spatial_lib::DatasetT *dataset) {
        int polygonCount;
        DB_STATUS ret = DBERR_OK;
        // first, broadcast the dataset info message
        ret = comm::controller::broadcastDatasetInfo(dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
        
        // load data and partition
        // ret = loadDatasetAndPartitionBinary(dataset->path, polygonCount);
        ret = loadDatasetAndPartitionBinaryOptimized(dataset->path, polygonCount);
        if (ret != DBERR_OK) {
            return ret;
        }

        return DBERR_OK;
    }

    DB_STATUS partitionDataset(spatial_lib::DatasetT *dataset) {
        double startTime;
        DB_STATUS ret;
        switch (dataset->fileType) {
            case spatial_lib::FT_BINARY:
                startTime = mpi_timer::markTime();
                ret = performPartitioningBinary(dataset);
                logger::log_success("Partitioning for dataset", dataset->nickname, "finished in", mpi_timer::getElapsedTime(startTime), "seconds.");
                if (ret != DBERR_OK) {
                    return ret;
                }
                break;
            case spatial_lib::FT_CSV:
            case spatial_lib::FT_WKT:
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported file type: ", dataset->fileType);
                break;
        }


        return DBERR_OK;
    }
    
}