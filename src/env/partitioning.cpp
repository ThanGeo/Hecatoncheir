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

        // sanity check
        if (HOST_RANK != AGENT_RANK) {
            // this will never happen unless somebody did a malakia
            logger::log_error(DBERR_INVALID_PARAMETER, "Host node rank and every agent's rank must match");
            return DBERR_INVALID_PARAMETER;
        }

        // initialize batches
        std::unordered_map<int,GeometryBatchT> batchMap;
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
                    ret = comm::controller::serializeAndSendGeometryBatch(batch);
                    if (ret != DBERR_OK) {
                        goto CLOSE_AND_RETURN;
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
        int polygonCount;
        DB_STATUS ret = DBERR_OK;  

        // first, broadcast the dataset info message
        ret = comm::controller::broadcastDatasetInfo(dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
       
        // load data and partition
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
                // perform the partitioning
                ret = performPartitioningBinary(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Partitioning failed for dataset", dataset->nickname);
                    return ret;
                }
                // wait for response by workers+agent that all is ok
                ret = comm::controller::gatherResponses();
                if (ret != DBERR_OK) {
                    return ret;
                }
                logger::log_success("Partitioning for dataset", dataset->nickname, "finished in", mpi_timer::getElapsedTime(startTime), "seconds.");
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