#include "env/partitioning.h"

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

    static DB_STATUS loadAndPartition(std::ifstream &fin, int polygonCount, std::vector<BatchT> &batches) {
        int recID;
        int partitionID;
        int vertexCount;
        double x,y;
        double xMin, yMin, xMax, yMax;
        DB_STATUS ret = DBERR_OK;

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

            std::vector<double> coords;
            coords.reserve(vertexCount);
            for(int i=0; i<vertexCount; i++){
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
                return ret;
            }

            // add to buffers for the batch
            for (auto partitionID : partitionIDs) {
                int nodeRank = g_config.partitioningInfo.getNodeRankForPartitionID(partitionID);
                BatchT *batch = &batches.at(nodeRank);
                batch->addObjectToBatch(recID, partitionID, vertexCount, coords);

                // if batch is full, send and reset
                if (batch->size >= batch->maxSize) {
                    
                    // send
                    if (batch->nodeRank != HOST_RANK) {
                        // if the destination is a worker node, send to that node's controller
                        ret = comm::controller::sendGeometryBatchToNode(*batch, batch->nodeRank, MSG_BATCH_POLYGON);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    } else {
                        // else, send to local agent
                        ret = comm::controller::sendGeometryBatchToAgent(*batch, MSG_BATCH_POLYGON);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        
                    }

                    // reset
                    batch->clearBatch();
                }

            }
        }

        // send any remaining batches
        for (auto &batch : batches) {
            // send
            if (batch.nodeRank != HOST_RANK) {
                // if the destination is a worker node, send to that node's controller
                ret = comm::controller::sendGeometryBatchToNode(batch, batch.nodeRank, MSG_BATCH_POLYGON);
                if (ret != DBERR_OK) {
                    return ret;
                }
            } else {
                // else, send to local agent
                ret = comm::controller::sendGeometryBatchToAgent(batch, MSG_BATCH_POLYGON);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }

            // reset
            batch.clearBatch();
        }


        return ret;
    }

    static DB_STATUS performPartitioningBinary(spatial_lib::DatasetT &dataset) {
        int polygonCount;
        // open file stream
        std::ifstream fin(dataset.path, std::fstream::in | std::ios_base::binary);

        /* INFO PACKS */
        // object count | rec ID | section ID | vertex count | ... |
        /* COORDS PACKS */
        // x | y | x | y | ... |

        // Node maps for each pack. Reserve a large chunk in advance.
        std::vector<BatchT> batches;
        for (int i=0; i<g_world_size; i++) {
            BatchT batch;
            batch.nodeRank = i;
            batch.maxSize = g_config.partitioningInfo.batchSize;
            batch.size = 0;
            
            std::vector<int> infoBuf;
            infoBuf.reserve(1 + g_config.partitioningInfo.batchSize * 3 * 3);
            infoBuf.emplace_back(0);    // current size

            std::vector<double> coordsBuf;
            coordsBuf.reserve(g_config.partitioningInfo.batchSize * 2 * 500);

            batch.infoPack = infoBuf;
            batch.coordsPack = coordsBuf;
            
            batches.emplace_back(batch);
        }


        //first read the total polygon count of the dataset
        fin.read((char*) &polygonCount, sizeof(int));


        // load data and partition
        DB_STATUS ret = loadAndPartition(fin, polygonCount, batches);
        if (ret != DBERR_OK) {
            return ret;
        }



        fin.close();

        return DBERR_OK;
    }

    DB_STATUS partitionDataset(spatial_lib::DatasetT &dataset) {
        DB_STATUS ret;
        switch (dataset.fileType) {
            case spatial_lib::FT_BINARY:
                ret = performPartitioningBinary(dataset);
                break;
            case spatial_lib::FT_CSV:
            case spatial_lib::FT_WKT:
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported file type: ", dataset.fileType);
                break;
        }


        return DBERR_OK;
    }
    
}