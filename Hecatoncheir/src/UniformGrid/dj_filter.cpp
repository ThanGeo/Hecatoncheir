#include "UniformGrid/filter.h"
#include <omp.h>

namespace uniform_grid
{
    namespace distance_filter
    {
        /** @brief For the given i,j offset and start partition i,j, it returns the partition ID that it corresponds to and the node that is responsible for it. */
        static DB_STATUS getPartitionIDandNodeRankForOffset(int iFine, int jFine, std::pair<int, int> &offset, int &overlapPartitionID, int &nodeRank) {
            overlapPartitionID = g_config.partitioningMethod->getPartitionID(
                iFine + offset.first, jFine + offset.second, 
                g_config.partitioningMethod->getGlobalPPD());

            if (overlapPartitionID < 0) {
                logger::log_error(DBERR_INVALID_PARTITION, "Invalid partition id:", overlapPartitionID, "for i,j", iFine, jFine, "and offsets", offset.first, offset.second);
                return DBERR_INVALID_PARTITION;
            }

            int oFineI = overlapPartitionID % g_config.partitioningMethod->getGlobalPPD(); 
            int oFineJ = overlapPartitionID / g_config.partitioningMethod->getGlobalPPD(); 

            int oCoarseI = oFineI / g_config.partitioningMethod->getPartitioningPPD();
            int oCoarseJ = oFineJ / g_config.partitioningMethod->getPartitioningPPD();
            int coarsePartitionID = g_config.partitioningMethod->getPartitionID(oCoarseI, oCoarseJ, g_config.partitioningMethod->getDistributionPPD());
            nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(coarsePartitionID);

            return DBERR_OK;
        }

        DB_STATUS evaluate(hec::DistanceJoinQuery *distanceJoinQuery, std::unordered_map<int, DJBatch>& borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetOptions.getDatasetByIdx(distanceJoinQuery->getDatasetRid());
            Dataset* S = g_config.datasetOptions.getDatasetByIdx(distanceJoinQuery->getDatasetSid());

            // Thread-local storage for border objects (one per thread)
            std::vector<std::unordered_map<int, DJBatch>> threadLocalBorderMaps(MAX_THREADS);

            #pragma omp parallel num_threads(MAX_THREADS) reduction(query_output_reduction:queryResult)
            {
                int tid = omp_get_thread_num();
                auto& localBorderMap = threadLocalBorderMaps[tid];
                std::vector<PartitionBase *>* partitions = R->index->getPartitions();
                DB_STATUS local_ret = DBERR_OK;
                int overlapPartitionID; 
                int nodeRank;
                #pragma omp for
                for (int i = 0; i < partitions->size(); i++) {
                    PartitionBase* partitionR = partitions->at(i);
                    int iFine = partitionR->partitionID % g_config.partitioningMethod->getGlobalPPD(); 
                    int jFine = partitionR->partitionID / g_config.partitioningMethod->getGlobalPPD(); 

                    std::vector<Shape*>* objectsR = partitionR->getContents();
                    if (objectsR != nullptr) {
                        for (auto &obj : *objectsR) {
                            //  Check if this R object needs to be distributed
                            std::vector<std::pair<int,int>> overlappingPartitionOffsets = obj->getOverlappingPartitionOffsets(
                                iFine, jFine, distanceJoinQuery->getDistanceValue(),
                                g_config.partitioningMethod->getPartPartionExtentX(),
                                g_config.partitioningMethod->getPartPartionExtentY(),
                                g_config.datasetOptions.dataspaceMetadata.xMinGlobal,
                                g_config.datasetOptions.dataspaceMetadata.yMinGlobal,
                                g_config.partitioningMethod->getGlobalPPD());

                            for (auto &offset : overlappingPartitionOffsets) {
                                local_ret = getPartitionIDandNodeRankForOffset(iFine, jFine, offset, overlapPartitionID, nodeRank);
                                if (local_ret != DBERR_OK) {
                                    #pragma omp cancel for
                                    ret = local_ret;
                                }
                                    
                                if (nodeRank != g_node_rank) {
                                    localBorderMap[nodeRank].addObjectR(*obj); // Thread-local, no contention
                                } else {
                                    // no distribution needed, evaluate locally against other local partitions
                                    PartitionBase* partitionS = S->index->getPartition(overlapPartitionID);
                                    if (partitionS != nullptr) {
                                        std::vector<Shape*>* objectsS = partitionS->getContents();
                                        if (objectsS != nullptr) {
                                            for (auto &objectS : *objectsS) {
                                                if (obj->distance(*objectS) <= distanceJoinQuery->getDistanceValue()) {
                                                    queryResult->addResult(obj->recID, objectS->recID);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Parallel distance join failed.");
                return ret;
            }

            // Merge thread-local border maps into the global one (serial section)
            for (auto& localMap : threadLocalBorderMaps) {
                for (auto& [rank, batch] : localMap) {
                    borderObjectsMap[rank].objectsR.insert(batch.objectsR.begin(), batch.objectsR.end());
                    borderObjectsMap[rank].objectsS.insert(batch.objectsS.begin(), batch.objectsS.end());
                }
            }

            // Thread-local storage for S border objects (one per thread)
            std::vector<std::unordered_map<int, DJBatch>> threadLocalBorderMapsS(MAX_THREADS);
            #pragma omp parallel num_threads(MAX_THREADS)
            {
                int tid = omp_get_thread_num();
                auto& localBorderMap = threadLocalBorderMaps[tid];
                std::vector<PartitionBase *>* partitions = S->index->getPartitions();
                DB_STATUS local_ret = DBERR_OK;
                int overlapPartitionID; 
                int nodeRank;
                #pragma omp for
                for (int i = 0; i < partitions->size(); i++) {
                    PartitionBase* partitionS = partitions->at(i);
                    int iFine = partitionS->partitionID % g_config.partitioningMethod->getGlobalPPD(); 
                    int jFine = partitionS->partitionID / g_config.partitioningMethod->getGlobalPPD(); 

                    std::vector<Shape*>* objectsS = partitionS->getContents();
                    if (objectsS != nullptr) {
                        for (auto &obj : *objectsS) {
                            std::vector<std::pair<int,int>> overlappingPartitionOffsets = obj->getOverlappingPartitionOffsets(
                                iFine, jFine, distanceJoinQuery->getDistanceValue(),
                                g_config.partitioningMethod->getPartPartionExtentX(),
                                g_config.partitioningMethod->getPartPartionExtentY(),
                                g_config.datasetOptions.dataspaceMetadata.xMinGlobal,
                                g_config.datasetOptions.dataspaceMetadata.yMinGlobal,
                                g_config.partitioningMethod->getGlobalPPD());

                            for (auto &offset : overlappingPartitionOffsets) {                                
                                local_ret = getPartitionIDandNodeRankForOffset(iFine, jFine, offset, overlapPartitionID, nodeRank);
                                if (local_ret != DBERR_OK) {
                                    #pragma omp cancel for
                                    ret = local_ret;
                                }
                                
                                if (nodeRank != g_node_rank) {
                                    localBorderMap[nodeRank].addObjectS(*obj); // Thread-local, no contention
                                }
                            }
                        }
                    }
                }
            }
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Parallel distance join failed.");
            }

            // Merge thread-local S border maps into the global one (serial section)
            for (auto& localMap : threadLocalBorderMaps) {
                for (auto& [rank, batch] : localMap) {
                    borderObjectsMap[rank].objectsR.insert(batch.objectsR.begin(), batch.objectsR.end());
                    borderObjectsMap[rank].objectsS.insert(batch.objectsS.begin(), batch.objectsS.end());
                }
            }

            return ret;
        }

        DB_STATUS evaluateDJBatch(hec::DistanceJoinQuery *distanceJoinQuery, DJBatch& batch, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetOptions.getDatasetByIdx(distanceJoinQuery->getDatasetRid());
            Dataset* S = g_config.datasetOptions.getDatasetByIdx(distanceJoinQuery->getDatasetSid());
            int overlapPartitionID; 
            int nodeRank;
            // R objects in batch
            /** @todo: parallelize */
            for (auto& objectR: batch.objectsR) {
                int iFine = std::floor((objectR.second.mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
                int jFine = std::floor((objectR.second.mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
                
                std::vector<std::pair<int,int>> overlappingPartitionOffsets = objectR.second.getOverlappingPartitionOffsets(
                    iFine, jFine, distanceJoinQuery->getDistanceValue(),
                    g_config.partitioningMethod->getPartPartionExtentX(),
                    g_config.partitioningMethod->getPartPartionExtentY(),
                    g_config.datasetOptions.dataspaceMetadata.xMinGlobal,
                    g_config.datasetOptions.dataspaceMetadata.yMinGlobal,
                    g_config.partitioningMethod->getGlobalPPD());

                for (auto &offset : overlappingPartitionOffsets) {                    
                    ret = getPartitionIDandNodeRankForOffset(iFine, jFine, offset, overlapPartitionID, nodeRank);
                    if (ret != DBERR_OK) {
                        return ret;
                    }

                    if (nodeRank == g_node_rank) {
                        // no distribution needed, evaluate locally against other local partitions
                        PartitionBase* partitionS = S->index->getPartition(overlapPartitionID);
                        if (partitionS != nullptr) {
                            std::vector<Shape*>* objectsS = partitionS->getContents();
                            if (objectsS != nullptr) {
                                for (auto &objectS : *objectsS) {
                                    if (objectR.second.distance(*objectS) <= distanceJoinQuery->getDistanceValue()) {
                                        queryResult->addResult(objectR.second.recID, objectS->recID);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // S objects in batch
            for (auto& objectS: batch.objectsS) {
                int iFine = std::floor((objectS.second.mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
                int jFine = std::floor((objectS.second.mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
                
                std::vector<std::pair<int,int>> overlappingPartitionOffsets = objectS.second.getOverlappingPartitionOffsets(
                    iFine, jFine, distanceJoinQuery->getDistanceValue(),
                    g_config.partitioningMethod->getPartPartionExtentX(),
                    g_config.partitioningMethod->getPartPartionExtentY(),
                    g_config.datasetOptions.dataspaceMetadata.xMinGlobal,
                    g_config.datasetOptions.dataspaceMetadata.yMinGlobal,
                    g_config.partitioningMethod->getGlobalPPD());

                
                for (auto &offset : overlappingPartitionOffsets) {
                    ret = getPartitionIDandNodeRankForOffset(iFine, jFine, offset, overlapPartitionID, nodeRank);
                    if (ret != DBERR_OK) {
                        return ret;
                    }

                    if (nodeRank == g_node_rank) {
                        // no distribution needed, evaluate locally against other local partitions
                        PartitionBase* partitionR = R->index->getPartition(overlapPartitionID);
                        if (partitionR != nullptr) {
                            std::vector<Shape*>* objectsR = partitionR->getContents();
                            if (objectsR != nullptr) {
                                for (auto &objectR : *objectsR) {
                                    if (objectR->distance(objectS.second) <= distanceJoinQuery->getDistanceValue()) {
                                        queryResult->addResult(objectR->recID, objectS.second.recID);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            return ret;
        }
    }
}