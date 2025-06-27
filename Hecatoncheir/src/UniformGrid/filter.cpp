#include "UniformGrid/filter.h"
#include "refinement/topology.h"
#include "env/partitioning.h"

namespace uniform_grid
{
    namespace knn_filter
    {        
        static DB_STATUS evaluate(hec::KNNQuery *knnQuery, std::unique_ptr<hec::QResultBase>& queryResult) {
            Shape qPoint;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(knnQuery->getDatasetID());
            // create query shape
            DB_STATUS ret = shape_factory::createEmpty(DT_POINT, qPoint);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't create shape object from knn query.");
                return DBERR_INVALID_GEOMETRY;
            }
            ret = qPoint.setFromWKT(knnQuery->getWKT());
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't set knn query shape from query WKT. WKT:", knnQuery->getWKT());
                return DBERR_INVALID_GEOMETRY;
            }

            std::vector<PartitionBase*>* partitions = dataset->index->getPartitions();
            for (auto &partition : *partitions) {
                if (partition == nullptr) {
                    continue;
                }
                /** CHECK PARTITION OPTIMIZATION */
                int iFine = partition->partitionID % g_config.partitioningMethod->getGlobalPPD(); 
                int jFine = partition->partitionID / g_config.partitioningMethod->getGlobalPPD(); 
                double xFineStart = g_config.datasetOptions.dataspaceMetadata.xMinGlobal + iFine * g_config.partitioningMethod->getPartPartionExtentX();
                double yFineStart = g_config.datasetOptions.dataspaceMetadata.yMinGlobal + jFine * g_config.partitioningMethod->getPartPartionExtentY();
                double xFineEnd = xFineStart + g_config.partitioningMethod->getDistPartionExtentX();
                double yFineEnd = yFineStart + g_config.partitioningMethod->getDistPartionExtentY();                
                // calculate query point distance to fine partition
                double distanceToPartition = qPoint.distanceToPartition(xFineStart, yFineStart, xFineEnd, yFineEnd);
                // if the distance to partition is larger than the current max in heap, skip this partition entirely
                if (!queryResult->checkDistance(distanceToPartition)) {
                    continue;
                }

                // get partition contents
                std::vector<Shape *>* contents = partition->getContents();
                if (contents == nullptr) {
                    continue;
                }
                // loop contents
                for (auto &obj: *contents) {
                    double distance = obj->distance(qPoint);
                    // add result (the heap handles insertions automatically)
                    queryResult->addResult(obj->recID, distance);
                }
            }

            return ret;
        }
    }

    /** @brief Should only be used for points, as non-point geometries are better benefited from the two layer indexing */
    namespace mbr_range_query_filter
    {
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            // forward to refinement
            ret = refinement::relate::refinementEntrypoint(objR, objS, g_config.queryPipeline.queryType, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Refinement failed.");
                return ret;
            }
            return ret;
        }

        /** @brief Evaluates the contents of the given partition against the provided window. All objects are geometrically refined, 
         * thus, this method should be used only for partitions that are partially covered by the window in both axes.
         */
        static DB_STATUS comparePartialPartition(Shape &window, PartitionBase* partition, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            if (partition == nullptr) {
                // not assigned partition
                return ret;
            }
            // get partition contents
            std::vector<Shape *>* contents = partition->getContents();
            if (contents == nullptr) {
                // empty partition
                return ret;
            }
            // loop contents
            for (auto &it: *contents) {
                // all contents need to be refined
                ret = forwardPair(&window, it, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Evaluation failed for window and object with id", it->recID);
                    return ret;
                }
            }
            return ret;
        }

        static DB_STATUS evaluateBoxQuery(hec::RangeQuery *rangeQuery, hec::QResultBase* queryResult) {
            Shape window;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(rangeQuery->getDatasetID());
            // create query shape
            DB_STATUS ret = shape_factory::createEmpty(DT_BOX, window);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't create shape object from query window.");
                return DBERR_INVALID_GEOMETRY;
            }
            ret = window.setFromWKT(rangeQuery->getWKT());
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't set window shape from query WKT. WKT:", rangeQuery->getWKT());
                return DBERR_INVALID_GEOMETRY;
            }
            window.setMBR();
            
            // get cells range
            int partitionMinX = std::floor((window.mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
            int partitionMinY = std::floor((window.mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
            int partitionMaxX = std::floor((window.mbr.pMax.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
            int partitionMaxY = std::floor((window.mbr.pMax.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
            
            int partitionID = -1;
            PartitionBase* partition = nullptr;
            
            if (partitionMinX == partitionMaxX && partitionMinY == partitionMaxY) {
                // single partition
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                ret = comparePartialPartition(window, partition, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Query evaluation failed for single partition query. Partition ID:", partitionID);
                    return ret;
                }
            } else if (partitionMinY == partitionMaxY) {
                // single row of partitions
                for (int i=partitionMinX; i<=partitionMaxX; i++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(i, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    ret = comparePartialPartition(window, partition, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Query evaluation failed for single row of partitions query. Partition ID:", partitionID);
                        return ret;
                    }
                }
            } else if (partitionMinX == partitionMaxX) {
                // single column of partitions
                for (int j=partitionMinY; j<=partitionMaxY; j++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    ret = comparePartialPartition(window, partition, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Query evaluation failed for single column of partitions query. Partition ID:", partitionID);
                        return ret;
                    }
                }
            } else {
                // block of partitions
                // bottom row 
                for (int i=partitionMinX; i<=partitionMaxX; i++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(i, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    ret = comparePartialPartition(window, partition, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Query evaluation failed for block of partitions query. Bottom row failed, Partition ID:", partitionID);
                        return ret;
                    }
                }
                // left-most column (skip bottom left corner cell)
                for (int j=partitionMinY+1; j<=partitionMaxY; j++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    ret = comparePartialPartition(window, partition, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Query evaluation failed for block of partitions query. left-most column failed, Partition ID:", partitionID);
                        return ret;
                    }
                }
                // top row (skip top left corner cell)
                for (int i=partitionMinX+1; i<=partitionMaxX; i++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(i, partitionMaxY, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    ret = comparePartialPartition(window, partition, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Query evaluation failed for block of partitions query. top row failed, Partition ID:", partitionID);
                        return ret;
                    }
                }
                // right-most column (skip top right corner cell)
                for (int j=partitionMinY+1; j<partitionMaxY; j++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(partitionMaxX, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    ret = comparePartialPartition(window, partition, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Query evaluation failed for block of partitions query. right-most column failed, Partition ID:", partitionID);
                        return ret;
                    }
                }
                // keep all the rest of the partitions' contents directly
                for (int i=partitionMinX+1; i<partitionMaxX; i++) {
                    for (int j=partitionMinY+1; j<partitionMaxY; j++) {
                        partitionID = g_config.partitioningMethod->getPartitionID(partitionMaxX, j, g_config.partitioningMethod->getGlobalPPD());
                        partition = dataset->index->getPartition(partitionID);
                        if (partition == nullptr) {
                            // not assigned partition
                            return ret;
                        }
                        // get partition contents
                        std::vector<Shape *>* contents = partition->getContents();
                        if (contents == nullptr) {
                            // empty partition
                            return ret;
                        }
                        // loop contents
                        for (auto &it: *contents) {
                            // keep contents directly
                            queryResult->addResult(it->recID);
                        }
                    }     
                }
            }
            return ret;
        }

        /** @brief evaluates polygonal queries (needs refinement for all candidates) */
        static DB_STATUS evaluatePolygonQuery(hec::RangeQuery *rangeQuery, hec::QResultBase* queryResult) {
            Shape window;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(rangeQuery->getDatasetID());
            // create query shape
            DB_STATUS ret = shape_factory::createEmpty(DT_POLYGON, window);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't create shape object from query window.");
                return DBERR_INVALID_GEOMETRY;
            }
            ret = window.setFromWKT(rangeQuery->getWKT());
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_GEOMETRY, "Couldn't set window shape from query WKT. WKT:", rangeQuery->getWKT());
                return DBERR_INVALID_GEOMETRY;
            }
            window.setMBR();
            
            // get cells range
            int partitionMinX = std::floor((window.mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
            int partitionMinY = std::floor((window.mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
            int partitionMaxX = std::floor((window.mbr.pMax.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
            int partitionMaxY = std::floor((window.mbr.pMax.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
            
            // loop partitions
            int partitionID = -1;
            PartitionBase* partition = nullptr;
            for (int i=partitionMinX; i<=partitionMaxX; i++) {
                for (int j=partitionMinY; j<=partitionMaxY; j++) {
                    // get partition
                    partitionID = g_config.partitioningMethod->getPartitionID(i, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition == nullptr) {
                        continue;
                    }
                    
                    // get partition contents
                    std::vector<Shape *>* contents = partition->getContents();
                    if (contents == nullptr) {
                        continue;
                    }

                    // loop contents
                    for (auto &it: *contents) {
                        // contents->at(i)->printGeometry();
                        ret = forwardPair(&window, it, queryResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Evaluation failed for window and object with id", it->recID);
                            return ret;
                        }
                    }
                }
            }

            return ret;
        }

        // @todo: different evaluation for box or polygon
        static DB_STATUS evaluate(hec::RangeQuery *rangeQuery, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(rangeQuery->getDatasetID());
            // create shape object for window
            Shape window;
            if (rangeQuery->getWKT().find("POLYGON") != std::string::npos) {
                // polygon query
                ret = evaluatePolygonQuery(rangeQuery, queryResult.get());
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Polygon query evaluation failed.");
                    return ret;
                }
            } else if (rangeQuery->getWKT().find("BOX") != std::string::npos) {
                // box query
                ret = evaluateBoxQuery(rangeQuery, queryResult.get());
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Box query evaluation failed.");
                    return ret;
                }
            } else {
                // invalid query type
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type. Only BOX and POLYGON allowed. WKT:", rangeQuery->getWKT());
                return DBERR_QUERY_INVALID_TYPE;
            }

            return ret;
        }
    } // range query mbr filter

    namespace distance_filter
    {

        static DB_STATUS evaluate(hec::DistanceJoinQuery *distanceJoinQuery, std::unordered_map<int, DJBatch>& borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult) {
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
                                g_config.datasetOptions.dataspaceMetadata.yMinGlobal);

                            for (auto &offset : overlappingPartitionOffsets) {
                                int overlapPartitionID = g_config.partitioningMethod->getPartitionID(
                                    iFine + offset.first, jFine + offset.second, 
                                    g_config.partitioningMethod->getGlobalPPD());

                                int oFineI = overlapPartitionID % g_config.partitioningMethod->getGlobalPPD(); 
                                int oFineJ = overlapPartitionID / g_config.partitioningMethod->getGlobalPPD(); 

                                int oCoarseI = oFineI / g_config.partitioningMethod->getPartitioningPPD();
                                int oCoarseJ = oFineJ / g_config.partitioningMethod->getPartitioningPPD();
                                int coarsePartitionID = g_config.partitioningMethod->getPartitionID(
                                    oCoarseI, oCoarseJ, g_config.partitioningMethod->getDistributionPPD());
                                int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(coarsePartitionID);
                                
                                if (nodeRank != g_parent_original_rank) {
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
                                g_config.datasetOptions.dataspaceMetadata.yMinGlobal);

                            for (auto &offset : overlappingPartitionOffsets) {
                                int overlapPartitionID = g_config.partitioningMethod->getPartitionID(
                                    iFine + offset.first, jFine + offset.second, 
                                    g_config.partitioningMethod->getGlobalPPD());

                                int oFineI = overlapPartitionID % g_config.partitioningMethod->getGlobalPPD(); 
                                int oFineJ = overlapPartitionID / g_config.partitioningMethod->getGlobalPPD(); 

                                int oCoarseI = oFineI / g_config.partitioningMethod->getPartitioningPPD();
                                int oCoarseJ = oFineJ / g_config.partitioningMethod->getPartitioningPPD();
                                int coarsePartitionID = g_config.partitioningMethod->getPartitionID(
                                    oCoarseI, oCoarseJ, g_config.partitioningMethod->getDistributionPPD());
                                int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(coarsePartitionID);
                                
                                if (nodeRank != g_parent_original_rank) {
                                    localBorderMap[nodeRank].addObjectS(*obj); // Thread-local, no contention
                                }
                            }
                        }
                    }
                }
            }

            // Merge thread-local S border maps into the global one (serial section)
            for (auto& localMap : threadLocalBorderMaps) {
                for (auto& [rank, batch] : localMap) {
                    borderObjectsMap[rank].objectsR.insert(batch.objectsR.begin(), batch.objectsR.end());
                    borderObjectsMap[rank].objectsS.insert(batch.objectsS.begin(), batch.objectsS.end());
                }
            }

            if (ret != DBERR_OK) {
                logger::log_error(ret, "Parallel distance join failed.");
            }
            return ret;
        }

        DB_STATUS evaluateDJBatch(hec::DistanceJoinQuery *distanceJoinQuery, DJBatch& batch, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetOptions.getDatasetByIdx(distanceJoinQuery->getDatasetRid());
            Dataset* S = g_config.datasetOptions.getDatasetByIdx(distanceJoinQuery->getDatasetSid());

            // R objects in batch
            for (auto& objectR: batch.objectsR) {
                int iFine = std::floor((objectR.second.mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
                int jFine = std::floor((objectR.second.mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
                
                std::vector<std::pair<int,int>> overlappingPartitionOffsets = objectR.second.getOverlappingPartitionOffsets(
                    iFine, jFine, distanceJoinQuery->getDistanceValue(),
                    g_config.partitioningMethod->getPartPartionExtentX(),
                    g_config.partitioningMethod->getPartPartionExtentY(),
                    g_config.datasetOptions.dataspaceMetadata.xMinGlobal,
                    g_config.datasetOptions.dataspaceMetadata.yMinGlobal);

                for (auto &offset : overlappingPartitionOffsets) {
                    int overlapPartitionID = g_config.partitioningMethod->getPartitionID(
                        iFine + offset.first, jFine + offset.second, 
                        g_config.partitioningMethod->getGlobalPPD());

                    int oFineI = overlapPartitionID % g_config.partitioningMethod->getGlobalPPD(); 
                    int oFineJ = overlapPartitionID / g_config.partitioningMethod->getGlobalPPD(); 

                    int oCoarseI = oFineI / g_config.partitioningMethod->getPartitioningPPD();
                    int oCoarseJ = oFineJ / g_config.partitioningMethod->getPartitioningPPD();
                    int coarsePartitionID = g_config.partitioningMethod->getPartitionID(
                        oCoarseI, oCoarseJ, g_config.partitioningMethod->getDistributionPPD());
                    int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(coarsePartitionID);

                    if (nodeRank == g_parent_original_rank) {
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
                    g_config.datasetOptions.dataspaceMetadata.yMinGlobal);

                for (auto &offset : overlappingPartitionOffsets) {
                    int overlapPartitionID = g_config.partitioningMethod->getPartitionID(
                        iFine + offset.first, jFine + offset.second, 
                        g_config.partitioningMethod->getGlobalPPD());

                    int oFineI = overlapPartitionID % g_config.partitioningMethod->getGlobalPPD(); 
                    int oFineJ = overlapPartitionID / g_config.partitioningMethod->getGlobalPPD(); 

                    int oCoarseI = oFineI / g_config.partitioningMethod->getPartitioningPPD();
                    int oCoarseJ = oFineJ / g_config.partitioningMethod->getPartitioningPPD();
                    int coarsePartitionID = g_config.partitioningMethod->getPartitionID(
                        oCoarseI, oCoarseJ, g_config.partitioningMethod->getDistributionPPD());
                    int nodeRank = g_config.partitioningMethod->getNodeRankForPartitionID(coarsePartitionID);

                    if (nodeRank == g_parent_original_rank) {
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

        DB_STATUS processQuery(hec::Query* query, std::unordered_map<int, DJBatch>& borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            // set to global config
            g_config.queryPipeline.queryType = (hec::QueryType) query->getQueryType();

            // switch based on query type
            switch (query->getQueryType()) {
                case hec::Q_DISTANCE_JOIN:
                    {
                        // cast
                        hec::DistanceJoinQuery* distanceQuery = dynamic_cast<hec::DistanceJoinQuery*>(query);
                        // evaluate
                        ret = distance_filter::evaluate(distanceQuery, borderObjectsMap, queryResult);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                    break;
                default:
                    logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type must be distance join for this version of uniform index query processing. Type:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                    return DBERR_FEATURE_UNSUPPORTED;
            }

            return ret;
        }

    }


    DB_STATUS processQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult) {
        DB_STATUS ret = DBERR_OK;
        // set to global config
        g_config.queryPipeline.queryType = (hec::QueryType) query->getQueryType();

        // switch based on query type
        switch (query->getQueryType()) {
            case hec::Q_RANGE:
                {
                    // cast
                    hec::RangeQuery* rangeQuery = dynamic_cast<hec::RangeQuery*>(query);
                    // evaluate
                    ret = mbr_range_query_filter::evaluate(rangeQuery, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                break;
            case hec::Q_KNN:
                {
                    // cast
                    hec::KNNQuery* kNNQuery = dynamic_cast<hec::KNNQuery*>(query);
                    // evaluate
                    ret = knn_filter::evaluate(kNNQuery, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported for Uniform Grid index:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }

    
}