#include "UniformGrid/filter.h"
#include "refinement/topology.h"

namespace uniform_grid
{
    /** Only used for points, as non-point geometries are better benefited from the two layer indexing */
    namespace mbr_range_query_filter
    {
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            // forward to refinement
            ret = refinement::relate::refinementEntrypoint(objR, objS, g_config.queryMetadata.queryType, queryResult);
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
        static DB_STATUS evaluate(hec::RangeQuery *rangeQuery, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(rangeQuery->getDatasetID());
            // create shape object for window
            Shape window;
            if (rangeQuery->getWKT().find("POLYGON") != std::string::npos) {
                // polygon query
                ret = evaluatePolygonQuery(rangeQuery, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_INVALID_GEOMETRY, "Polygon query evaluation failed.");
                    return DBERR_INVALID_GEOMETRY;
                }
            } else if (rangeQuery->getWKT().find("BOX") != std::string::npos) {
                // box query
                ret = evaluateBoxQuery(rangeQuery, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_INVALID_GEOMETRY, "Box query evaluation failed.");
                    return DBERR_INVALID_GEOMETRY;
                }
            } else {
                // invalid query type
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type. Only BOX and POLYGON allowed. WKT:", rangeQuery->getWKT());
                return DBERR_QUERY_INVALID_TYPE;
            }

            return ret;
        }
    } // range query mbr filter




















    DB_STATUS processQuery(hec::Query* query, hec::QResultBase* queryResult) {
        DB_STATUS ret = DBERR_OK;
        // set to global config
        g_config.queryMetadata.queryType = (hec::QueryType) query->getQueryType();

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
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported for Uniform Grid index:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }
}