#include "env/comm_common.h"

namespace comm
{

    DB_STATUS probe(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
        // blocking, won't stop probing until a message with this specification is found
        int mpi_ret = MPI_Probe(sourceRank, tag, comm, &status);
        if(mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_COMM_PROBE_FAILED, "Blocking probe failed");
            return DBERR_COMM_PROBE_FAILED;
        }
        return DBERR_OK;
    }

    DB_STATUS probe(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status, int &messageExists) {
        // non-blocking, will return if no message with this specification is found at this moment
        int mpi_ret = MPI_Iprobe(sourceRank, tag, comm, &messageExists, &status);
        if(mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_COMM_PROBE_FAILED, "Non Blocking probe failed");
            return DBERR_COMM_PROBE_FAILED;
        }
        return DBERR_OK;
    }

    namespace execute
    {
        DB_STATUS prepareDataset(SerializedMsg<char> &msg, int &datasetID) {
            DB_STATUS ret = DBERR_OK;
            // deserialize message
            DatasetMetadata datasetMetadata;
            datasetMetadata.deserialize(msg.data, msg.count);
            // create dataset
            Dataset dataset(datasetMetadata);
    
            // generate the partition data filepath 
            ret = storage::generatePartitionFilePath(&dataset);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed creating the dataset from the dataset metadata message");
                return ret;
            }
    
            // generate approximation filepaths
            ret = storage::generateAPRILFilePath(&dataset);
            if (ret != DBERR_OK) {
                return ret;
            }
    
            // add the dataset structure to the config
            ret = g_config.datasetOptions.addDataset(std::move(dataset), datasetID);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to add dataset to config options.");
                return ret;
            }


            return ret;
        }
    
        DB_STATUS buildIndex(SerializedMsg<char> &msg) {
            // unpack
            std::vector<int> messageContents;
            DB_STATUS ret = unpack::unpackValues(msg, messageContents);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack build index contents.");
                return ret;
            }

            // logger::log_success("Global dataspace:", g_config.datasetOptions.dataspaceMetadata.xMinGlobal, g_config.datasetOptions.dataspaceMetadata.yMinGlobal, g_config.datasetOptions.dataspaceMetadata.xMaxGlobal, g_config.datasetOptions.dataspaceMetadata.yMaxGlobal);
            // printf("Partitioning metadata set: (%f,%f),(%f,%f)\n", g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal, g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal, g_config.partitioningMethod->distGridDataspaceMetadata.xMaxGlobal, g_config.partitioningMethod->distGridDataspaceMetadata.yMaxGlobal);
            // printf("Partitioning metadata set: gPPD %d, dPPD %d, pPPD %d\n", g_config.partitioningMethod->getGlobalPPD(), g_config.partitioningMethod->getDistributionPPD(), g_config.partitioningMethod->getPartitioningPPD());
            // printf("distExtents %f,%f, partitionExtents %f,%f\n", g_config.partitioningMethod->getDistPartionExtentX(), g_config.partitioningMethod->getDistPartionExtentY(), g_config.partitioningMethod->getPartPartionExtentX(), g_config.partitioningMethod->getPartPartionExtentY());

            // first number is the build index
            hec::IndexType indexType = (hec::IndexType) messageContents[0];
            /** @todo parallelize for two datasets? */
            for (int i=1; i<messageContents.size(); i++) {
                // get dataset to index
                Dataset *dataset = g_config.datasetOptions.getDatasetByIdx(messageContents[i]);
                // build index
                ret = dataset->buildIndex(indexType);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to build index");
                    return ret;
                }
                if (g_config.queryPipeline.IntermediateFilter) {
                    // generate APRIL
                    ret = dataset->buildAPRIL();
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to create APRIL");
                        return ret;
                    }
                }
            }

            return ret;
        }

        DB_STATUS evaluateDJQuery(SerializedMsg<char> &msg, hec::Query** queryPtr, std::unordered_map<int, DJBatch> &borderObjectsMap, std::unique_ptr<hec::QResultBase> &queryResult) {
            DB_STATUS ret = DBERR_OK;
            // unpack query
            char* localBuffer = msg.data;
            *queryPtr = hec::Query::createFromBuffer(localBuffer);
            if (*queryPtr == nullptr) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Failed to create query from message buffer.");
                return DBERR_NULL_PTR_EXCEPTION;
            }

            // setup query result object
            int res = qresult_factory::createNew(*queryPtr, queryResult);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                return DBERR_OBJ_CREATION_FAILED;
            }

            // batches of objects that rest on border areas
            for (int i=1; i<g_world_size; i++) {
                borderObjectsMap[i] = DJBatch(i, g_config.datasetOptions.getDatasetR()->metadata.dataType, g_config.datasetOptions.getDatasetS()->metadata.dataType);
            }
            // evaluate local distances and find the border objects
            ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(*queryPtr, borderObjectsMap, queryResult);
            if (ret != DBERR_OK) {
                return ret;
            }

            return ret;
        }

        DB_STATUS evaluateQuery(SerializedMsg<char> &msg, std::unique_ptr<hec::QResultBase> &queryResult) {
            DB_STATUS ret = DBERR_OK;

            // unpack query
            char* localBuffer = msg.data;
            hec::Query* queryPtr = hec::Query::createFromBuffer(localBuffer);
            if (queryPtr == nullptr) {
                logger::log_error(ret, "Failed to create query from message buffer.");
                return ret;
            }

            // setup query result object
            int res = qresult_factory::createNew(queryPtr, queryResult);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                return DBERR_OBJ_CREATION_FAILED;
            }

            // evaluate query based on index (stored in R dataset)
            ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(queryPtr, queryResult);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free query memory
            delete queryPtr;

            return ret;
        }

        DB_STATUS batchRangeQueries(SerializedMsg<char> &msg, std::unordered_map<int, std::unique_ptr<hec::QResultBase>> &batchResults) {
            // unpack query batch
            std::vector<hec::Query*> queryBatch;
            DB_STATUS ret = unpack::unpackQueryBatch(msg, &queryBatch);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack query.");
                return ret;
            }
            
            // holds all the final batch results
            std::vector<std::unordered_map<int, std::unique_ptr<hec::QResultBase>>> threadLocalMaps(MAX_THREADS);

            #pragma omp parallel num_threads(MAX_THREADS)
            {
                int tid = omp_get_thread_num();
                auto& localResults = threadLocalMaps[tid];
                #pragma omp for
                for (int i = 0; i < queryBatch.size(); i++) {
                    std::unique_ptr<hec::QResultBase> queryResult;
                    int res = qresult_factory::createNew(queryBatch[i], queryResult);
                    if (res != 0) {
                        logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                        #pragma omp cancel for
                        ret = DBERR_OBJ_CREATION_FAILED;
                        continue;
                    }

                    DB_STATUS local_ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(queryBatch[i], queryResult);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel for
                        ret = local_ret;
                    }

                    int queryID = queryBatch[i]->getQueryID();
                    localResults[queryID] = std::move(queryResult);
                }
            }
            if (ret != DBERR_OK) {
                return ret;
            }
            // After parallel region, merge maps
            for (auto& localMap : threadLocalMaps) {
                for (auto& [qid, resultPtr] : localMap) {
                    batchResults[qid] = std::move(resultPtr);
                }
            }

            return ret;
        }

        DB_STATUS batchRangeQueries(std::vector<hec::Query*> &queryBatch, std::unordered_map<int, std::unique_ptr<hec::QResultBase>> &batchResults) {
            DB_STATUS ret = DBERR_OK;
            // holds all the final batch results
            std::vector<std::unordered_map<int, std::unique_ptr<hec::QResultBase>>> threadLocalMaps(MAX_THREADS);

            #pragma omp parallel num_threads(MAX_THREADS)
            {
                int tid = omp_get_thread_num();
                auto& localResults = threadLocalMaps[tid];
                #pragma omp for
                for (int i = 0; i < queryBatch.size(); i++) {
                    std::unique_ptr<hec::QResultBase> queryResult;
                    int res = qresult_factory::createNew(queryBatch[i], queryResult);
                    if (res != 0) {
                        logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                        #pragma omp cancel for
                        ret = DBERR_OBJ_CREATION_FAILED;
                        continue;
                    }

                    DB_STATUS local_ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(queryBatch[i], queryResult);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel for
                        ret = local_ret;
                    }

                    int queryID = queryBatch[i]->getQueryID();
                    localResults[queryID] = std::move(queryResult);
                }
            }
            if (ret != DBERR_OK) {
                return ret;
            }
            // After parallel region, merge maps
            for (auto& localMap : threadLocalMaps) {
                for (auto& [qid, resultPtr] : localMap) {
                    batchResults[qid] = std::move(resultPtr);
                }
            }

            return ret;
        }

    }

}
