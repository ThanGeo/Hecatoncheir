#include "env/comm_common.h"
#include "env/comm_host.h"

namespace comm
{
    /**
     * Host controller only
     */
    namespace host
    {

        namespace forward 
        {
            /** @brief received an already probed message from the driver and forwards it to the other workers */
            static DB_STATUS forwardInstructionMessage(int sourceTag) {
                MPI_Status status;
                // receive the msg
                DB_STATUS ret = recv::receiveInstructionMessage(DRIVER_GLOBAL_RANK, sourceTag, g_global_intra_comm, status);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_RECV, "Failed receiving instruction from driver");
                    return DBERR_COMM_RECV;
                }
                
                // send instruction to controllers
                #pragma omp parallel num_threads(MAX_THREADS)
                {
                    DB_STATUS local_ret = DBERR_OK;
                    #pragma omp for
                    for (int i = 1; i < g_world_size; i++) {
                        local_ret = send::sendInstructionMessage(i, status.MPI_TAG, g_worker_comm);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            logger::log_error(DBERR_COMM_SEND, "Failed forwarding instruction to worker", i);
                            ret = local_ret;
                        }
                    }
                }
                return ret;
            }
        }

        DB_STATUS gatherResponses() {
            DB_STATUS ret = DBERR_OK;
            // use threads to parallelize gathering of responses
            #pragma omp parallel num_threads(MAX_THREADS)
            {
                DB_STATUS local_ret = DBERR_OK;
                MPI_Status status;
                #pragma omp for
                for (int i=1; i<g_world_size; i++) {
                    // probe for responses
                    local_ret = probe(i, MPI_ANY_TAG, g_worker_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel for
                        ret = local_ret;
                    }
                    // receive response
                    local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_worker_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel for
                        ret = local_ret;
                    }
                    // check response
                    if (status.MPI_TAG != MSG_ACK) {
                        #pragma omp cancel for
                        logger::log_error(DBERR_OPERATION_FAILED, "Worker", status.MPI_SOURCE, "finished with error");
                        ret = DBERR_OPERATION_FAILED;
                    }
                }
            }

            return ret;
        }

        static DB_STATUS handlePrepareDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // broadcast it to all workers
            ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
            if (ret != DBERR_OK) {
                return ret;
            }

            // perform the preparation
            int id = -1;
            ret = comm::execute::prepareDataset(msg, id);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free memory
            msg.clear();

            // return dataset ID
            SerializedMsg<char> msgToDriver(MPI_CHAR);
            // serialize value
            ret = pack::packValues(msgToDriver, id);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed packing indexes to message.");
                return ret;
            }

            // send the dataset ID to the driver
            ret = comm::send::sendMessage(msgToDriver, DRIVER_GLOBAL_RANK, MSG_DATASET_INDEX, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending indexes message to driver.");
                return ret;
            }

            // free memory
            msgToDriver.clear();
            return ret;
        }

        static DB_STATUS handleUnloadDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // broadcast message
            ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
            if (ret != DBERR_OK) {
                return ret;
            }

            // unpack
            std::vector<int> messageContents;
            ret = unpack::unpackValues(msg, messageContents);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack build index contents.");
                return ret;
            }
            
            // free message memory
            msg.clear();

            // unload datasets
            for (auto &datasetID : messageContents) {
                ret = g_config.datasetOptions.unloadDataset(datasetID);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }

            // wait for ACK from everyone
            ret = gatherResponses();
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Not all nodes finished successfully.");
                return ret;
            }
            
            // send ACK to the driver
            ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending ACK message to driver.");
                return ret;
            }

            return ret;
        }

        static DB_STATUS handlePartitionDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // unpack the indexes of the datasets to partition
            std::vector<int> datasetIndexes;
            ret = unpack::unpackValues(msg, datasetIndexes);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack dataset indexes.");
                return ret;
            }

            // free memory
            msg.clear();

            // double check if bounds are set for all datasets
            for (auto &index: datasetIndexes) {
                Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(index);
                if (dataset == nullptr) {
                    logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Get dataset by index returned nullptr at partitioning. Index:", index);
                    return DBERR_NULL_PTR_EXCEPTION;
                }
                if (!dataset->metadata.dataspaceMetadata.boundsSet) {
                    // calculate all metadata for dataset
                    ret = storage::reader::calculateDatasetMetadata(g_config.datasetOptions.getDatasetByIdx(index));
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // only count lines
                    size_t totalObjects = 0;
                    ret = storage::reader::getDatasetLineCountWithSystemCall(dataset, totalObjects);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    dataset->totalObjects = totalObjects;
                }
            }
            // update dataspace
            g_config.datasetOptions.updateDataspace();
            g_config.datasetOptions.dataspaceMetadata.boundsSet = true;

            // update the grids' dataspace metadata in the partitioning object 
            g_config.partitioningMethod->setDistGridDataspace(g_config.datasetOptions.dataspaceMetadata);
            g_config.partitioningMethod->setPartGridDataspace(g_config.datasetOptions.dataspaceMetadata);

            // perform the partitioning for each dataset in the message
            for (auto &index : datasetIndexes) {
                // get dataset to partition
                Dataset* dataset = g_config.datasetOptions.getDatasetByIdx((DatasetIndex) index);
                // serialize dataset metadata to send
                SerializedMsg<char> datasetInfoMsg(MPI_CHAR);
                ret = dataset->serialize(&datasetInfoMsg.data, datasetInfoMsg.count);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Dataset info serialization failed.");
                    return ret;
                }
                // broadcast dataset info to workers + agent
                ret = broadcast::broadcastMessage(datasetInfoMsg, MSG_DATASET_METADATA);
                if (ret != DBERR_OK) {
                    return ret;
                }

                // free message
                datasetInfoMsg.clear();

                // signal the begining of the partitioning
                SerializedMsg<char> signal(MPI_CHAR);
                ret = pack::packValues(signal, index);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Packing partitioning init message failed.");
                    return ret;
                }
                // broadcast to all workers + agent
                ret = broadcast::broadcastMessage(signal, MSG_PARTITION_DATASET);
                if (ret != DBERR_OK) {
                    return ret;
                }
                
                // partition the data
                ret = partitioning::partitionDataset(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed while partitioning dataset with index", index);
                    return ret;
                }
                // wait for acks
                ret = gatherResponses();
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Not all nodes finished successfully.");
                    return ret;
                }

                logger::log_success("Stored", dataset->objects.size(),"(", dataset->totalObjects, ") objects after partitioning");


                // free memory
                signal.clear();
            }

            // send ACK to the driver
            ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending ACK to driver.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleBuildIndexMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // broadcast it
            ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
            if (ret != DBERR_OK) {
                return ret;
            }

            // build the index
            ret = comm::execute::buildIndex(msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free memory
            msg.clear();

            // wait for ACK from everyone
            ret = gatherResponses();
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Not all nodes finished successfully.");
                return ret;
            }
            
            // send ACK to the driver
            ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending ACK message to driver.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleLoadDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // unpack the indexes of the datasets to load
            std::vector<int> datasetIndexes;
            ret = unpack::unpackValues(msg, datasetIndexes);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack dataset indexes.");
                return ret;
            }

            // free memory
            msg.clear();

            // notify the controllers and the local agent to load each dataset in the message
            for (auto &index : datasetIndexes) {
                // signal the begining of the partitioning
                SerializedMsg<char> signal(MPI_CHAR);
                // pack
                ret = pack::packValues(signal, index);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Packing partitioning init message failed.");
                    return ret;
                }
                
                // send to controllers
                #pragma omp parallel num_threads(MAX_THREADS)
                {
                    DB_STATUS local_ret = DBERR_OK;
                    #pragma omp for
                    for (int i=0; i<g_world_size; i++) {
                        if (i != HOST_LOCAL_RANK) {
                            // send to controllers
                            local_ret = send::sendMessage(signal, i, MSG_LOAD_DATASET, g_controller_comm);
                            if (ret != DBERR_OK) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_COMM_SEND, "Failed forwarding message to controller", i);
                                ret = local_ret;
                            }
                        } else {
                            // send signal to agent
                            local_ret = send::sendMessage(signal, AGENT_RANK, MSG_LOAD_DATASET, g_agent_comm);
                            if (local_ret != DBERR_OK) {
                                ret = local_ret;
                                logger::log_error(ret, "Failed sending partition init message. Dataset index: ", index);
                            }
                        }
                    }
                }
                if (ret != DBERR_OK) {
                    // parallel region failed
                    logger::log_error(ret, "Parallel send failed.");
                    return ret;
                }
                // free memory
                signal.clear();

                // wait for ACK from everyone
                ret = gatherResponses();
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Not all nodes finished successfully.");
                    return ret;
                }
            }

            // send ACK to the driver
            ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending ACK to driver.");
                return ret;
            }
            return ret;
        }
        
        static DB_STATUS gatherJoinResults(hec::Query* query, std::unique_ptr<hec::QResultBase>& localResults, std::unique_ptr<hec::QResultBase>& totalResults) {
            if (!totalResults) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Null query result pointer while gathering join results.");
                return DBERR_NULL_PTR_EXCEPTION;
            }
            DB_STATUS ret = DBERR_OK;
            // use threads to parallelize gathering of responses
            #pragma omp parallel num_threads(MAX_THREADS) reduction(query_output_reduction:totalResults)
            {
                DB_STATUS local_ret = DBERR_OK;
                MPI_Status status;

                #pragma omp for
                for (int i=1; i<g_world_size; i++) {
                    local_ret = probe(MPI_ANY_SOURCE, MPI_ANY_TAG, g_worker_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel for
                        ret = local_ret;
                    }
                    // check tag
                    if (status.MPI_TAG == MSG_NACK) {
                        // something failed during query evaluation
                        local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_worker_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = DBERR_COMM_RECEIVED_NACK;
                            logger::log_error(ret, "Received response from controller", i);
                        }
                    } else {
                        // receive result
                        SerializedMsg<char> msg(MPI_CHAR);
                        local_ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = DBERR_COMM_RECEIVED_NACK;
                            logger::log_error(ret, "Received NACK from agent.");
                        }
                        // unpack
                        hec::QResultBase* workerRaw = nullptr;
                        int res = qresult_factory::createNew(query, &workerRaw);
                        if (res != 0) {
                            #pragma omp cancel for
                            logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                            ret = DBERR_OBJ_CREATION_FAILED;
                            continue;
                        }
                        std::unique_ptr<hec::QResultBase> workerResult(workerRaw);
                        workerResult->deserialize(msg.data, msg.count);
                        // add results
                        logger::log_task("Merging results from worker", i);
                        local_ret = mergeResultObjects(totalResults.get(), workerResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "Merging query results failed.");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // free memory
                        msg.clear();
                    }
                }
            }
            
            // merge localResults as well
            logger::log_task("Merging local results");
            ret = mergeResultObjects(totalResults.get(), localResults.get());
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Merging local query results failed.");
                return ret;
            }

            return ret;
        }

        static DB_STATUS gatherBatchResults(std::unordered_map<int, hec::QResultBase*> &batchResultsMap) {
            DB_STATUS ret = DBERR_OK;
            MPI_Status status;
            int messageFound;
            int finishedWorkers = 0;

            while (finishedWorkers != g_world_size) {
                messageFound = 0;
                // probe agent comm
                ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status, messageFound);
                if (ret != DBERR_OK) {
                    return ret;
                }
                if (messageFound) {
                    // agent response
                    // check tag
                    if (status.MPI_TAG == MSG_NACK) {
                        // something failed during query evaluation
                        ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_agent_comm, status);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Failed to receive probed NACK response.");
                            return ret;
                        }
                        logger::log_error(ret, "Received NACK from agent.");
                        return DBERR_COMM_RECEIVED_NACK;
                    } else if (status.MPI_TAG == MSG_ACK) {
                        // received ACK, worker is finished
                        ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_agent_comm, status);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Failed to receive ACK response.");
                            return ret;
                        }
                        finishedWorkers++;
                        // logger::log_success("Received ACK from agent. Finished workers:", finishedWorkers);
                    } else {
                        // receive result message
                        SerializedMsg<char> msg(MPI_CHAR);
                        ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Failed to receive result from agent.");
                            return ret;
                        }
                        if (status.MPI_TAG == MSG_QUERY_BATCH_RESULT) {
                            // batch query result
                            // unpack
                            ret = unpack::unpackBatchResults(msg, batchResultsMap);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Failed to unpack batch results from message.");
                                return ret;
                            }
                            // free memory
                            msg.clear();
                        } else if (status.MPI_TAG == MSG_QUERY_RESULT) {
                            // single query result
                            hec::QResultBase* localResult;
                            // peek query and result types
                            hec::QueryType queryType = localResult->getQueryTypeFromSerializedBuffer(msg.data, msg.count);
                            hec::QueryResultType queryResultType = localResult->getResultTypeFromSerializedBuffer(msg.data, msg.count);
                            int res = qresult_factory::createNew(-1, queryType, queryResultType, &localResult);
                            if (res != 0) {
                                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                                return DBERR_OBJ_CREATION_FAILED;
                            }
                            // unpack
                            localResult->deserialize(msg.data, msg.count);
                            // add results to appropriate object
                            if (batchResultsMap.find(localResult->getQueryID()) == batchResultsMap.end()) {
                                // new entry
                                batchResultsMap[localResult->getQueryID()] = localResult;
                            } else {
                                // merge results
                                batchResultsMap[localResult->getQueryID()]->mergeResults(localResult);
                            }
                            // free memory
                            msg.clear();
                            delete localResult;
                            // logger::log_task("Received result for query", localResult.getID(), "from agent");
                        } else {
                            // wrong message tag
                            logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Unexpected message with tag", status.MPI_TAG, "Should have been either", MSG_QUERY_BATCH_RESULT, "or", MSG_QUERY_RESULT);
                            return DBERR_COMM_INVALID_MSG_TAG;
                        }
                    }
                    messageFound = 0;
                }

                // probe controller comm
                ret = probe(MPI_ANY_SOURCE, MPI_ANY_TAG, g_controller_comm, status, messageFound);
                if (ret != DBERR_OK) {
                    return ret;
                }
                if (messageFound) {
                    // controller response
                    // check tag
                    if (status.MPI_TAG == MSG_NACK) {
                        // something failed during query evaluation
                        ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_controller_comm, status);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Failed to receive probed NACK response.");
                            return ret;
                        }
                        logger::log_error(ret, "Received NACK from agent.");
                        return DBERR_COMM_RECEIVED_NACK;
                    } else if (status.MPI_TAG == MSG_ACK) {
                        // received ACK, worker is finished
                        ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_controller_comm, status);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Failed to receive ACK response.");
                            return ret;
                        }
                        finishedWorkers++;
                        // logger::log_success("Received ACK from worker", status.MPI_SOURCE,". Finished workers:", finishedWorkers);
                    } else {
                        // receive result message
                        SerializedMsg<char> msg(MPI_CHAR);
                        ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Failed to receive result from worker", status.MPI_SOURCE);
                            return ret;
                        }
                        if (status.MPI_TAG == MSG_QUERY_BATCH_RESULT) {
                            // batch query result
                            // unpack
                            ret = unpack::unpackBatchResults(msg, batchResultsMap);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Failed to unpack batch results from message.");
                                return ret;
                            }
                            // free memory
                            msg.clear();
                        } else if (status.MPI_TAG == MSG_QUERY_RESULT) {
                            // single query result
                            hec::QResultBase* localResult;
                            // peek query and result types
                            hec::QueryType queryType = localResult->getQueryTypeFromSerializedBuffer(msg.data, msg.count);
                            hec::QueryResultType queryResultType = localResult->getResultTypeFromSerializedBuffer(msg.data, msg.count);
                            int res = qresult_factory::createNew(-1, queryType, queryResultType, &localResult);
                            if (res != 0) {
                                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                                return DBERR_OBJ_CREATION_FAILED;
                            }
                            // unpack
                            localResult->deserialize(msg.data, msg.count);
                            // add results to appropriate object
                            if (batchResultsMap.find(localResult->getQueryID()) == batchResultsMap.end()) {
                                // new entry
                                batchResultsMap[localResult->getQueryID()] = localResult;
                            } else {
                                // merge results
                                batchResultsMap[localResult->getQueryID()]->mergeResults(localResult);
                            }
                            // free memory
                            msg.clear();
                            delete localResult;
                        } else {
                            // wrong message tag
                            logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Unexpected message with tag", status.MPI_TAG, "Should have been either", MSG_QUERY_BATCH_RESULT, "or", MSG_QUERY_RESULT);
                            return DBERR_COMM_INVALID_MSG_TAG;
                        }
                    }
                    messageFound = 0;
                }
            }
            return ret;
        }

        static DB_STATUS performJoinQuery(MPI_Status &status, SerializedMsg<char> &msg, hec::Query* query) {
            DB_STATUS ret = DBERR_OK;
            // broadcast join query to every worker
            logger::log_task("Broadcasting query...");
            ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
            if (ret != DBERR_OK) {
                return ret;
            }

            // evaluate and store results in the result object (pointer)
            std::unique_ptr<hec::QResultBase> localResults;
            logger::log_task("Executing query...");
            ret = execute::joinQuery(msg, localResults);
            if (ret != DBERR_OK) {
                return ret;
            }
            logger::log_task("Local results:", localResults->getResultCount());

            // create total query result object
            hec::QResultBase* rawTotalResults = nullptr;
            int res = qresult_factory::createNew(query, &rawTotalResults);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                return DBERR_OBJ_CREATION_FAILED;
            }
            std::unique_ptr<hec::QResultBase> totalResults(rawTotalResults);

            // wait for result
            logger::log_task("Gathering results...");
            ret = gatherJoinResults(query, localResults, totalResults);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while gathering results for query.");
                return ret;
            }
            logger::log_success("Gathered", totalResults->getResultCount(), "results.");

            // serialize results to message
            SerializedMsg<char> resultMsg(MPI_CHAR);
            totalResults->serialize(&resultMsg.data, resultMsg.count);
            // send result to driver
            logger::log_task("Sending results...");
            ret = send::sendMessage(resultMsg, DRIVER_GLOBAL_RANK, MSG_QUERY_RESULT, g_global_intra_comm);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // free memory
            resultMsg.clear();
            return ret;
        }

        static DB_STATUS performDistanceJoinQuery(MPI_Status &status, SerializedMsg<char> &msg, hec::Query* query) {
            // DB_STATUS ret = DBERR_OK;
            // // broadcast join query to every worker
            // ret = broadcast::broadcastMessage(msg, MSG_QUERY_DJ_INIT);
            // if (ret != DBERR_OK) {
            //     return ret;
            // }
            
            // // intermediate exchange of data
            // ret = distance_join::distanceJoinTraffic();
            // if (ret != DBERR_OK) {
            //     return ret;
            // } 
            
            // // create query result object
            // hec::QResultBase* rawTotalResults = nullptr;
            // int res = qresult_factory::createNew(query, &rawTotalResults);
            // if (res != 0) {
            //     logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
            //     return DBERR_OBJ_CREATION_FAILED;
            // }
            // std::unique_ptr<hec::QResultBase> totalResults(rawTotalResults);

            // // wait for final results 
            // ret = gatherJoinResults(query, totalResults);
            // if (ret != DBERR_OK) {
            //     logger::log_error(ret, "Failed while gathering results for query.");
            //     return ret;
            // }
            // // serialize to message
            // SerializedMsg<char> resultMsg(MPI_CHAR);
            // totalResults->serialize(&resultMsg.data, resultMsg.count);
            // // send result to driver
            // ret = send::sendMessage(resultMsg, DRIVER_GLOBAL_RANK, MSG_QUERY_RESULT, g_global_intra_comm);
            // if (ret != DBERR_OK) {
            //     return ret;
            // }
            
            // // free memory
            // resultMsg.clear();
            // return ret;
            return DBERR_FEATURE_UNSUPPORTED;
        }

        static DB_STATUS gatherkNNResults(hec::Query* query, std::unique_ptr<hec::QResultBase>& totalResults) {
            if (!totalResults) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Null query result pointer while gathering join results.");
                return DBERR_NULL_PTR_EXCEPTION;
            }
            DB_STATUS ret = DBERR_OK;
            // use threads to parallelize gathering of responses
            #pragma omp parallel num_threads(MAX_THREADS) reduction(query_output_reduction:totalResults)
            {
                DB_STATUS local_ret = DBERR_OK;
                MPI_Status status;
                #pragma omp for
                for (int i=0; i<g_world_size; i++) {
                    // probe for responses
                    if (i == 0) {
                        // wait for agent's response
                        local_ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // check tag
                        if (status.MPI_TAG == MSG_NACK) {
                            // something failed during query evaluation
                            local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_agent_comm, status);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel for
                                ret = DBERR_COMM_RECEIVED_NACK;
                                logger::log_error(ret, "Received NACK from agent.");
                            }
                        } else {
                            // receive result message
                            SerializedMsg<char> msg(MPI_CHAR);
                            local_ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel for
                                logger::log_error(ret, "Received response from agent.");
                                ret = local_ret;
                            }
                            // unpack
                            hec::QResultBase* localRaw = nullptr;
                            int res = qresult_factory::createNew(query, &localRaw);
                            if (res != 0) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                                ret = DBERR_OBJ_CREATION_FAILED;
                                continue;
                            }
                            std::unique_ptr<hec::QResultBase> localResult(localRaw);
                            localResult->deserialize(msg.data, msg.count);
                            // add results
                            local_ret = mergeResultObjects(totalResults.get(), localResult.get());
                            if (local_ret != DBERR_OK) {
                                logger::log_error(ret, "Merging query results failed.");
                                #pragma omp cancel for
                                ret = local_ret;
                            }
                        }
                    } else {
                        // wait for workers' responses (each thread with id X receives from worker with rank X)
                        local_ret = probe(i, MPI_ANY_TAG, g_controller_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // check tag
                        if (status.MPI_TAG == MSG_NACK) {
                            // something failed during query evaluation
                            local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_controller_comm, status);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel for
                                ret = DBERR_COMM_RECEIVED_NACK;
                                logger::log_error(ret, "Received response from controller", i);
                            }
                        } else {
                            // receive result
                            SerializedMsg<char> msg(MPI_CHAR);
                            local_ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel for
                                ret = DBERR_COMM_RECEIVED_NACK;
                                logger::log_error(ret, "Received NACK from agent.");
                            }
                            // unpack
                            hec::QResultBase* localRaw = nullptr;
                            int res = qresult_factory::createNew(query, &localRaw);
                            if (res != 0) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                                ret = DBERR_OBJ_CREATION_FAILED;
                                continue;
                            }
                            std::unique_ptr<hec::QResultBase> localResult(localRaw);

                            localResult->deserialize(msg.data, msg.count);
                            // add results
                            local_ret = mergeResultObjects(totalResults.get(), localResult.get());
                            if (local_ret != DBERR_OK) {
                                logger::log_error(ret, "Merging query results failed.");
                                #pragma omp cancel for
                                ret = local_ret;
                            }
                        }
                    }
                }
            }
            return ret;
        }

        static DB_STATUS performKnnQuery(MPI_Status &status, SerializedMsg<char> &msg, hec::Query* query) {
            DB_STATUS ret = DBERR_OK;
            // broadcast knn query to every worker
            ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
            if (ret != DBERR_OK) {
                return ret;
            }
            // create query result object
            hec::QResultBase* rawTotalResults = nullptr;
            int res = qresult_factory::createNew(query, &rawTotalResults);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                return DBERR_OBJ_CREATION_FAILED;
            }
            std::unique_ptr<hec::QResultBase> totalResults(rawTotalResults);
            // wait for result
            ret = gatherkNNResults(query, totalResults);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while gathering results for query.");
                return ret;
            }
            // serialize to message
            SerializedMsg<char> resultMsg(MPI_CHAR);
            totalResults->serialize(&resultMsg.data, resultMsg.count);
            // send result to driver
            ret = send::sendMessage(resultMsg, DRIVER_GLOBAL_RANK, MSG_QUERY_RESULT, g_global_intra_comm);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free memory
            resultMsg.clear();
            return ret;
        }

        static DB_STATUS handleQueryMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // unpack query
            char* localBuffer = msg.data;
            hec::Query* query = hec::Query::createFromBuffer(localBuffer);
            if (query == nullptr) {
                logger::log_error(DBERR_DESERIALIZE_FAILED, "Failed to create query from msg buffer.");
                return DBERR_DESERIALIZE_FAILED;
            }
            
            switch (query->getQueryType()) {
                case hec::Q_INTERSECTION_JOIN:
                case hec::Q_INSIDE_JOIN:
                case hec::Q_DISJOINT_JOIN:
                case hec::Q_EQUAL_JOIN:
                case hec::Q_MEET_JOIN:
                case hec::Q_CONTAINS_JOIN:
                case hec::Q_COVERS_JOIN:
                case hec::Q_COVERED_BY_JOIN:
                case hec::Q_FIND_RELATION_JOIN:
                    // perform the Join
                    ret = performJoinQuery(status, msg, query);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case hec::Q_DISTANCE_JOIN:
                    // perform the distance join
                    ret = performDistanceJoinQuery(status, msg, query);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case hec::Q_KNN:
                    // perform the kNN
                    ret = performKnnQuery(status, msg, query);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", query->getQueryType());
                    return DBERR_QUERY_INVALID_TYPE;
            }

            // free memory
            delete query;
            msg.clear();
            return ret;
        }

        static DB_STATUS handleQueryBatchRangeMessageParallel(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // unpack query batch
            std::vector<hec::Query*> queryBatch;
            ret = unpack::unpackQueryBatch(msg, &queryBatch);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack query batch message.");
                return ret;
            }

            // free msg memory
            msg.clear();

            // buffer all results
            std::unordered_map<int, hec::QResultBase*> batchResultsMap;

            // evaluate queries in batches
            for (int i=0; i<queryBatch.size(); i += QUERY_BATCH_SIZE) {
                SerializedMsg<char> batchMsg(MPI_CHAR);
                // pack QUERY_BATCH_SIZE queries together
                int endIndex = std::min(i + QUERY_BATCH_SIZE, static_cast<int>(queryBatch.size()));
                std::vector<hec::Query*> splitBatch = std::vector<hec::Query*>(queryBatch.begin() + i, queryBatch.begin() + endIndex);
                ret = pack::packQueryBatch(&splitBatch, batchMsg);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to pack query with id:", queryBatch[i]->getQueryID());
                    return ret;
                }
                // logger::log_success("Packed", splitBatch.size(), "queries.");
                
                // broadcast range query to every worker
                ret = broadcast::broadcastMessage(batchMsg, MSG_QUERY_BATCH_RANGE);
                if (ret != DBERR_OK) {
                    return ret;
                }

                // free query msg memory
                batchMsg.clear();

                // send a batch finished message to notify that this was all of this batch's queries
                ret = broadcast::broadcastInstructionMessage(MSG_INSTR_BATCH_FINISHED);
                if (ret != DBERR_OK) {
                    return ret;
                }
                
                // gather batch results
                ret = gatherBatchResults(batchResultsMap);
                if (ret != DBERR_OK) {
                    return ret;
                }

                // logger::log_success("I have results for", batchResultsMap.size(), "queries so far");
            }

            // delete queries
            for (auto &it: queryBatch) {
                delete it;
            }

            // serialize final batch results
            SerializedMsg<char> finalResultsMsg(MPI_CHAR);
            ret = pack::packBatchResults(batchResultsMap, finalResultsMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack final results.");
                return ret;
            }

            // send back to driver
            ret = send::sendMessage(finalResultsMsg, DRIVER_GLOBAL_RANK, MSG_QUERY_BATCH_RESULT, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Couldn't send results to Driver.");
                return ret;
            }

            // free memory
            finalResultsMsg.clear();

            // delete query results
            for (auto& [key, value] : batchResultsMap) {
                delete value;
            }
            batchResultsMap.clear();
            return ret;
        }

        static DB_STATUS handleQueryBatchKNNMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // unpack query batch
            std::vector<hec::Query*> queryBatch;
            ret = unpack::unpackQueryBatch(msg, &queryBatch);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack query batch message.");
                return ret;
            }

            // free msg memory
            msg.clear();

            // buffer all results
            std::unordered_map<int, hec::QResultBase*> batchResultsMap;

            // evaluate queries one-by-one, not really parallelizable for KNN
            // @todo: make parallelizable (super asynchronous)
            for (int i=0; i<queryBatch.size(); i++) {
                // create query msg to send
                SerializedMsg<char> queryMsg(MPI_CHAR);
                int res = queryBatch[i]->serialize(&queryMsg.data, queryMsg.count);
                if (res != 0) {
                    logger::log_error(DBERR_SERIALIZE_FAILED, "Failed to serialize knn query to message.");
                    return DBERR_SERIALIZE_FAILED;
                }
                // broadcast knn query to every worker
                ret = broadcast::broadcastMessage(queryMsg, MSG_QUERY);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // free memory
                queryMsg.clear();
                // create query result object
                hec::QResultBase* rawQueryResults = nullptr;
                res = qresult_factory::createNew(queryBatch[i], &rawQueryResults);
                if (res != 0) {
                    logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                    return DBERR_OBJ_CREATION_FAILED;
                }
                std::unique_ptr<hec::QResultBase> queryResults(rawQueryResults);
                // wait for result
                ret = gatherkNNResults(queryBatch[i], queryResults);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed while gathering results for query.");
                    return ret;
                }
                // std::vector<size_t> results = queryResults->getResultList();
                // logger::log_task("Query", queryBatch[i]->getQueryID(), queryBatch[i]->getWKT(), "results:");
                // for (auto &it: results) {
                //     logger::log_task(".  ", it);
                // }
                // add to batch
                batchResultsMap[i] = queryResults.release();
            }

            // delete queries
            for (auto &it: queryBatch) {
                delete it;
            }

            // serialize final batch results
            SerializedMsg<char> finalResultsMsg(MPI_CHAR);
            ret = pack::packBatchResults(batchResultsMap, finalResultsMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack final results.");
                return ret;
            }

            // send back to driver
            ret = send::sendMessage(finalResultsMsg, DRIVER_GLOBAL_RANK, MSG_QUERY_BATCH_RESULT, g_global_intra_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Couldn't send results to Driver.");
                return ret;
            }

            // free memory
            finalResultsMsg.clear();

            // delete query results
            for (auto& [key, value] : batchResultsMap) {
                delete value;
            }
            batchResultsMap.clear();
            return ret;
        }

        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            // check message tag
            switch (status.MPI_TAG) {
                case MSG_INSTR_FIN:
                    /* terminate everything */
                    // forward instruction to other workers
                    ret = forward::forwardInstructionMessage(MSG_INSTR_FIN);
                    if (ret != DBERR_OK) {
                        logger::log_error(DBERR_COMM_RECV, "Failed forwarding instruction from driver");
                        return DBERR_COMM_RECV;
                    }
                    
                    // wait for ACKs
                    ret = gatherResponses();
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Not all nodes finished successfully.");
                        return ret;
                    }
                    // send ACK to driver
                    ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed sending ACK to driver.");
                        return ret;
                    }
                    return DB_FIN;
                case MSG_PREPARE_DATASET:
                    /* dataset metadata message */
                    ret = handlePrepareDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling dataset metadata message.");
                        return ret;
                    }
                    break;
                case MSG_UNLOAD_DATASET:
                    /* unload dataset message */
                    ret = handleUnloadDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling dataset metadata message.");
                        return ret;
                    }
                    break;
                case MSG_PARTITION_DATASET:
                    /** initiate partitioning */
                    ret = handlePartitionDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling partition dataset message.");
                        return ret;
                    }
                    break;
                case MSG_LOAD_DATASET:
                    /** initiate dataset loading */
                    ret = handleLoadDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling load dataset message.");
                        return ret;
                    }
                    break;
                case MSG_BUILD_INDEX:
                    /** initiate index building */
                    ret = handleBuildIndexMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling load dataset message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY:
                    /** Initiate query */
                    ret = handleQueryMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling query message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY_BATCH_RANGE:
                    /** Initiate query */
                    ret = handleQueryBatchRangeMessageParallel(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling query message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY_BATCH_KNN:
                    /** Initiate query */
                    ret = handleQueryBatchKNNMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling query message.");
                        return ret;
                    }
                    break;
                default:
                    // unkown instruction
                    logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Didn't expect message with tag", status.MPI_TAG);
                    return DBERR_COMM_UNKNOWN_INSTR;
            }

            return ret;
        }

        DB_STATUS broadcastSysMetadata() {
            SerializedMsg<char> msgPack(MPI_CHAR);
            // serialize
            DB_STATUS ret = pack::packSystemMetadata(msgPack);
            if (ret != DBERR_OK) {
                return ret;
            }

            // send the pack to workers
            ret = comm::broadcast::broadcastMessage(msgPack, MSG_SYS_INFO);
            if (ret != DBERR_OK) {
                return ret;
            }              

            // free
            msgPack.clear();

            return ret;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            // continuously listen for driver messages until termination is requested
            while(true){
                // check for DRIVER messages  
                // logger::log_task("Probing for messages from driver...");
                ret = probe(DRIVER_GLOBAL_RANK, MPI_ANY_TAG, g_global_intra_comm, status);
                if (ret != DBERR_OK){
                    return ret;
                }
                // driver has sent a message, propagate to everyone
                ret = host::pullIncoming(status);
                if (ret == DB_FIN) {
                    // it was a termination message
                    break;
                } else if (ret != DBERR_OK) {
                    // something went wrong
                    return ret;
                }
            }
            return ret;
        }
    }
}