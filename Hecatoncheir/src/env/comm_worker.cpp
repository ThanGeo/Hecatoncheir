#include "env/comm_common.h"
#include "env/comm_worker.h"

namespace comm
{
    namespace worker
    {
        DB_STATUS serializeAndSendGeometryBatch(Batch* batch) {
            DB_STATUS ret = DBERR_OK;
            // check if batch is valid
            if (!batch->isValid()) {
                logger::log_error(DBERR_BATCH_FAILED, "Batch is invalid, check its destRank and tag");
                return DBERR_BATCH_FAILED;
            }
            if (batch->destRank > HOST_LOCAL_RANK) {
                // send batch message to workers
                SerializedMsg<char> msg(MPI_CHAR);
                // logger::log_success("Sending batch of size", batch->objectCount);
                // serialize (@todo: add try/catch for segfauls, mem access etc...)   
                ret = batch->serialize(&msg.data, msg.count);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_BATCH_FAILED, "Batch serialization failed");
                    return DBERR_BATCH_FAILED;
                }
                ret = comm::send::sendMessage(msg, batch->destRank, batch->tag, *batch->comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Sending serialized geometry batch failed");
                    return ret;
                }
                // free memory
                msg.clear();
            } else {
                // prepare dataset object
                Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(batch->datasetID);
                if (dataset == nullptr) {
                    logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Empty dataset object (no metadata). Can not partition. Batch dataset id:", batch->datasetID);
                    return DBERR_NULL_PTR_EXCEPTION;
                }
                # pragma omp critical
                {
                    // local batch, handle first
                    for (auto &obj : batch->objects) {
                        dataset->storeObject(obj);
                    }
                }

            }
            return ret;
        }

        /** @brief Deserializes a serialized batch message and stores its data on disk and in the proper containers in-memory. 
         * @param[in] buffer The serialized batch message
         * @param[in] bufferSize The message's size
         * @param[in] outFile A file pointer pointing to an already open partition file on disk. Its position should be after the object count/dataset metadata position.
         * @param[in] dataset The dataset getting partitioned
         * @param[out] continueListening If the message is empty (0 objects aster serialization), then it signals the end of the partitioning for the dataset.
        */
        static DB_STATUS deserializeBatchMessageAndHandle(char *buffer, int bufferSize, FILE* partitionFile, Dataset *dataset, int &continueListening) {
            DB_STATUS ret = DBERR_OK;
            Batch batch;
            // deserialize the batch
            batch.deserialize(buffer, bufferSize);
            // logger::log_success("Received batch with", batch.objectCount, "objects");
            if (batch.objectCount > 0) {
                // append to partition file
                ret = storage::writer::partitionFile::appendBatchToPartitionFile(partitionFile, &batch, dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Failed when writing batch to partition file");
                    return DBERR_DISK_WRITE_FAILED;
                }
                
            } else {
                // empty batch, set flag to stop listening for this dataset
                continueListening = 0;
                // update the object count value in the file
                ret = storage::writer::updateObjectCountInFile(partitionFile, dataset->totalObjects);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Failed when updating partition file object count");
                    return DBERR_DISK_WRITE_FAILED;
                }
            }
            return ret;
        }

        static DB_STATUS deserializeBatchMessageAndHandle(char *buffer, int bufferSize, Dataset *dataset, int &continueListening) {
            DB_STATUS ret = DBERR_OK;
            Batch batch;
            // deserialize the batch
            batch.deserialize(buffer, bufferSize);
            // logger::log_success("Received batch with", batch.objectCount, "objects");
            if (batch.objectCount > 0) {    
                for (auto &obj : batch.objects) {
                    // store into dataset
                    dataset->storeObject(obj);
                }
            } else {
                // empty batch, set flag to stop listening for this dataset
                continueListening = 0;
                // update total object count and capacity
                dataset->totalObjects = dataset->objects.size();
                // logger::log_success("Received", dataset->totalObjects, "objects");
            }

            return ret;
        }

        static DB_STATUS deserializeSingleShapeMessageAndHandle(SerializedMsg<char> &msg, Dataset *dataset, int &continueListening) {
            DB_STATUS ret = DBERR_OK;
            Shape object;
            if (msg.count == 0) {
                // empty batch, set flag to stop listening for this dataset
                continueListening = 0;
                // update total object count and capacity
                dataset->totalObjects = dataset->objects.size();
                dataset->objects.shrink_to_fit();
                // logger::log_success("Received", dataset->totalObjects, "objects");
            } else {
                // deserialize the shape
                ret = unpack::unpackShape(msg, object);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // store into dataset
                dataset->storeObject(object);
            }
            
            return ret;
        }

        /** @brief Pulls a serialized batch message and stores its contents ON DISK (persistence).
         * @warning the objects are not stored in memory.
         */
        static DB_STATUS pullBatchMessageAndHandle(MPI_Status status, FILE* partitionFile, Dataset *dataset, int &continueListening) {
            int tag = status.MPI_TAG;
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling serialized message");
                return ret;
            }

            // deserialize based on tag
            switch (tag) {
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    ret = deserializeBatchMessageAndHandle(msg.data, msg.count, partitionFile, dataset, continueListening);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_SINGLE_POLYGON:
                    return DBERR_FEATURE_UNSUPPORTED;
                    break;
                default:
                    logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Invalid message tag");
                    break;
            }
            // free memory
            msg.clear();
            return ret;
        }

        /** @brief Pulls a serialized batch message and stores its contents in memory, in the given dataset. */
        static DB_STATUS pullBatchMessageAndHandle(MPI_Status status, Dataset *dataset, int &continueListening) {
            int tag = status.MPI_TAG;
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling serialized message");
                return ret;
            }

            // deserialize based on tag
            switch (tag) {
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    ret = deserializeBatchMessageAndHandle(msg.data, msg.count, dataset, continueListening);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_SINGLE_POLYGON:
                    ret = deserializeSingleShapeMessageAndHandle(msg, dataset, continueListening);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Invalid message tag:", tag);
                    break;
            }
            // free memory
            msg.clear();
            return ret;
        }

        static DB_STATUS listenForDatasetPartitioning(DatasetIndex datasetIndex) {
            DB_STATUS ret = DBERR_OK;
            MPI_Status status;
            int listen = 1;

            // prepare dataset object
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(datasetIndex);
            if (dataset == nullptr) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Empty dataset object (no metadata). Can not partition.");
                return DBERR_NULL_PTR_EXCEPTION;
            }
            
            // logger::log_success("Global dataspace:", g_config.datasetOptions.dataspaceMetadata.xMinGlobal, g_config.datasetOptions.dataspaceMetadata.yMinGlobal, g_config.datasetOptions.dataspaceMetadata.xMaxGlobal, g_config.datasetOptions.dataspaceMetadata.yMaxGlobal);
            // listen for dataset batches until an empty batch arrives
            while(listen) {
                // proble blockingly for batch
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG, g_worker_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // pull
                switch (status.MPI_TAG) {
                    // case MSG_INSTR_FIN: // todo: maybe this is not needed
                    //     /* stop listening for instructions */
                    //     ret = recv::receiveInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG, g_worker_comm, status);
                    //     if (ret == DBERR_OK) {
                    //         // break the listening loop
                    //         return DB_FIN;
                    //     }
                    //     break;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                    case MSG_SINGLE_POLYGON:
                        /* batch geometry message */
                        ret = pullBatchMessageAndHandle(status, dataset, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }  
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "After the dataset metadata pack, only geometry packs are expected. Received message with tag", status.MPI_TAG);
                        ret = DBERR_COMM_WRONG_MESSAGE_ORDER;
                        return ret;
                }
            }

            // logger::log_success("Received", dataset->objects.size(),"(", dataset->totalObjects, ") objects after partitioning");

            return ret;
        }

        static DB_STATUS listenForDatasetPartitioningWithPersistence(DatasetIndex datasetIndex) {
            DB_STATUS ret = DBERR_OK;
            MPI_Status status;
            FILE* partitionFile;
            int listen = 1;
            size_t dummy = 0;

            // prepare dataset object
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(datasetIndex);
            if (dataset == nullptr) {
                logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Empty dataset object (no metadata). Can not partition.");
                return DBERR_NULL_PTR_EXCEPTION;
            }
            
            // open the partition data file
            partitionFile = fopen(dataset->metadata.path.c_str(), "wb+");
            if (partitionFile == NULL) {
                logger::log_error(DBERR_MISSING_FILE, "Couldnt open dataset file:", dataset->metadata.path);
                return DBERR_MISSING_FILE;
            }
            // write a dummy value for the total object count in the very begining of the file
            // it will be corrected when the batches are finished
            fwrite(&dummy, sizeof(size_t), 1, partitionFile);

            // write the dataset metadata to the partition file
            ret = storage::writer::partitionFile::appendDatasetMetadataToPartitionFile(partitionFile, dataset);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while writing the dataset metadata to the partition file");
                goto STOP_LISTENING;
            }

            // listen for dataset batches until an empty batch arrives
            while(listen) {
                // proble blockingly for batch
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG, g_worker_comm, status);
                if (ret != DBERR_OK) {
                    goto STOP_LISTENING;
                }
                // pull
                switch (status.MPI_TAG) {
                    // case MSG_INSTR_FIN: // todo: maybe this is not needed
                    //     /* stop listening for instructions */
                    //     ret = recv::receiveInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG, g_worker_comm, status);
                    //     if (ret == DBERR_OK) {
                    //         // break the listening loop
                    //         return DB_FIN;
                    //     }
                    //     goto STOP_LISTENING;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = pullBatchMessageAndHandle(status, partitionFile, dataset, listen);
                        if (ret != DBERR_OK) {
                            goto STOP_LISTENING;
                        }  
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "After the dataset metadata pack, only geometry packs are expected. Received message with tag", status.MPI_TAG);
                        ret = DBERR_COMM_WRONG_MESSAGE_ORDER;
                        goto STOP_LISTENING;
                }
            }
            
STOP_LISTENING:
            // close files
            if (partitionFile == NULL) {
                fclose(partitionFile);
            }
            
            return ret;
        }

        /**
        @brief pulls the probed instruction message and based on its tag, performs the requested instruction
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullInstructionAndPerform(MPI_Status &status) {
            // receive the instruction message
            DB_STATUS ret = recv::receiveInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG, g_worker_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // perform the corresponding instruction
            switch (status.MPI_TAG) {
                case MSG_INSTR_FIN:
                    // send ACK
                    ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to send ACK for termination instruction.");
                        return ret;
                    }
                    return DB_FIN;
                case MSG_INSTR_BATCH_FINISHED:
                    // send ACK
                    ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to send ACK for batch finished.");
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_COMM_UNKNOWN_INSTR, "Unknown instruction");
                    return DBERR_COMM_UNKNOWN_INSTR;
            }
            return ret;
        }

        static DB_STATUS handleSysMetadataMessage(MPI_Status status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack metadata and store
            ret = unpack::unpackSystemMetadata(msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // verify local directories
            ret = configurer::verifySystemDirectories();
            if (ret != DBERR_OK) {
                // error
                logger::log_error(ret, "Failed while verifying system directories.");
                return ret ;
            }
            // free memory
            msg.clear();
            // send response
            if (ret == DBERR_OK) {
                // send ACK 
                ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Send ACK failed.");
                }
            } else {
                // there was an error, send NACK and propagate error code locally
                DB_STATUS errorCode = ret;
                ret = send::sendResponse(HOST_LOCAL_RANK, MSG_NACK, g_worker_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Send NACK failed.");
                    return ret;
                }
                logger::log_error(ret, "Handling system metadata message failed");
                return errorCode;
            }
            return ret;
        }

        static DB_STATUS handlePrepareDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);

            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // perform the preparation
            int id = -1;
            ret = comm::execute::prepareDataset(msg, id);
            if (ret != DBERR_OK) {
                return ret;
            }

            return ret;
        }

        static DB_STATUS handleUnloadDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
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

            // send ACK back that the datasets has been unloaded successfully
            ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleDatasetInfoMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // temp Dataset object
            Dataset tempDataset;
            ret = tempDataset.deserialize(msg.data, msg.count);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Deserializing dataset info failed.");
                return ret;
            }
            // logger::log_task("temp dataset:", tempDataset.objects.size(), tempDataset.metadata.internalID);
            // get actual dataset and replace total object count + dataspace info
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(tempDataset.metadata.internalID);
            // logger::log_task("dataset:", dataset->objects.size(), dataset->metadata.internalID);
            dataset->totalObjects = tempDataset.totalObjects;
            dataset->metadata.dataspaceMetadata = tempDataset.metadata.dataspaceMetadata;
            g_config.datasetOptions.updateDataspace();
            g_config.partitioningMethod->setDistGridDataspace(g_config.datasetOptions.dataspaceMetadata);
            g_config.partitioningMethod->setPartGridDataspace(g_config.datasetOptions.dataspaceMetadata);
            g_config.datasetOptions.dataspaceMetadata.boundsSet = true;

            // printf("Partitioning metadata set: (%f,%f),(%f,%f)\n", g_config.partitioningMethod->distGridDataspaceMetadata.xMinGlobal, g_config.partitioningMethod->distGridDataspaceMetadata.yMinGlobal, g_config.partitioningMethod->distGridDataspaceMetadata.xMaxGlobal, g_config.partitioningMethod->distGridDataspaceMetadata.yMaxGlobal);
            // printf("Partitioning metadata set: gPPD %d, dPPD %d, pPPD %d\n", g_config.partitioningMethod->getGlobalPPD(), g_config.partitioningMethod->getDistributionPPD(), g_config.partitioningMethod->getPartitioningPPD());
            // printf("distExtents %f,%f, partitionExtents %f,%f\n", g_config.partitioningMethod->getDistPartionExtentX(), g_config.partitioningMethod->getDistPartionExtentY(), g_config.partitioningMethod->getPartPartionExtentX(), g_config.partitioningMethod->getPartPartionExtentY());
            
            // free memory
            msg.clear();
            return ret;
        }

        static DB_STATUS handlePartitionDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack
            std::vector<int> datasetIndexes;
            ret = unpack::unpackValues(msg, datasetIndexes);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack dataset indexes.");
                return ret;
            }
            
            // free message memory
            msg.clear();

            for (auto &index : datasetIndexes) {
                if (g_config.datasetOptions.getDatasetByIdx(index)->metadata.persist) {
                    // if we need persistence (store on disk)
                    ret = listenForDatasetPartitioningWithPersistence((DatasetIndex)index);
                    if (ret != DBERR_OK) {
                        // there was an error, send NACK and propagate error code locally
                        DB_STATUS errorCode = ret;
                        ret = send::sendResponse(HOST_LOCAL_RANK, MSG_NACK, g_worker_comm);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Send NACK failed.");
                            return ret;
                        }
                        return errorCode;
                    } 
                } else {
                    // no persistence
                    ret = listenForDatasetPartitioning((DatasetIndex)index);
                    if (ret != DBERR_OK) {
                        // there was an error, send NACK and propagate error code locally
                        DB_STATUS errorCode = ret;
                        ret = send::sendResponse(HOST_LOCAL_RANK, MSG_NACK, g_worker_comm);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Send NACK failed.");
                            return ret;
                        }
                        return errorCode;
                    } 
                }
            }

            // send ACK back that all data for this dataset has been received and processed successfully
            ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleBuildIndexMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // build the index
            ret = comm::execute::buildIndex(msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // send ACK back that the datasets has been loaded successfully
            ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleLoadDatasetMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack
            std::vector<int> datasetIndexes;
            ret = unpack::unpackValues(msg, datasetIndexes);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack dataset indexes.");
                return ret;
            }
            
            // free message memory
            msg.clear();

            for (auto &index : datasetIndexes) {
                // get dataset container
                Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(index);
                if (dataset == nullptr) {
                    logger::log_error(DBERR_NULL_PTR_EXCEPTION, "The dataset with id", index, "does not exist or has not been prepared by the system.");
                    return DBERR_NULL_PTR_EXCEPTION;
                }
                // load partitioned dataset
                ret = storage::reader::partitionFile::loadDatasetComplete(dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Failed loading partition file MBRs");
                    return ret;
                }  
                if (dataset->metadata.dataType == DT_POLYGON || dataset->metadata.dataType == DT_LINESTRING) {
                    // load APRIL
                    ret = APRIL::reader::loadAPRIL(dataset);
                    if (ret != DBERR_OK) {
                        logger::log_error(DBERR_DISK_READ_FAILED, "Failed loading partition APRIL");
                        return ret;
                    }
                }
                
            }

            // send ACK back that the datasets has been loaded successfully
            ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleGlobalDataspaceMessage(MPI_Status &status) {
            SerializedMsg<double> msg(MPI_DOUBLE);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack
            ret = g_config.datasetOptions.dataspaceMetadata.deserialize(msg.data, msg.count);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Deserializing global dataspace message failed.");
                return ret;
            }
            // set global dataspace to all datasets
            g_config.datasetOptions.updateDatasetDataspaceToGlobal();

            g_config.datasetOptions.dataspaceMetadata.print();

            // free message memory
            msg.clear();
            // dont send ACK, return @todo, maybe send?
            return ret;
        }

        static DB_STATUS handleQueryMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // evaluate and store results in the result object (pointer)
            // logger::log_task("Evaluating query...");
            std::unique_ptr<hec::QResultBase> queryResult;
            ret = execute::joinQuery(msg, queryResult);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free message memory
            msg.clear();

            // pack results to send
            SerializedMsg<char> resultMsg(MPI_CHAR);
            queryResult->serialize(&resultMsg.data, resultMsg.count);

            // send results
            // logger::log_task("Sending", queryResult->getResultCount(), "query results.");
            ret = send::sendMessage(resultMsg, HOST_LOCAL_RANK, MSG_QUERY_RESULT, g_worker_comm);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free message memory
            resultMsg.clear();

            return ret;
        }

        static DB_STATUS handleDJBatchRequestMessage(MPI_Status &status, std::unordered_map<int, DJBatch> &borderObjectsMap) {
            DB_STATUS ret = DBERR_OK;
            // receive the message
            SerializedMsg<char> requestMsg(MPI_CHAR);
            ret = recv::receiveMessage(status, requestMsg.type, g_worker_comm, requestMsg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // get node rank from message
            std::vector<int> rank;
            ret = unpack::unpackValues(requestMsg, rank);
            if (ret != DBERR_OK) {
                return ret;
            }
            if (rank.size() != 1) {
                logger::log_error(DBERR_BUFFER_SIZE_MISMATCH, "Unexpected size in DJ batch request rank message.");
                return DBERR_BUFFER_SIZE_MISMATCH;
            }
            int nodeRank = rank[0];
            if (nodeRank < 0 || nodeRank > g_world_size) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Invalid node rank in DJ batch request message. Rank:", nodeRank);
                return DBERR_INVALID_PARAMETER;
            }
            // free memory
            requestMsg.clear();
            // create and send the batches
            Dataset* R = g_config.datasetOptions.getDatasetR();
            Dataset* S = g_config.datasetOptions.getDatasetS();
            SerializedMsg<char> batchMsg(MPI_CHAR);
            // serialize
            ret = borderObjectsMap[nodeRank].serialize(&batchMsg.data, batchMsg.count);
            if (ret != DBERR_OK) {
                return ret;
            }
            // send
            ret = send::sendMessage(batchMsg, HOST_LOCAL_RANK, MSG_QUERY_DJ_BATCH, g_worker_comm);
            if (ret != DBERR_OK) {
                return ret;
            }
            // free memory
            batchMsg.clear();
            return ret;
        }

        static DB_STATUS handleDJEvaluateBatchMessage(MPI_Status &status, hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            // receive the message
            SerializedMsg<char> batchMsg(MPI_CHAR);
            ret = recv::receiveMessage(status, batchMsg.type, g_worker_comm, batchMsg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // unpack batch
            DJBatch batch;
            ret = batch.deserialize(batchMsg.data, batchMsg.count);
            if (ret != DBERR_OK) {
                return ret;
            }

            // evaluate the distance join on the newly received objects
            // logger::log_success("Received", batch.objectsR.size(), "objects R and", batch.objectsS.size(), "objects S.");
            ret = g_config.datasetOptions.getDatasetR()->index->evaluateDJBatch(query, batch, queryResult);
            if (ret != DBERR_OK) {
                return ret;
            }

            // send ACK for this batch
            ret = send::sendResponse(HOST_LOCAL_RANK, MSG_ACK, g_worker_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 
            
            return ret;
        }

        static DB_STATUS handleDistanceJoinQueryMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack query
            char* localBuffer = msg.data;
            hec::Query* queryPtr = hec::Query::createFromBuffer(localBuffer);
            if (queryPtr == nullptr) {
                logger::log_error(ret, "Failed to create query from message buffer.");
                return ret;
            }

            // free message memory
            msg.clear();

            // setup query result object
            std::unique_ptr<hec::QResultBase> queryResult;
            int res = qresult_factory::createNew(queryPtr, queryResult);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                return DBERR_OBJ_CREATION_FAILED;
            }

            // batches of objects that rest on border areas
            std::unordered_map<int, DJBatch> borderObjectsMap;
            for (int i=0; i<g_world_size; i++) {
                borderObjectsMap[i] = DJBatch(i, g_config.datasetOptions.getDatasetR()->metadata.dataType, g_config.datasetOptions.getDatasetS()->metadata.dataType);
            }
            // evaluate local distances and find the border objects
            ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(queryPtr, borderObjectsMap, queryResult);
            if (ret != DBERR_OK) {
                return ret;
            }

            // send border batch sizes (object count) to controller
            SerializedMsg<char> borderObjectSizesMsg(MPI_CHAR);
            ret = pack::packBorderObjectSizes(borderObjectsMap, borderObjectSizesMsg);
            if (ret != DBERR_OK) {
                return ret;
            }
            ret = send::sendMessage(borderObjectSizesMsg, HOST_LOCAL_RANK, MSG_QUERY_DJ_COUNT, g_worker_comm);
            if (ret != DBERR_OK) {
                return ret;
            }
            borderObjectSizesMsg.clear();

            // listening loop
            bool listen = true;
            while (listen) {
                // probe for the next step
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG, g_worker_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // handle message based on its tag/action
                switch (status.MPI_TAG) {
                    case MSG_QUERY_DJ_REQUEST_INIT:
                        // message requests a batch of objects to be sent
                        ret = handleDJBatchRequestMessage(status, borderObjectsMap);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        break;
                    case MSG_QUERY_DJ_BATCH:
                        // message contains a batch of objects for evaluation
                        ret = handleDJEvaluateBatchMessage(status, queryPtr, queryResult);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        break;
                    case MSG_QUERY_DJ_FIN:
                        // query is finished
                        // receive message
                        ret = recv::receiveInstructionMessage(HOST_LOCAL_RANK, MSG_QUERY_DJ_FIN, g_worker_comm, status);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        // stop listening
                        listen = false;
                        break;
                    default:
                        logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Unexpected message during DJ with tag", status.MPI_TAG);
                        return DBERR_COMM_INVALID_MSG_TAG;
                }
            }

            // pack results to send
            SerializedMsg<char> resultMsg(MPI_CHAR);
            queryResult->serialize(&resultMsg.data, resultMsg.count);

            // send results
            ret = send::sendMessage(resultMsg, HOST_LOCAL_RANK, MSG_QUERY_RESULT, g_worker_comm);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free message memory
            resultMsg.clear();

            // free query memory
            delete queryPtr;

            return ret;
        }

        static DB_STATUS handleQueryBatchRangeMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_worker_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // holds all the final batch results
            std::unordered_map<int, std::unique_ptr<hec::QResultBase>> batchResults;
            // evaluate batch
            ret = execute::batchRangeQueries(msg, batchResults);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // free message memory
            msg.clear();
            
            // Pack results
            SerializedMsg<char> batchResultMsg(MPI_CHAR);
            ret = pack::packBatchResults(batchResults, batchResultMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack batch results.");
                return ret;
            }

            // Send results
            ret = send::sendMessage(batchResultMsg, HOST_LOCAL_RANK, MSG_QUERY_BATCH_RESULT, g_worker_comm);
            if (ret != DBERR_OK) {
                return ret;
            }

            // Cleanup
            batchResultMsg.clear();
            batchResults.clear();

            return ret;
        }

        /**
        @brief pulls incoming message sent by the local controller 
         * (the one probed last, whose metadata is stored in the status parameter)
         * Based on the tag of the message, it performs the corresponding request.
         */
        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            /* instruction message */
            if (status.MPI_TAG >= MSG_INSTR_BEGIN && status.MPI_TAG < MSG_INSTR_END) {
                ret = pullInstructionAndPerform(status);
                return ret;

            }
            /* non-instruction message */
            switch (status.MPI_TAG) {
                case MSG_SYS_INFO:
                    // logger::log_success("MSG_SYS_INFO");
                    ret = handleSysMetadataMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_PREPARE_DATASET:
                    // logger::log_success("MSG_PREPARE_DATASET");
                    ret = handlePrepareDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_UNLOAD_DATASET:
                    ret = handleUnloadDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_DATASET_METADATA:
                    ret = handleDatasetInfoMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_PARTITION_DATASET:
                    // logger::log_success("MSG_PARTITION_DATASET");
                    ret = handlePartitionDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                
                case MSG_LOAD_DATASET:
                    // logger::log_success("MSG_LOAD_DATASET");
                    ret = handleLoadDatasetMessage(status);
                    if (ret != DBERR_OK) {
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
                case MSG_GLOBAL_DATASPACE:
                    // logger::log_success("MSG_GLOBAL_DATASPACE");
                    ret = handleGlobalDataspaceMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling load dataset message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY:
                    ret = handleQueryMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_QUERY_DJ_INIT:
                    ret = handleDistanceJoinQueryMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_QUERY_BATCH_RANGE:
                    ret = handleQueryBatchRangeMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_QUERY_BATCH_KNN:
                    // unsupported yet
                    logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Unsupported batch KNN on agent.");
                    return DBERR_FEATURE_UNSUPPORTED;
                    break;
                default:
                    logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Didn't expect message with tag", status.MPI_TAG);
                    return DBERR_COMM_WRONG_MESSAGE_ORDER;
            }
            return ret;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            while(true){
                /* Do a blocking probe to wait for the next task/instruction by the local controller (parent) */
                // logger::log_task("Probing for messages from host...");
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG, g_worker_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // pull the probed message
                ret = pullIncoming(status);
                if (ret == DB_FIN) {
                    goto STOP_LISTENING;
                } else if(ret != DBERR_OK){
                    return ret;
                }
            }
STOP_LISTENING:
            return DBERR_OK;
        }
    } 
}