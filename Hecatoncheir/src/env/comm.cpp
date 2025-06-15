#include "env/comm.h"

#include <unordered_set>

namespace comm 
{

    #define QUERY_BATCH_SIZE 1000

    /** @brief Returns the object count of a serialized batch message.
     * @details The first value (sizeof(DataType)) indicates the data type of the objects in the message.
     * The second value (sizeof(size_t) is the object count.)
     */
    static size_t getBatchObjectCountFromMessage(SerializedMsg<char> &msg) {
        size_t objectCount = -1;
        size_t offset = sizeof(DataType);  // batch object count is after the data type value
        memcpy(&objectCount, &msg.data[offset], sizeof(size_t));
        return objectCount;
    }

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

    /**
    @brief probes for a message in the given communicator with the specified parameters.
     * if it exists, its metadata is stored in the status parameters.
     * returns true/false if a message that fits the parameters exists.
     * Does not receive the message, must call MPI_Recv for that.
     */
    // static DB_STATUS probe(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status, int &messageExists) {
    //     int mpi_ret = MPI_Iprobe(sourceRank, tag, comm, &messageExists, &status);
    //     if(mpi_ret != MPI_SUCCESS) {
    //         logger::log_error(DBERR_COMM_PROBE_FAILED, "Non Blocking probe failed");
    //         return DBERR_COMM_PROBE_FAILED;
    //     }
    //     return DBERR_OK;
    // }

    // /**
    // @brief probes for a message in the given communicator with the specified parameters
    //  * and blocks until such a message arrives. 
    //  * Does not receive the message, must call MPI_Recv for that.
    //  */
    // static DB_STATUS probe(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
    //     int mpi_ret = MPI_Probe(sourceRank, tag, comm, &status);
    //     if(mpi_ret != MPI_SUCCESS) {
    //         logger::log_error(DBERR_COMM_PROBE_FAILED, "Blocking probe failed");
    //         return DBERR_COMM_PROBE_FAILED;
    //     }
    //     return DBERR_OK;
    // }

    /** @brief 
     * Waits for a response from the AGENT (ACK or NACK)
     * When it comes, it forwards it to the host controller.
     */
    static DB_STATUS waitForResponse() {
        MPI_Status status;
        // wait for response by the agent that all is well for the APRIL creation
        DB_STATUS ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
        if (ret != DBERR_OK) {
            return ret;
        }
        // receive response from agent
        ret = recv::receiveResponse(AGENT_RANK, status.MPI_TAG, g_agent_comm, status);
        if (ret != DBERR_OK) {
            return ret;
        }
        // forward response to host controller
        ret = send::sendResponse(HOST_LOCAL_RANK, status.MPI_TAG,g_controller_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Forwarding response to host controller failed");
            return ret;
        }
        // check response, if its NACK then terminate
        if (status.MPI_TAG == MSG_NACK) {
            logger::log_error(DBERR_COMM_RECEIVED_NACK, "Received NACK by agent");
            return DBERR_COMM_RECEIVED_NACK;
        }
        return ret;
    }    

    /** @brief Wait for result message from the local agent. 
     * If it returns NACK, propagate the error and terminate. 
     * if it returns a result message, forward it to the host controller.
     * */
    static DB_STATUS waitForResults() {
        MPI_Status status;
        // probe
        DB_STATUS ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
        if (ret != DBERR_OK) {
            return ret;
        }
        // check message tag
        if (status.MPI_TAG == MSG_NACK) {
            // an error occured in the agent
            ret = recv::receiveResponse(AGENT_RANK, status.MPI_TAG, g_agent_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward response to host controller
            ret = send::sendResponse(HOST_LOCAL_RANK, status.MPI_TAG, g_controller_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forwarding response to host controller failed");
                return ret;
            }
            // its NACK so terminate
            logger::log_error(DBERR_COMM_RECEIVED_NACK, "Received NACK by agent");
            return DBERR_COMM_RECEIVED_NACK;

        } else {
            // result msg
            SerializedMsg<char> msg(MPI_CHAR);
            ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Receive result message failed.");
                return ret;
            }
            // send it to host controller
            ret = send::sendMessage(msg, HOST_LOCAL_RANK, status.MPI_TAG, g_controller_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forwarding response to host controller failed");
                return ret;
            }
            // free msg
            msg.clear();
        }
        return ret;
    }    

    namespace agent
    {
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
                    // logger::log_success("Stored object", obj.recID);
                }
            } else {
                // empty batch, set flag to stop listening for this dataset
                continueListening = 0;
                // update total object count and capacity
                dataset->totalObjects = dataset->objects.size();
                dataset->objects.shrink_to_fit();
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
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
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
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
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

            // allocate space based on total objects
            dataset->objects.reserve(dataset->totalObjects);
            
            // logger::log_success("Global dataspace:", g_config.datasetOptions.dataspaceMetadata.xMinGlobal, g_config.datasetOptions.dataspaceMetadata.yMinGlobal, g_config.datasetOptions.dataspaceMetadata.xMaxGlobal, g_config.datasetOptions.dataspaceMetadata.yMaxGlobal);
            // listen for dataset batches until an empty batch arrives
            while(listen) {
                // proble blockingly for batch
                ret = probe(PARENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // pull
                switch (status.MPI_TAG) {
                    // case MSG_INSTR_FIN: // todo: maybe this is not needed
                    //     /* stop listening for instructions */
                    //     ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_agent_comm, status);
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
                ret = probe(PARENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                if (ret != DBERR_OK) {
                    goto STOP_LISTENING;
                }
                // pull
                switch (status.MPI_TAG) {
                    // case MSG_INSTR_FIN: // todo: maybe this is not needed
                    //     /* stop listening for instructions */
                    //     ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_agent_comm, status);
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
            DB_STATUS ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_agent_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // perform the corresponding instruction
            switch (status.MPI_TAG) {
                case MSG_INSTR_FIN:
                    // send ACK
                    ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_agent_comm);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to send ACK for termination instruction.");
                        return ret;
                    }
                    return DB_FIN;
                case MSG_INSTR_BATCH_FINISHED:
                    // send ACK
                    ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_agent_comm);
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
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
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
                ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_agent_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Send ACK failed.");
                }
            } else {
                // there was an error, send NACK and propagate error code locally
                DB_STATUS errorCode = ret;
                ret = send::sendResponse(PARENT_RANK, MSG_NACK, g_agent_comm);
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
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
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
            ret = g_config.datasetOptions.addDataset(dataset);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to add dataset to config options.");
                return ret;
            }

            // free message memory
            msg.clear();

            // return dataset ID
            SerializedMsg<int> msgToController(MPI_INT);
            ret = pack::packDatasetIndexes({dataset.metadata.internalID}, msgToController);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed packing indexes to message.");
                return ret;
            }
            ret = comm::send::sendMessage(msgToController, PARENT_RANK, MSG_DATASET_INDEX, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending indexes message to parent controller.");
                return ret;
            }
            // free memory
            msgToController.clear();
            return ret;
        }

        static DB_STATUS handleDatasetInfoMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
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
            // get actual dataset and replace total object count + dataspace info
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(tempDataset.metadata.internalID);
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
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack
            std::vector<int> datasetIndexes;
            ret = unpack::unpackIntegers(msg, datasetIndexes);
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
                        ret = send::sendResponse(PARENT_RANK, MSG_NACK, g_agent_comm);
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
                        ret = send::sendResponse(PARENT_RANK, MSG_NACK, g_agent_comm);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "Send NACK failed.");
                            return ret;
                        }
                        return errorCode;
                    } 
                }
            }

            // send ACK back that all data for this dataset has been received and processed successfully
            ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleBuildIndexMessage(MPI_Status &status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack
            std::vector<int> messageContents;
            ret = unpack::unpackIntegers(msg, messageContents);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack build index contents.");
                return ret;
            }
            
            // free message memory
            msg.clear();

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
                if (g_config.queryMetadata.IntermediateFilter) {
                    // generate APRIL
                    ret = dataset->buildAPRIL();
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to create APRIL");
                        return ret;
                    }
                }
            }

            // send ACK back that the datasets has been loaded successfully
            ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleLoadDatasetMessage(MPI_Status &status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack
            std::vector<int> datasetIndexes;
            ret = unpack::unpackIntegers(msg, datasetIndexes);
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
            ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send ACK failed.");
            }

            return ret;
        }

        static DB_STATUS handleGlobalDataspaceMessage(MPI_Status &status) {
            SerializedMsg<double> msg(MPI_DOUBLE);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
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
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack query
            hec::Query* queryPtr;
            ret = unpack::unpackQuery(msg, &queryPtr);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack query.");
                return ret;
            }

            // free message memory
            msg.clear();

            // setup query result object
            hec::QResultBase* queryResult;
            int res = qresult_factory::createNew(queryPtr->getQueryID(), queryPtr->getQueryType(), queryPtr->getResultType(), &queryResult);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                return DBERR_OBJ_CREATION_FAILED;
            }

            // evaluate query based on index (stored in R dataset)
            ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(queryPtr, queryResult);
            if (ret != DBERR_OK) {
                return ret;
            }

            // pack results to send
            SerializedMsg<char> resultMsg(MPI_CHAR);
            queryResult->serialize(&resultMsg.data, resultMsg.count);

            // send results
            ret = send::sendMessage(resultMsg, PARENT_RANK, MSG_QUERY_RESULT, g_agent_comm);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free message memory
            resultMsg.clear();

            // free query memory
            delete queryPtr;

            // free query result memory
            delete queryResult;

            return ret;
        }

        static DB_STATUS handleQueryBatchMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack query batch
            std::vector<hec::Query*> queryBatch;
            ret = unpack::unpackQueryBatch(msg, &queryBatch);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack query.");
                return ret;
            }

            // logger::log_success("Unpacked", queryBatch.size(), "queries to process.");

            // free message memory
            msg.clear();
            
            // holds all the final batch results
            std::unordered_map<int, hec::QResultBase*> batchResults;
            
            std::vector<std::unordered_map<int, hec::QResultBase*>> threadLocalMaps(MAX_THREADS);
            #pragma omp parallel num_threads(MAX_THREADS)
            {
                int tid = omp_get_thread_num();
                auto& localResults = threadLocalMaps[tid];
                #pragma omp for
                for (int i = 0; i < queryBatch.size(); i++) {
                    hec::QResultBase* queryResult = nullptr;

                    int res = qresult_factory::createNew(queryBatch[i]->getQueryID(), queryBatch[i]->getQueryType(), queryBatch[i]->getResultType(), &queryResult);
                    if (res != 0) {
                        logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                        #pragma omp cancel for
                        ret = DBERR_OBJ_CREATION_FAILED;
                    }

                    DB_STATUS local_ret = g_config.datasetOptions.getDatasetR()->index->evaluateQuery(queryBatch[i], queryResult);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel for
                        ret = local_ret;
                    }

                    localResults[queryBatch[i]->getQueryID()] = queryResult;
                }
            }
            if (ret != DBERR_OK) {
                return ret;
            }
            // After parallel region, merge maps
            for (auto& map : threadLocalMaps) {
                mergeBatchResultMaps(batchResults, map);
            }

            // pack results to send
            SerializedMsg<char> batchResultMsg(MPI_CHAR);
            ret = pack::packBatchResults(batchResults, batchResultMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack batch results.");
                return ret;
            }

            // logger::log_success("Packed results for", batchResults.size(), "queries");

            // send results
            ret = send::sendMessage(batchResultMsg, PARENT_RANK, MSG_QUERY_BATCH_RESULT, g_agent_comm);
            if (ret != DBERR_OK) {
                return ret;
            }

            // free message memory
            batchResultMsg.clear();

            // delete queries
            for (auto &it: queryBatch) {
                delete it;
            }

            // delete query results
            for (auto& [key, value] : batchResults) {
                delete value;
            }
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
                    // logger::log_success("MSG_QUERY");
                    ret = handleQueryMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_QUERY_BATCH:
                    ret = handleQueryBatchMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
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
                ret = probe(PARENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
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

    namespace controller
    {
        namespace forward
        {

            /**
            @brief forwards a message from the Host controller to the Agent and waits for the agent's response
            * which will be returned to the host
            */
            template <typename T> 
            static DB_STATUS forwardAndWaitResponse(SerializedMsg<T> msg, MPI_Status &status) {
                // receive the message
                DB_STATUS ret = recv::receiveMessage(status, msg.type,g_controller_comm, msg);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // forward to local agent
                ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // free memory
                msg.clear();
                // wait for response by the agent that system init went ok
                ret = waitForResponse();
                if (ret != DBERR_OK) {
                    return ret;
                }
                return ret;
            }

            /**
            @brief receives and forwards a message from source to dest
            * 
            * @param sourceRank 
            * @param sourceTag 
            * @param sourceComm 
            * @param destRank 
            * @param destComm 
            * @return DB_STATUS 
            */
            static DB_STATUS forwardInstructionMessage(int sourceRank, int sourceTag, MPI_Comm sourceComm, int destRank, MPI_Comm destComm) {
                MPI_Status status;
                // receive instruction
                DB_STATUS ret = recv::receiveInstructionMessage(sourceRank, sourceTag, sourceComm, status);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_RECV, "Failed forwarding instruction");
                    return DBERR_COMM_RECV;
                }
                // send it
                ret = send::sendInstructionMessage(destRank, status.MPI_TAG, destComm);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_SEND, "Failed forwarding instruction");
                    return DBERR_COMM_SEND;
                }
                return DBERR_OK;
            }

            /** @brief receive a response and forward it elsewhere */
            static DB_STATUS forwardResponse(int sourceRank, int sourceTag, MPI_Comm sourceComm, int destRank, MPI_Comm destComm) {
                if (sourceTag != MSG_ACK && sourceTag != MSG_NACK) {
                    logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Response messages can only be ACK or NACK. Tag:", sourceTag);
                    return DBERR_COMM_INVALID_MSG_TAG;
                }
                
                MPI_Status status;
                // receive response
                DB_STATUS ret = recv::receiveResponse(sourceRank, sourceTag, sourceComm, status);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_RECV, "Failed forwarding response");
                    return DBERR_COMM_RECV;
                }
                // send it
                ret = send::sendResponse(destRank, status.MPI_TAG, destComm);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_SEND, "Failed forwarding response");
                    return DBERR_COMM_SEND;
                }

                return DBERR_OK;
            }

            /**
            @brief Forwards a serialized batch message received through sourceComm to destrank, through destComm
            * The message has to be already probed (metadata stored in status)
            * 
            * @param status 
            * @return DB_STATUS 
            */
            static DB_STATUS forwardBatchMessage(MPI_Comm sourceComm, int destRank, MPI_Comm destComm, MPI_Status &status, int &continueListening) {
                DB_STATUS ret = DBERR_OK;
                SerializedMsg<char> msg(MPI_CHAR);
                // receive batch message
                ret = recv::receiveMessage(status, msg.type, sourceComm, msg);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed pulling serialized message");
                    return ret;
                }
                
                // send the packs to the agent
                ret = send::sendMessage(msg, destRank, status.MPI_TAG, destComm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Forwarding geometry batch failed");
                    return ret;
                }
                // get batch object count
                size_t objectCount = getBatchObjectCountFromMessage(msg);
                if (objectCount == 0) {
                    continueListening = 0;
                }
                // free memory
                msg.clear();
                return ret;
            }

            static DB_STATUS forwardAPRILMetadataMessage(MPI_Comm sourceComm, int destRank, MPI_Comm destComm, MPI_Status &status) {
                SerializedMsg<int> msg(MPI_INT);
                // receive the message
                DB_STATUS ret = recv::receiveMessage(status, msg.type, sourceComm, msg);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // forward to local agent
                ret = send::sendMessage(msg, destRank, status.MPI_TAG, destComm);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // free memory
                msg.clear();
                // wait for response by the agent that all is well for the APRIL creation
                ret = waitForResponse();
                if (ret != DBERR_OK) {
                    return ret;
                }
                return ret;
            }

            static DB_STATUS forwardQueryResultsMessage(MPI_Comm sourceComm, int destRank, MPI_Comm destComm, MPI_Status &status) {
                SerializedMsg<int> msg(MPI_INT);
                // receive the message
                DB_STATUS ret = recv::receiveMessage(status, msg.type, sourceComm, msg);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // forward
                ret = send::sendMessage(msg, destRank, status.MPI_TAG, destComm);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // free memory
                msg.clear();
                return ret;
            }

            /**
             * Waits for the results of the query that is being processed
             * When they come, forward them to the host controller.
             */
            static DB_STATUS waitForResults() {
                MPI_Status status;
                // wait for results by the agent
                DB_STATUS ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                if (status.MPI_TAG == MSG_QUERY_RESULT) {
                    ret = forwardQueryResultsMessage(g_agent_comm, HOST_LOCAL_RANK,g_controller_comm, status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Forwarding result to host controller failed");
                        return ret;
                    }
                } else if (status.MPI_TAG == MSG_NACK) {
                    // NACK, an error occurred
                    // receive response from agent
                    ret = recv::receiveResponse(AGENT_RANK, status.MPI_TAG, g_agent_comm, status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    // forward response to host controller
                    ret = send::sendResponse(HOST_LOCAL_RANK, status.MPI_TAG,g_controller_comm);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Forwarding response to host controller failed");
                        return ret;
                    }
                    // check response, if its NACK then terminate
                    if (status.MPI_TAG == MSG_NACK) {
                        logger::log_error(DBERR_COMM_RECEIVED_NACK, "Received NACK by agent");
                        return DBERR_COMM_RECEIVED_NACK;
                    }
                } else {
                    // error, unexpected message, send a NACK to the host 
                    ret = send::sendResponse(HOST_LOCAL_RANK, MSG_NACK,g_controller_comm);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Sending NACK to host controller failed");
                        return ret;
                    }
                }
                return ret;
            }

            static DB_STATUS forwardQueryMetadataMessage(MPI_Comm sourceComm, int destRank, MPI_Comm destComm, MPI_Status &status) {
                SerializedMsg<int> msg(MPI_INT);
                // receive the message
                DB_STATUS ret = recv::receiveMessage(status, msg.type, sourceComm, msg);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // forward to local agent
                ret = send::sendMessage(msg, destRank, status.MPI_TAG, destComm);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // free memory
                msg.clear();
                // wait for the results by the agent 
                ret = waitForResults();
                if (ret != DBERR_OK) {
                    return ret;
                }
                return ret;
            }
        }

        DB_STATUS serializeAndSendGeometryBatch(Batch* batch) {
            DB_STATUS ret = DBERR_OK;
            SerializedMsg<char> msg(MPI_CHAR);
            // check if batch is valid
            if (!batch->isValid()) {
                logger::log_error(DBERR_BATCH_FAILED, "Batch is invalid, check its destRank, tag and comm");
                return DBERR_BATCH_FAILED;
            }
            // logger::log_success("Sending batch of size", batch->objectCount);
            // serialize (@todo: add try/catch for segfauls, mem access etc...)   
            ret = batch->serialize(&msg.data, msg.count);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_BATCH_FAILED, "Batch serialization failed");
                return DBERR_BATCH_FAILED;
            }
            // send batch message
            ret = comm::send::sendMessage(msg, batch->destRank, batch->tag, *batch->comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending serialized geometry batch failed");
                return ret;
            }
            // free memory
            msg.clear();
            // logger::log_task("Sent batch to node", batch->destRank);
            return ret;
        }

        DB_STATUS serializeAndSendGeometry(Shape *shape, int destRank, MPI_Comm &comm) {
            DB_STATUS ret = DBERR_OK;
            SerializedMsg<char> msg(MPI_CHAR);

            if (shape == nullptr) {
                // send empty
                msg.count = 0;
                ret = comm::send::sendMessage(msg, destRank, MSG_SINGLE_POLYGON, comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Sending serialized geometry batch failed");
                    return ret;
                }
            } else {
                // serialize
                ret = pack::packShape(shape, msg);
                if (ret != DBERR_OK) {
                    return ret;
                }

                // send batch message
                ret = comm::send::sendMessage(msg, destRank, MSG_SINGLE_POLYGON, comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Sending serialized geometry batch failed");
                    return ret;
                }
                // free memory
                msg.clear();
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

        static DB_STATUS listenForDatasetPartitioning() {
            DB_STATUS ret = DBERR_OK;
            MPI_Status status;
            int listen = 1;
            while(listen) {
                // proble blockingly
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG, g_controller_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // a message has been probed, check its tag
                switch (status.MPI_TAG) {
                    // case MSG_INSTR_FIN:
                    //     /* stop listening  */
                    //     ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_agent_comm, status);
                    //     if (ret == DBERR_OK) {
                    //         // break the listening loop
                    //         return DB_FIN;
                    //     }
                    //     return ret;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                    case MSG_SINGLE_POLYGON:
                        /* batch geometry message */
                        ret = forward::forwardBatchMessage(g_controller_comm, AGENT_RANK, g_agent_comm, status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        } 
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "After the dataset metadata pack, only batch messages are expected");
                        return DBERR_COMM_WRONG_MESSAGE_ORDER;
                }
            }
            return ret;
        }

        static DB_STATUS handlePrepareDatasetMessage(MPI_Status status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // free message memory
            msg.clear();
            // wait for response from agent with the dataset ID
            SerializedMsg<int> msgFromAgent(MPI_INT);
            ret = probe(AGENT_RANK, MSG_DATASET_INDEX, g_agent_comm, status);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Probing failed agent response.");
                return ret;
            }
            ret = recv::receiveMessage(status, msgFromAgent.type, g_agent_comm, msgFromAgent);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Receiving failed for agent response.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleDatasetInfoMessage(MPI_Status status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // free message memory
            msg.clear();
            return ret;
        }

        static DB_STATUS handlePartitionDatasetMessage(MPI_Status &status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // free message memory
            msg.clear();

            // start listening for partitioning messages (batches)
            ret = listenForDatasetPartitioning();
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // wait for response by agent and forward it to the host controller when it arrives
            ret = waitForResponse();
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Partitioning failed.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleBuildIndexMessage(MPI_Status &status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // free message memory
            msg.clear();

            // wait for response by agent and forward it to the host controller when it arrives
            ret = waitForResponse();
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Build index failed.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleLoadDatasetMessage(MPI_Status &status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // free message memory
            msg.clear();

            // wait for response by agent and forward it to the host controller when it arrives
            ret = waitForResponse();
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Partitioning failed.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleGlobalDataspaceMessage(MPI_Status &status) {
            SerializedMsg<double> msg(MPI_DOUBLE);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // free message memory
            msg.clear();

            // no ACK, return
            return ret;
        }

        static DB_STATUS handleQueryMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_controller_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding query message to agent");
                return ret;
            }
            // free message memory
            msg.clear();

            // wait for response by agent and forward it to the host controller when it arrives
            ret = waitForResults();
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while waiting for query results.");
                return ret;
            }
            return ret;
        }

        /**
        @brief pulls incoming message sent by the host controller 
         * (the one probed last, whose metadata is stored in the status parameter)
         * Based on the tag of the message, it performs the corresponding request.
         * The controller almost always forwards the message to its local agent.
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            
            // check message tag
            switch (status.MPI_TAG) {
                case MSG_INSTR_FIN:
                    /* terminate */
                    ret = forward::forwardInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG, g_controller_comm, AGENT_RANK, g_agent_comm);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    /* wait for ACK from agent and forward to host controller*/
                    ret = forward::forwardResponse(AGENT_RANK, MSG_ACK, g_agent_comm, HOST_LOCAL_RANK, g_controller_comm);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    return DB_FIN;
                case MSG_INSTR_BATCH_FINISHED:
                    /* batch is finished, forward */
                    ret = forward::forwardInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG, g_controller_comm, AGENT_RANK, g_agent_comm);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    /* wait for ACK from agent and forward to host controller*/
                    ret = forward::forwardResponse(AGENT_RANK, MSG_ACK, g_agent_comm, HOST_LOCAL_RANK, g_controller_comm);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_PREPARE_DATASET:
                    ret = handlePrepareDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Handling dataset metadata message failed.");
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
                    ret = handlePartitionDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling partition dataset message.");
                        return ret;
                    }
                    break;
                case MSG_LOAD_DATASET:
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
                case MSG_GLOBAL_DATASPACE:
                    ret = handleGlobalDataspaceMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling load dataset message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY:
                case MSG_QUERY_BATCH:
                    ret = handleQueryMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling query message.");
                        return ret;
                    }
                    break;                    
                case MSG_SYS_INFO:
                    /* char messages */
                    {
                        SerializedMsg<char> msg(MPI_CHAR);
                        ret = forward::forwardAndWaitResponse(msg, status);
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        break;
                    }
                default:
                    // unkown instruction
                    logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Didn't expect message with tag", status.MPI_TAG);
                    return DBERR_COMM_UNKNOWN_INSTR;
            }

            return ret;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            int messageFound = 0;
            while(true){
                /* controller probes non-blockingly for messages either by the agent or by other controllers */

                // check whether the agent has sent a message
                ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status, messageFound);
                if (ret != DBERR_OK){
                    return ret;
                }
                // if (messageFound) {
                    
                // }

                // check for HOST CONTROLLER has sent a message
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG, g_controller_comm, status, messageFound);
                if (ret != DBERR_OK){
                    return ret;
                }
                if (messageFound) {
                    // pull the message and perform its request
                    ret = controller::pullIncoming(status);
                    // true only if the termination instruction has been received
                    if (ret == DB_FIN) {
                        goto STOP_LISTENING;
                    } else if (ret != DBERR_OK) {
                        return ret;
                    }
                }
            }
STOP_LISTENING:
            return DBERR_OK;
        }


    }

    /**
     * Host controller only
     */
    namespace host
    {

        namespace forward 
        {
            /** @brief received an already probed message from the driver and forwards it to the other controllers and the local agent */
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
                    for (int i = 0; i < g_world_size; i++) {
                        if (i != HOST_LOCAL_RANK) {
                            local_ret = send::sendInstructionMessage(i, status.MPI_TAG, g_controller_comm);
                            if (local_ret != DBERR_OK) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_COMM_SEND, "Failed forwarding instruction to controller", i);
                                ret = local_ret;
                            }
                        } else {
                            local_ret = send::sendInstructionMessage(AGENT_RANK, sourceTag, g_agent_comm);
                            if (local_ret != DBERR_OK) {
                                logger::log_error(DBERR_COMM_SEND, "Failed forwarding instruction to agent");
                                ret = DBERR_COMM_SEND;
                            }
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
                for (int i=0; i<g_world_size; i++) {
                    // probe for responses
                    if (i == 0) {
                        // wait for agent's response
                        local_ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // receive response
                        local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_agent_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    } else {
                        // wait for workers' responses (each thread with id X receives from worker with rank X)
                        local_ret = probe(i, MPI_ANY_TAG, g_controller_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // receive response
                        local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_controller_comm, status);
                        if (local_ret != DBERR_OK) {
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                    // check response
                    if (status.MPI_TAG != MSG_ACK) {
                        #pragma omp cancel for
                        logger::log_error(DBERR_OPERATION_FAILED, "Node", status.MPI_SOURCE, "finished with error");
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
            
            ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
            if (ret != DBERR_OK) {
                return ret;
            }

            // deserialize message
            DatasetMetadata datasetMetadata;
            datasetMetadata.deserialize(msg.data, msg.count);
            // add the dataset structure to the config
            Dataset dataset(datasetMetadata);
            ret = g_config.datasetOptions.addDataset(dataset);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to add dataset to config options.");
                return ret;
            }

            // free memory
            msg.clear();

            // wait for response from agent with the dataset ID
            SerializedMsg<int> msgToDriver(MPI_INT);
            // probe
            ret = probe(AGENT_RANK, MSG_DATASET_INDEX, g_agent_comm, status);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Probing failed agent response.");
                return ret;
            }
            // receive
            ret = recv::receiveMessage(status, msgToDriver.type, g_agent_comm, msgToDriver);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Receiving failed for agent response.");
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

        static DB_STATUS handlePartitionDatasetMessage(MPI_Status &status) {
            SerializedMsg<int> msg(MPI_INT);
            std::vector<int> datasetIndexes;
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // unpack the indexes of the datasets to partition
            ret = unpack::unpackIntegers(msg, datasetIndexes);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack dataset indexes.");
                return ret;
            }

            // free memory
            msg.clear();

            // double check if bounds are set for all datasets
            for (auto &index: datasetIndexes) {
                Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(index);
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
                Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(index);
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
                SerializedMsg<int> signal(MPI_INT);
                ret = pack::packIntegers(signal, index);
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
                // logger::log_task("Partitioning dataset", dataset->metadata.internalID);
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
            SerializedMsg<int> msg(MPI_INT);
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
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            // unpack the indexes of the datasets to load
            std::vector<int> datasetIndexes;
            ret = unpack::unpackIntegers(msg, datasetIndexes);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to unpack dataset indexes.");
                return ret;
            }

            // free memory
            msg.clear();

            // notify the controllers and the local agent to load each dataset in the message
            for (auto &index : datasetIndexes) {
                // signal the begining of the partitioning
                SerializedMsg<int> signal(MPI_INT);
                // pack
                ret = pack::packIntegers(signal, index);
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
        
        static DB_STATUS gatherJoinResults(hec::QResultBase* totalResults) {
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
                            hec::QResultBase* localResult;
                            int res = qresult_factory::createNew(totalResults->getQueryID(), totalResults->getQueryType(), totalResults->getResultType(), &localResult);
                            if (res != 0) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                                ret = DBERR_OBJ_CREATION_FAILED;
                            }
                            localResult->deserialize(msg.data, msg.count);
                            // add results
                            local_ret = mergeResultObjects(totalResults, localResult);
                            if (local_ret != DBERR_OK) {
                                logger::log_error(ret, "Merging query results failed.");
                                #pragma omp cancel for
                                ret = local_ret;
                            }
                            // free memory
                            delete localResult;
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
                            hec::QResultBase* localResult = nullptr;
                            int res = qresult_factory::createNew(totalResults->getQueryID(), totalResults->getQueryType(), totalResults->getResultType(), &localResult);
                            if (res != 0) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result objects.");
                                ret = DBERR_OBJ_CREATION_FAILED;
                            }
                            localResult->deserialize(msg.data, msg.count);
                            // add results
                            local_ret = mergeResultObjects(totalResults, localResult);
                            if (local_ret != DBERR_OK) {
                                logger::log_error(ret, "Merging query results failed.");
                                #pragma omp cancel for
                                ret = local_ret;
                            }
                            // free memory
                            delete localResult;
                        }
                    }
                }
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
                            // logger::log_success("Unpacked results for", batchResultsMap.size(), "queries");
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
                            // logger::log_success("Unpacked results for", batchResultsMap.size(), "queries");
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

        static DB_STATUS handleQueryMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // unpack query
            hec::Query* query;
            ret = unpack::unpackQuery(msg, &query);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            switch (query->getQueryType()) {
                case hec::Q_RANGE:
                {
                    // send query only to the responsible nodes
                    hec::RangeQuery* rangeQuery = dynamic_cast<hec::RangeQuery*>(query);

                    return DBERR_FEATURE_UNSUPPORTED;
                }
                    break;
                case hec::Q_INTERSECTION_JOIN:
                case hec::Q_INSIDE_JOIN:
                case hec::Q_DISJOINT_JOIN:
                case hec::Q_EQUAL_JOIN:
                case hec::Q_MEET_JOIN:
                case hec::Q_CONTAINS_JOIN:
                case hec::Q_COVERS_JOIN:
                case hec::Q_COVERED_BY_JOIN:
                case hec::Q_FIND_RELATION_JOIN:
                    // broadcast join query to every worker
                    ret = broadcast::broadcastMessage(msg, status.MPI_TAG);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", query->getQueryType());
                    return DBERR_QUERY_INVALID_TYPE;
            }

            // free memory
            msg.clear();

            // create query result object
            hec::QResultBase* totalResults;
            // peek query and result types
            int res = qresult_factory::createNew(query->getQueryID(), query->getQueryType(), query->getResultType(), &totalResults);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                return DBERR_OBJ_CREATION_FAILED;
            }

            // wait for result
            ret = gatherJoinResults(totalResults);
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
            delete totalResults;
            return ret;
        }

        static DB_STATUS handleQueryBatchMessageParallel(MPI_Status &status) {
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
                ret = broadcast::broadcastMessage(batchMsg, MSG_QUERY_BATCH);
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

        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            // check message tag
            switch (status.MPI_TAG) {
                case MSG_INSTR_FIN:
                    /* terminate everything */
                    // forward instruction
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
                    // logger::log_success("MSG_PREPARE_DATASET");
                    ret = handlePrepareDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling dataset metadata message.");
                        return ret;
                    }
                    break;
                case MSG_PARTITION_DATASET:
                    /** initiate partitioning */
                    // logger::log_success("MSG_PARTITION_DATASET");
                    ret = handlePartitionDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling partition dataset message.");
                        return ret;
                    }
                    break;
                case MSG_LOAD_DATASET:
                    /** initiate dataset loading */
                    // logger::log_success("MSG_LOAD_DATASET");
                    ret = handleLoadDatasetMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling load dataset message.");
                        return ret;
                    }
                    break;
                case MSG_BUILD_INDEX:
                    /** initiate index building */
                    // logger::log_success("MSG_BUILD_INDEX");
                    ret = handleBuildIndexMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling load dataset message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY:
                    /** Initiate query */
                    // logger::log_success("MSG_QUERY");
                    ret = handleQueryMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling query message.");
                        return ret;
                    }
                    break;
                case MSG_QUERY_BATCH:
                    /** Initiate query */
                    // logger::log_success("MSG_QUERY_BATCH");
                    ret = handleQueryBatchMessageParallel(status);
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

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            int messageFound = 0;
            while(true){
                // check for DRIVER messages  
                // @todo: make wait-probe instead of nowait?
                ret = probe(DRIVER_GLOBAL_RANK, MPI_ANY_TAG, g_global_intra_comm, status, messageFound);
                if (ret != DBERR_OK){
                    return ret;
                }
                
                if (messageFound) {
                    // driver has sent a message, propagate to everyone
                    ret = host::pullIncoming(status);
                    // true only if the termination instruction has been received
                    if (ret == DB_FIN) {
                        goto STOP_LISTENING;
                    } else if (ret != DBERR_OK) {
                        return ret;
                    }
                }
            }
STOP_LISTENING:
            return DBERR_OK;
        }
    }
}