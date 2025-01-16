#include "env/comm.h"

namespace comm 
{
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

    /**
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

    namespace agent
    {
        /** @brief Deserializes a serialized batch message and stores its data on disk and in the proper containers in-memory. 
         * @param[in] buffer The serialized batch message
         * @param[in] bufferSize The message's size
         * @param[in] outFile A file pointer pointing to an already open partition file on disk. Its position should be after the object count/dataset metadata position.
         * @param[in] dataset The dataset getting partitioned
         * @param[out] continueListening If the message is empty (0 objects aster serialization), then it signals the end of the partitioning for the dataset.
        */
        static DB_STATUS deserializeBatchMessageAndStore(char *buffer, int bufferSize, FILE* outFile, Dataset *dataset, int &continueListening) {
            DB_STATUS ret = DBERR_OK;
            Batch batch;
            // deserialize the batch
            batch.deserialize(buffer, bufferSize);
            // calculate two layer classes for each object, in each partition
            ret = partitioning::calculateTwoLayerClasses(batch);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Calculating two layer index classes failed for batch.");
                return ret;
            }
            // logger::log_success("Received batch with", batch.objectCount, "objects");
            if (batch.objectCount > 0) {
                // do stuff
                ret = storage::writer::partitionFile::appendBatchToPartitionFile(outFile, &batch, dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Failed when writing batch to partition file");
                    return DBERR_DISK_WRITE_FAILED;
                }
            } else {
                // empty batch, set flag to stop listening for this dataset
                continueListening = 0;
                // and write total objects in the begining of the partitioned file
                ret = storage::writer::updateObjectCountInFile(outFile, dataset->totalObjects);
                // logger::log_success("Saved", dataset->totalObjects,"total objects.");
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Failed when updating partition file object count");
                    return DBERR_DISK_WRITE_FAILED;
                }
            }

            return ret;
        }

        static DB_STATUS pullSerializedMessageAndHandle(MPI_Status status, FILE* outFile, Dataset *dataset, int &continueListening) {
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
                    ret = deserializeBatchMessageAndStore(msg.data, msg.count, outFile, dataset, continueListening);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Invalid message tag");
                    break;
            }
            // free memory
            free(msg.data);
            return ret;
        }

        static DB_STATUS generateDatasetFromMessage(SerializedMsg<char> &datasetMetadataMsg, Dataset &dataset) {
            // deserialize message
            dataset.deserialize(datasetMetadataMsg.data, datasetMetadataMsg.count);
            // generate partition filepath
            DB_STATUS ret = storage::generatePartitionFilePath(dataset);
            if (ret != DBERR_OK) {
                return ret;
            }
            return DBERR_OK;
        }

        static DB_STATUS listenForDatasetPartitioning(MPI_Status status) {
            Dataset dataset;
            FILE* outFile;
            SerializedMsg<char> datasetMetadataMsg(MPI_CHAR);
            int listen = 1;
            size_t dummy = 0;

            // proble blockingly for the dataset metadata
            DB_STATUS ret = probe(PARENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // verify that it is the proper message
            if (status.MPI_TAG != MSG_DATASET_METADATA) {
                logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Partitioning: Expected dataset metadata message but received message with tag", status.MPI_TAG);
                return DBERR_COMM_WRONG_MESSAGE_ORDER;
            }
            // retrieve dataset metadata pack
            ret = recv::receiveMessage(status, datasetMetadataMsg.type, g_agent_comm, datasetMetadataMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling the dataset metadata message");
                goto STOP_LISTENING;
            }
            // create dataset from the received message
            ret = generateDatasetFromMessage(datasetMetadataMsg, dataset);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed creating the dataset from the dataset metadata message");
                goto STOP_LISTENING;
            }
            // update the grids' dataspace metadata in the partitioning object 
            g_config.partitioningMethod->setDistGridDataspace(dataset.metadata.dataspaceMetadata);
            g_config.partitioningMethod->setPartGridDataspace(dataset.metadata.dataspaceMetadata);

            // free memory
            free(datasetMetadataMsg.data);
            
            // open the partition data file
            outFile = fopen(dataset.metadata.path.c_str(), "wb+");
            if (outFile == NULL) {
                logger::log_error(DBERR_MISSING_FILE, "Couldnt open dataset file:", dataset.metadata.path);
                return DBERR_MISSING_FILE;
            }

            // write a dummy value for the total object count in the very begining of the file
            // it will be corrected when the batches are finished
            fwrite(&dummy, sizeof(size_t), 1, outFile);

            // write the dataset metadata to the partition file
            ret = storage::writer::partitionFile::appendDatasetMetadataToPartitionFile(outFile, &dataset);
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
                    case MSG_INSTR_FIN: // todo: maybe this is not needed
                        /* stop listening for instructions */
                        ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_agent_comm, status);
                        if (ret == DBERR_OK) {
                            // break the listening loop
                            return DB_FIN;
                        }
                        goto STOP_LISTENING;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = pullSerializedMessageAndHandle(status, outFile, &dataset, listen);
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
            // close partition file
            if (outFile == NULL) {
                fclose(outFile);
            }
            // respond
            if (ret == DBERR_OK) {
                // send ACK back that all data for this dataset has been received and processed successfully
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
                return errorCode;
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
                    return DB_FIN;
                case MSG_INSTR_PARTITIONING_INIT:
                    /* begin dataset partitioning */
                    ret = listenForDatasetPartitioning(status);
                    if (ret != DBERR_OK) {
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
            // printf("Partitioning metadata set: Type %d, dPPD %d, pPPD %d\n", g_config.partitioningMethod->getType(), g_config.partitioningMethod->getDistributionPPD(), g_config.partitioningMethod->getPartitioningPPD());
            // verify local directories
            ret = configurer::verifySystemDirectories();
            if (ret != DBERR_OK) {
                // error
                logger::log_error(ret, "Failed while verifying system directories.");
                return ret ;
            }
            // free memory
            free(msg.data);
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

        static DB_STATUS handleAPRILMetadataMessage(MPI_Status status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack metadata and store
            ret = unpack::unpackAPRILMetadata(msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // free memory
            free(msg.data);

            // create the APRIL approximations for the dataset(s)
            for (auto& it: g_config.datasetOptions.datasets) {
                // ret = APRIL::generation::disk::init(it.second);
                ret = APRIL::generation::memory::init(it.second);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to generate APRIL for dataset", it.second.metadata.internalID);
                }
            }
            if (ret == DBERR_OK) {
                // send ACK back that all data for this dataset has been received and processed successfully
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
                return errorCode;
            }
            return ret;
        }

        static DB_STATUS respondWithQueryResults(QueryOutput &queryOutput) {
            // serialize results
            SerializedMsg<int> msg(MPI_INT);
            DB_STATUS ret = pack::packQueryResults(msg, queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Serializing query output failed");
                return ret;
            }
            // send to local controller
            ret = send::sendMessage(msg, PARENT_RANK, MSG_QUERY_RESULT, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Send query results failed");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleQueryMetadataMessage(MPI_Status status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // unpack metadata and store
            ret = unpack::unpackQueryMetadata(msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // free memory
            free(msg.data);
            QueryOutput queryOutput;
            // execute
            ret = twolayer::processQuery(queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Processing query failed.");
                return ret;
            }
            // send the result to the local controller
            ret = respondWithQueryResults(queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Responding with query results failed.");
                return ret;
            }
            return ret;
        }

        static DB_STATUS handleLoadDatasetsMessage(MPI_Status status) {
            SerializedMsg<char> msg(MPI_CHAR);
            std::vector<std::string> nicknames;
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                goto EXIT_SAFELY;
            }
            {
                // message received
                // create empty dataset;
                Dataset dataset;
                DatasetIndex datasetIndex;
                // unpack metadata and store
                ret = unpack::unpackDatasetLoadMsg(msg, dataset, datasetIndex);
                if (ret != DBERR_OK) {
                    goto EXIT_SAFELY;
                }
                // free memory
                free(msg.data);
                // generate partition file path from dataset nickname
                ret = storage::generatePartitionFilePath(dataset);
                if (ret != DBERR_OK) {
                    goto EXIT_SAFELY;
                }
                // add the EMPTY dataset to the configuration
                ret = g_config.datasetOptions.addDataset(datasetIndex, dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to add dataset to configuration. Dataset nickname:", dataset.metadata.internalID, "dataset index:", datasetIndex);
                    goto EXIT_SAFELY;
                }
                // get the dataset's reference
                Dataset *datasetRef;
                ret = g_config.datasetOptions.getDatasetByIdx(datasetIndex, &datasetRef);
                if (datasetRef == nullptr) {
                    return DBERR_NULL_PTR_EXCEPTION;
                }
                // load partition file (and create MBRs)
                ret = storage::reader::partitionFile::loadDatasetComplete(datasetRef);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Failed loading partition file MBRs");
                    goto EXIT_SAFELY;
                }  
                // // dont forget to update the dataspace metadata of the configuration
                // g_config.datasetOptions.updateDataspace();
                // // verification prints
                // if (datasetIndex == DATASET_R) {
                //     printf("Loaded dataset R %s:\n", g_config.datasetOptions.getDatasetR()->nickname.c_str());
                //     // g_config.datasetOptions.getDatasetR()->printObjectsGeometries();
                //     // g_config.datasetOptions.getDatasetR()->printObjectsPartitions();
                //     g_config.datasetOptions.getDatasetR()->printPartitions();
                // } else if (datasetIndex == DATASET_S) {
                //     // printf("Loaded dataset S %s. Objects:\n", g_config.datasetOptions.getDatasetS()->nickname.c_str());
                //     // g_config.datasetOptions.getDatasetS()->printObjectsGeometries();
                //     // g_config.datasetOptions.getDatasetS()->printObjectsPartitions();
                // }
            }
EXIT_SAFELY:
            // respond
            if (ret == DBERR_OK) {
                // send ACK back that all data for this dataset has been received and processed successfully
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
                return errorCode;
            }
            return ret;
        }

        static DB_STATUS handleLoadAPRILMessage(MPI_Status status) {
            SerializedMsg<int> msg(MPI_INT);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                goto EXIT_SAFELY;
            }
            // unpack metadata and store
            ret = unpack::unpackAPRILMetadata(msg);
            if (ret != DBERR_OK) {
                goto EXIT_SAFELY;
            }
            // free memory
            free(msg.data);
            
            // load APRIL for each dataset
            for (auto &it: g_config.datasetOptions.datasets) {
                // logger::log_task("APRIL metadata: N =", dataset.aprilConfig.getN(), "compression = ", dataset.aprilConfig.compression, "partitions = ", dataset.aprilConfig.partitions);
                // set approximation type for dataset
                it.second.approxType = AT_APRIL;
                // generate APRIL filepaths
                ret = storage::generateAPRILFilePath(it.second);
                if (ret != DBERR_OK) {
                    goto EXIT_SAFELY;
                }
                // load
                ret = APRIL::reader::loadAPRIL(it.second);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Failed loading APRIL");
                    goto EXIT_SAFELY;
                }
            }
EXIT_SAFELY:
            // respond
            if (ret == DBERR_OK) {
                // send ACK back that all data for this dataset has been received and processed successfully
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
                return errorCode;
            }

            return ret;
        }

        static DB_STATUS handleDatasetMetadataMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_agent_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // this should not happen in the controller
            // deserialize message
            DatasetMetadata datasetMetadata;
            datasetMetadata.deserialize(msg.data, msg.count);
            // add the dataset structure to the config
            Dataset dataset(datasetMetadata);
            ret = g_config.datasetOptions.addDataset(datasetMetadata.internalID, dataset);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to add dataset to config options.");
                return ret;
            }

            // free message memory
            free(msg.data);

            // return dataset ID
            SerializedMsg<int> msgToController(MPI_INT);
            ret = pack::packDatasetIndexes({datasetMetadata.internalID}, msgToController);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed packing indexes to message.");
                return ret;
            }
            ret = comm::send::sendMessage(msgToController, PARENT_RANK, MSG_DATASET_INDEX, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed sending indexes message to parent controller.");
                return ret;
            }
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
                    ret = handleSysMetadataMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_DATASET_METADATA:
                    ret = handleDatasetMetadataMessage(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                // case MSG_APRIL_CREATE:
                //     /* APRIL metadata message */
                //     ret = handleAPRILMetadataMessage(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }
                //     break;
                // case MSG_LOAD_DATASET:
                //     /* load datasets */
                //     ret = handleLoadDatasetsMessage(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }
                //     break;
                // case MSG_LOAD_APRIL:
                //     /* load datasets */
                //     ret = handleLoadAPRILMessage(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }
                //     break;
                // case MSG_QUERY_INIT:
                //     /* query metadata */
                //     ret = handleQueryMetadataMessage(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }
                //     break;
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
                free(msg.data);
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
                free(msg.data);
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
                free(msg.data);
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
                free(msg.data);
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
                free(msg.data);
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
            // serialize (todo: add try/catch for segfauls, mem access etc...)   
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
            // logger::log_task("sent batch");
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
            free(msgPack.data);

            return ret;
        }

        /**
        @brief Pulls and forwards a dataset metadata message to the local agent
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS forwardDatasetMetadataToAgent(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            int tag = status.MPI_TAG;
            SerializedMsg<char> datasetMetadataMsg(MPI_CHAR);
            // receive dataset metadata message
            ret = recv::receiveMessage(status, datasetMetadataMsg.type,g_controller_comm, datasetMetadataMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling the dataset metadata message");
                return ret;
            }
            // send
            ret = send::sendDatasetMetadataMessage(datasetMetadataMsg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 
            // free temp memory
            free(datasetMetadataMsg.data);
            return ret;
        }

        static DB_STATUS listenForDatasetPartitioning(MPI_Status status) {
            // proble blockingly for the dataset metadata
            DB_STATUS ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG,g_controller_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // verify that it is the proper message
            if (status.MPI_TAG != MSG_DATASET_METADATA) {
                logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Partitioning: Expected dataset metadata message but received message with tag", status.MPI_TAG);
                return DBERR_COMM_WRONG_MESSAGE_ORDER;
            }
            // forward dataset metadata to agent
            int listen = 1;
            ret = forwardDatasetMetadataToAgent(status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // next, listen for the metadataPacks and coordPacks explicitly
            while(listen) {
                // proble blockingly
                ret = probe(HOST_LOCAL_RANK, MPI_ANY_TAG,g_controller_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // a message has been probed, check its tag
                switch (status.MPI_TAG) {
                    case MSG_INSTR_FIN:
                        /* stop listening for instructions */
                        ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_agent_comm, status);
                        if (ret == DBERR_OK) {
                            // break the listening loop
                            return DB_FIN;
                        }
                        return ret;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
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

            // wait for response by the agent that all is well for this dataset partitioning
            ret = waitForResponse();
            if (ret != DBERR_OK) {
                return ret;
            }
            // todo: maybe do something on nack?
            return ret;
        }

        static DB_STATUS handleDatasetMetadataMessage(MPI_Status status) {
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
            free(msg.data);
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
                    ret = forward::forwardInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG,g_controller_comm, AGENT_RANK, g_agent_comm);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    return DB_FIN;
                case MSG_DATASET_METADATA:
                    ret = handleDatasetMetadataMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Handling dataset metadata message failed.");
                        return ret;
                    }
                    break;
                // case MSG_INSTR_PARTITIONING_INIT:
                //     /* init partitioning */
                //     ret = forward::forwardInstructionMessage(HOST_LOCAL_RANK, status.MPI_TAG,g_controller_comm, AGENT_RANK, g_agent_comm);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }
                //     // start listening for partitioning messages
                //     ret = listenForDatasetPartitioning(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }
                //     break;
                // case MSG_LOAD_DATASET:
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
                // case MSG_LOAD_APRIL:
                // case MSG_APRIL_CREATE:
                //     /* int messages, wait for response */
                //     {
                //         SerializedMsg<int> msg;
                //         ret = forward::forwardAndWaitResponse(msg, status);
                //         if (ret != DBERR_OK) {
                //             return ret;
                //         }
                //         break;
                //     }
                // case MSG_QUERY_INIT:
                //     /* int messages, wait for results */
                //     ret = forward::forwardQueryMetadataMessage(g_controller_comm, AGENT_RANK, g_agent_comm, status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     }  
                //     break;
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
            /** @brief Receives and forwards a message to all controlers and the local agent */
            template <typename T> 
            static DB_STATUS forwardMessage(SerializedMsg<T> msg, MPI_Status &status) {
                // receive the message
                DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // forward to local agent
                ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed forwarding message to agent");
                    return ret;
                }
                // send to controllers
                #pragma omp parallel
                {
                    DB_STATUS local_ret = DBERR_OK;
                    #pragma omp for
                    for (int i=0; i<g_world_size; i++) {
                        if (i != HOST_LOCAL_RANK) {
                            // send to controllers
                            local_ret = send::sendMessage(msg, i, status.MPI_TAG, g_controller_comm);
                            if (ret != DBERR_OK) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_COMM_SEND, "Failed forwarding message to controller", i);
                                ret = local_ret;
                            }
                        }
                    }
                }
                // free memory
                free(msg.data);
                
                return ret;
            }

            /** @brief received an already probed message from the driver and forwards it to the other controllers and the local agent */
            static DB_STATUS forwardInstructionMessage(int sourceTag) {
                MPI_Status status;
                // receive the msg
                DB_STATUS ret = recv::receiveInstructionMessage(DRIVER_GLOBAL_RANK, sourceTag, g_global_intra_comm, status);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_RECV, "Failed receiving instruction from driver");
                    return DBERR_COMM_RECV;
                }
                // send to agent
                ret = send::sendInstructionMessage(AGENT_RANK, sourceTag, g_agent_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_COMM_SEND, "Failed forwarding instruction to agent");
                    return DBERR_COMM_SEND;
                }
                // send to controllers
                #pragma omp parallel
                {
                    DB_STATUS local_ret = DBERR_OK;
                    #pragma omp for
                    for (int i=0; i<g_world_size; i++) {
                        if (i != HOST_LOCAL_RANK) {
                            // send to controllers
                            local_ret = send::sendInstructionMessage(i, status.MPI_TAG, g_controller_comm);
                            if (ret != DBERR_OK) {
                                #pragma omp cancel for
                                logger::log_error(DBERR_COMM_SEND, "Failed forwarding instruction to controller", i);
                                ret = local_ret;
                            }
                        }
                    }
                }
                return ret;
            }
        }

        DB_STATUS broadcastDatasetMetadata(Dataset* dataset) {
            // SerializedMsg<char> msgPack(MPI_CHAR);
            // // serialize
            // DB_STATUS ret = dataset->serialize(&msgPack.data, msgPack.count);
            // if (ret == DBERR_MALLOC_FAILED) {
            //     logger::log_error(DBERR_MALLOC_FAILED, "Dataset serialization failed");
            //     return DBERR_MALLOC_FAILED;
            // }
            // // broadcast the pack
            // ret = broadcast::broadcastMessage(msgPack, MSG_DATASET_METADATA);
            // if (ret != DBERR_OK) {
            //     logger::log_error(ret, "Failed to broadcast dataset metadata");
            //     return ret;
            // }
            // // free
            // free(msgPack.data);
            // return ret;
            return DBERR_FEATURE_UNSUPPORTED;
        }
        
        DB_STATUS gatherResponses() {
            DB_STATUS ret = DBERR_OK;

            // use threads to parallelize gathering of responses
            #pragma omp parallel num_threads(g_world_size)
            {
                DB_STATUS local_ret = DBERR_OK;
                int messageFound = 0;
                MPI_Status status;
                int tid = omp_get_thread_num();

                // probe for responses
                if (tid == 0) {
                    // wait for agent's response
                    local_ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                    // receive response
                    local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_agent_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                } else {
                    // wait for workers' responses (each thread with id X receives from worker with rank X)
                    local_ret = probe(tid, MPI_ANY_TAG,g_controller_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                    // receive response
                    local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG,g_controller_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                }

                // check response
                if (status.MPI_TAG != MSG_ACK) {
                    #pragma omp cancel parallel
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Node", status.MPI_SOURCE, "finished with error");
                    ret = DBERR_PARTITIONING_FAILED;
                }
            }

            return ret;
        }

        static DB_STATUS handleQueryResultMessage(MPI_Status &status, MPI_Comm &comm, QueryType queryType, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            SerializedMsg<int> msg;
            // receive the serialized message
            ret = recv::receiveMessage(status, MPI_INT, comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Error receiving query result message");
                return ret;
            }
            // unpack
            ret = unpack::unpackQueryResults(msg, queryType, queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Error unpacking query results message");
                return ret;
            }
            return ret;
        }

        DB_STATUS gatherResults() {
            DB_STATUS ret = DBERR_OK;
            int threadThatFailed = -1;
            QueryOutput queryOutput;
            // use threads to parallelize gathering of results
            #pragma omp parallel num_threads(g_world_size) reduction(query_output_reduction:queryOutput)
            {
                DB_STATUS local_ret = DBERR_OK;
                int messageFound = 0;
                MPI_Status status;
                int tid = omp_get_thread_num();
                QueryOutput localQueryOutput;

                // probe for results
                if (tid == 0) {
                    // wait for agent's results
                    local_ret = probe(AGENT_RANK, MPI_ANY_TAG, g_agent_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        threadThatFailed = tid;
                        ret = local_ret;
                    }
                    // todo: check tag?
                    // receive results
                    local_ret = handleQueryResultMessage(status, g_agent_comm, g_config.queryMetadata.type, localQueryOutput);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        threadThatFailed = tid;
                        ret = local_ret;
                    }
                } else {
                    // wait for workers' results (each thread with id X receives from worker with rank X)
                    local_ret = probe(tid, MPI_ANY_TAG,g_controller_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        threadThatFailed = tid;
                        ret = local_ret;
                    }
                    // todo: check tag?
                    // receive results
                    local_ret = handleQueryResultMessage(status,g_controller_comm, g_config.queryMetadata.type, localQueryOutput);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        threadThatFailed = tid;
                        ret = local_ret;
                    }
                }
                // add localQueryOutput to reduction
                queryOutput.queryResults += localQueryOutput.queryResults;
                queryOutput.postMBRFilterCandidates += localQueryOutput.postMBRFilterCandidates;
                queryOutput.refinementCandidates += localQueryOutput.refinementCandidates;
                switch (g_config.queryMetadata.type) {
                    case Q_RANGE:
                    case Q_DISJOINT:
                    case Q_INTERSECT:
                    case Q_INSIDE:
                    case Q_CONTAINS:
                    case Q_COVERS:
                    case Q_COVERED_BY:
                    case Q_MEET:
                    case Q_EQUAL:
                        queryOutput.trueHits += localQueryOutput.trueHits;
                        queryOutput.trueNegatives += localQueryOutput.trueNegatives;
                        break;
                    case Q_FIND_RELATION:
                        for (auto &it : localQueryOutput.topologyRelationsResultMap) {
                            queryOutput.topologyRelationsResultMap[it.first] += it.second;
                        }
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", g_config.queryMetadata.type);
                        threadThatFailed = tid;
                        ret = DBERR_QUERY_INVALID_TYPE;
                        break;
                }
            }
            // check for errors
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Gathering results failed. Thread responsible:", threadThatFailed);
                return ret;
            }
            // store to the global output and return
            g_queryOutput.shallowCopy(queryOutput);
            return ret;
        }

        static DB_STATUS handleDatasetMetadataMessage(MPI_Status &status) {
            SerializedMsg<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_global_intra_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward to local agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_agent_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding message to agent");
                return ret;
            }
            // send to controllers
            #pragma omp parallel
            {
                DB_STATUS local_ret = DBERR_OK;
                #pragma omp for
                for (int i=0; i<g_world_size; i++) {
                    if (i != HOST_LOCAL_RANK) {
                        // send to controllers
                        local_ret = send::sendMessage(msg, i, status.MPI_TAG, g_controller_comm);
                        if (ret != DBERR_OK) {
                            #pragma omp cancel for
                            logger::log_error(DBERR_COMM_SEND, "Failed forwarding message to controller", i);
                            ret = local_ret;
                        }
                    }
                }
            }
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
                    return DB_FIN;
                case MSG_DATASET_METADATA:
                    /* dataset metadata message */
                    ret = handleDatasetMetadataMessage(status);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed while handling dataset metadata message.");
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