#include "env/comm.h"

namespace comm 
{
    static DB_STATUS receiveAndVerifyResponse(MPI_Status &status) {

    }


    /**
     * @brief probes for a message in the given communicator with the specified parameters.
     * if it exists, its info is stored in the status parameters.
     * returns true/false if a message that fits the parameters exists.
     * Does not receive the message, must call MPI_Recv for that.
     */
    static int probeNonBlocking(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
        int messageExists = false;        
        MPI_Iprobe(sourceRank, tag, comm, &messageExists, &status);
        return messageExists;
    }

    /**
     * @brief probes for a message in the given communicator with the specified parameters
     * and blocks until such a message arrives. 
     * Does not receive the message, must call MPI_Recv for that.
     */
    static DB_STATUS probeBlocking(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
        int mpi_ret = MPI_Probe(sourceRank, tag, comm, &status);
        if(mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_COMM_PROBE_FAILED, "Blocking probe failed");
            return DBERR_COMM_PROBE_FAILED;
        }
        return DBERR_OK;
    }

    /**
     * @brief receives and forwards a message from source to dest
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

    namespace agent
    {
        /**
         * @brief performs the init system instruction
         * 
         * @return DB_STATUS 
         */
        static DB_STATUS performInitInstruction() {
            // verify local directories
            DB_STATUS ret = configure::verifySystemDirectories();
            if (ret != DBERR_OK) {
                // error
                logger::log_error(ret, "Failed while verifying system directories.");
                return ret ;
            }
            return ret;
        }

        /**
         * @brief pulls the probed instruction message and based on its tag, performs the requested instruction
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullInstructionAndPerform(MPI_Status &status) {
            // receive the instruction message
            DB_STATUS ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // perform the corresponding instruction
            switch (status.MPI_TAG) {
                case MSG_INSTR_INIT:
                    ret = performInitInstruction();
                    break;
                case MSG_INSTR_FIN:
                    return DB_FIN;
                default:
                    logger::log_error(DBERR_COMM_UNKNOWN_INSTR, "Unknown instruction");
                    return DBERR_COMM_UNKNOWN_INSTR;
            }
            return ret;
        }

        static DB_STATUS deserializeBatchMessageAndStore(char *buffer, int bufferSize, int &continueListening) {
            GeometryBatchT batch;
            // deserialize
            batch.deserialize(buffer, bufferSize);

            if (batch.objectCount > 0) {
                // do stuff
                logger::log_success("Received batch with", batch.objectCount, "objects");
            } else {
                // empty batch, set flag to stop listening for this dataset
                continueListening = 0;
            }

            return DBERR_OK;
        }

        static DB_STATUS pullSerializedMessageAndHandle(MPI_Status status, int &continueListening) {
            int tag = status.MPI_TAG;
            SerializedMsgT<char> msg(MPI_CHAR);
            // receive the message
            DB_STATUS ret = recv::receiveMessage(status, msg.type, g_local_comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling serialized message");
                return ret;
            }

            // deserialize based on tag
            switch (tag) {
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    ret = deserializeBatchMessageAndStore(msg.data, msg.count, continueListening);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                default:
                    logger::log_error(DBERR_COMM_INVALID_MSG_TAG, "Invalid message tag");
                    break;
            }

            return ret;
        }

        static DB_STATUS listenForDataset(MPI_Status status) {
            SerializedMsgT<char> datasetInfoMsg(MPI_CHAR);
            spatial_lib::DatasetT dataset;
            int listen = 1;

            // retrieve dataset info pack
            DB_STATUS ret = recv::receiveMessage(status, datasetInfoMsg.type, g_local_comm, datasetInfoMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling the dataset info message");
                goto STOP_LISTENING;
            }

            // new dataset
            // deserialize
            dataset.deserialize(datasetInfoMsg.data, datasetInfoMsg.count);
            // store in local g_config
            g_config.datasetInfo.addDataset(dataset);
            // free temp memory
            free(datasetInfoMsg.data);

            // listen for dataset batches until an empty batch arrives
            while(listen) {
                // proble blockingly for batch
                ret = probeBlocking(PARENT_RANK, MPI_ANY_TAG, g_local_comm, status);
                if (ret != DBERR_OK) {
                    goto STOP_LISTENING;
                }
                // pull
                switch (status.MPI_TAG) {
                    case MSG_INSTR_FIN: // todo: maybe this is not needed
                        /* stop listening for instructions */
                        ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
                        if (ret == DBERR_OK) {
                            // break the listening loop
                            return DB_FIN;
                        }
                        goto STOP_LISTENING;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = pullSerializedMessageAndHandle(status, listen);
                        if (ret != DBERR_OK) {
                            goto STOP_LISTENING;
                        }  
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "After the dataset info pack, only geometry packs are expected");
                        ret = DBERR_COMM_WRONG_MESSAGE_ORDER;
                        goto STOP_LISTENING;
                }
            }
            
STOP_LISTENING:
            if (ret == DBERR_OK) {
                // send ACK back that all data for this dataset has been received and processed successfully
                ret = send::sendResponse(PARENT_RANK, MSG_ACK, g_local_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Send ACK failed.");
                }
            } else {
                // there was an error, send NACK and propagate error code locally
                DB_STATUS errorCode = ret;
                ret = send::sendResponse(PARENT_RANK, MSG_NACK, g_local_comm);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Send NACK failed.");
                    return ret;
                }
                return errorCode;
            }
            return ret;
        }

        /**
         * @brief pulls incoming message sent by the local controller 
         * (the one probed last, whose info is stored in the status parameter)
         * Based on the tag of the message, it performs the corresponding request.
         * @param status 
         * @return DB_STATUS 
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
                case MSG_DATASET_INFO:
                    /* dataset partitioning */
                    ret = listenForDataset(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    /* geometry messages must follow the dataset info message (handled in listenForDataset()) */
                    logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Batch messages must follow a dataset info pack", status.MPI_TAG);
                    return DBERR_COMM_WRONG_MESSAGE_ORDER;
                default:
                    logger::log_error(DBERR_COMM_UNKNOWN_INSTR, "Unknown instruction type with tag", status.MPI_TAG);
                    return DBERR_COMM_UNKNOWN_INSTR;
            }
            return ret;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            while(true){
                /* Do a blocking probe to wait for the next task/instruction by the local controller (parent) */
                ret = probeBlocking(PARENT_RANK, MPI_ANY_TAG, g_local_comm, status);
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
        DB_STATUS serializeAndSendGeometryBatch(GeometryBatchT &batch, int destRank, int tag, MPI_Comm &comm) {
            DB_STATUS ret = DBERR_OK;
            SerializedMsgT<char> msg(MPI_CHAR);
            // serialize (todo: add try/catch for segfauls, mem access etc...)   
            msg.count = batch.serialize(&msg.data);
            if (msg.count == -1) {
                logger::log_error(DBERR_BATCH_FAILED, "Batch serialization failed");
                return DBERR_BATCH_FAILED;
            }
            // send batch message
            ret = comm::send::sendMessage(msg, destRank, tag, comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending serialized geometry batch failed");
                return ret;
            }
            
            return ret;
        }

        DB_STATUS broadcastDatasetInfo(spatial_lib::DatasetT* dataset) {
            SerializedMsgT<char> msgPack(MPI_CHAR);
            // serialize
            msgPack.count = dataset->serialize(&msgPack.data);
            if (msgPack.count == -1) {
                logger::log_error(DBERR_MALLOC_FAILED, "Dataset serialization failed");
                return DBERR_MALLOC_FAILED;
            }

            // send the pack
            DB_STATUS ret = broadcast::broadcastDatasetInfo(msgPack);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to broadcast dataset info");
                return ret;
            }

            // send it to the local agent
            ret = send::sendDatasetInfoMessage(msgPack, AGENT_RANK, MSG_DATASET_INFO, g_local_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 

            // free
            free(msgPack.data);

            return ret;
        }
        
        DB_STATUS sendInstructionToAgent(int tag) {
            // send it to agent
            DB_STATUS ret = send::sendInstructionMessage(AGENT_RANK, tag, g_local_comm);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_COMM_SEND, "Failed sending instruction to agent");
                return DBERR_COMM_SEND;
            }
            return ret;
        }

        DB_STATUS forwardInstructionToAgent(MPI_Status status) {
            int messageTag = status.MPI_TAG;
            // receive instruction msg
            DB_STATUS ret = recv::receiveInstructionMessage(status.MPI_SOURCE, messageTag, g_global_comm, status);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding instruction with tag", messageTag, "to agent");
                return ret;
            }
            // send it to agent
            ret = sendInstructionToAgent(messageTag);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed forwarding instruction with tag", messageTag, "to agent");
                return ret;
            }
            return DBERR_OK;
        }

        /**
         * @brief Forwards a serialized batch message to the agent
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS forwardSerializedMessageToAgent(MPI_Status status, int &continueListening) {
            DB_STATUS ret = DBERR_OK;
            int tag = status.MPI_TAG;
            SerializedMsgT<char> msg(MPI_CHAR);
            // receive batch message
            ret = recv::receiveMessage(status, msg.type, g_global_comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling serialized message");
                return ret;
            }
            
            // send the packs to the agent
            ret = send::sendMessage(msg, AGENT_RANK, status.MPI_TAG, g_local_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forwarding geometry batch failed");
                return ret;
            }
            // get batch object count
            int objectCount;
            memcpy(&objectCount, msg.data, sizeof(int));
            if (objectCount == 0) {
                continueListening = 0;
            }
            return ret;
        }

        /**
         * @brief Pulls and forwards a dataset info message to the local agent
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS forwardDatasetInfoToAgent(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            int tag = status.MPI_TAG;
            SerializedMsgT<char> datasetInfoMsg(MPI_CHAR);
            // receive dataset info message
            ret = recv::receiveMessage(status, datasetInfoMsg.type, g_global_comm, datasetInfoMsg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed pulling the dataset info message");
                return ret;
            }
            // send
            ret = send::sendDatasetInfoMessage(datasetInfoMsg, AGENT_RANK, status.MPI_TAG, g_local_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 
            // free temp memory
            free(datasetInfoMsg.data);
            return ret;
        }

        static DB_STATUS listenForDataset(MPI_Status status) {
            // forward dataset info to agent
            DB_STATUS ret = forwardDatasetInfoToAgent(status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // next, listen for the infoPacks and coordPacks explicitly
            int listen = 1;
            while(listen) {
                // proble blockingly
                ret = probeBlocking(g_host_rank, MPI_ANY_TAG, g_global_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // a message has been probed, check its tag
                switch (status.MPI_TAG) {
                    case MSG_INSTR_FIN:
                        /* stop listening for instructions */
                        ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
                        if (ret == DBERR_OK) {
                            // break the listening loop
                            return DB_FIN;
                        }
                        return ret;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = forwardSerializedMessageToAgent(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        } 
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "After the dataset info pack, only batch messages are expected");
                        return DBERR_COMM_WRONG_MESSAGE_ORDER;
                }
            }

            // wait for response by the agent that all is well for this dataset partitioning
            ret = probeBlocking(AGENT_RANK, MPI_ANY_TAG, g_local_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // receive response
            ret = recv::receiveResponse(AGENT_RANK, status.MPI_TAG, g_local_comm, status);
            if (ret != DBERR_OK) {
                return ret;
            }
            // forward response to host controller
            ret = send::sendResponse(g_host_rank, status.MPI_TAG, g_global_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forwarding response to host controller failed");
                return ret;
            }
            // todo: maybe do something on nack?
            return ret;
        }

        /**
         * @brief pulls incoming message sent by the host controller 
         * (the one probed last, whose info is stored in the status parameter)
         * Based on the tag of the message, it performs the corresponding request.
         * The controller almost always forwards the message to its local agent.
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            
            // check message tag
            switch (status.MPI_TAG) {
                case MSG_INSTR_INIT:
                    /* initialize */
                    ret = comm::controller::forwardInstructionToAgent(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_INSTR_FIN:
                    /* terminate */
                    ret = comm::controller::forwardInstructionToAgent(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    return DB_FIN;
                case MSG_DATASET_INFO:
                    /* message containing dataset info */
                    ret = listenForDataset(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    logger::log_error(DBERR_COMM_WRONG_MESSAGE_ORDER, "Geometry packs must follow a dataset info pack", status.MPI_TAG);
                    return DBERR_COMM_WRONG_MESSAGE_ORDER;    // change to error codes to stop agent from working after errors
                default:
                    // unkown instruction
                    logger::log_error(DBERR_COMM_UNKNOWN_INSTR, "Controller failed with unkown instruction");
                    return DBERR_COMM_UNKNOWN_INSTR;
            }

            return ret;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            int messageFound;
            while(true){
                /* controller probes non-blockingly for messages either by the agent or by other controllers */

                // check whether the agent has sent a message
                messageFound = probeNonBlocking(AGENT_RANK, MPI_ANY_TAG, g_local_comm, status);
                if (messageFound) {
                    
                }

                // check whether the host controller has sent a message
                messageFound = probeNonBlocking(g_host_rank, MPI_ANY_TAG, g_global_comm, status);
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

                // do periodic waiting before probing again
                sleep(LISTENING_INTERVAL);
            }
STOP_LISTENING:
            return DBERR_OK;
        }

        DB_STATUS gatherResponses() {
            DB_STATUS ret = DBERR_OK;

            // use threads to parallelize gathering of responses
            #pragma omp parallel num_threads(g_world_size)
            {
                DB_STATUS local_ret = DBERR_OK;
                int messageFound;
                MPI_Status status;
                int tid = omp_get_thread_num();

                // probe for responses
                if (tid == 0) {
                    // wait for agent's response
                    local_ret = probeBlocking(AGENT_RANK, MPI_ANY_TAG, g_local_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                    // receive response
                    local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_local_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                } else {
                    // wait for workers' responses (each thread with id X receives from worker with rank X)
                    local_ret = probeBlocking(tid, MPI_ANY_TAG, g_global_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                    // receive response
                    local_ret = recv::receiveResponse(status.MPI_SOURCE, status.MPI_TAG, g_global_comm, status);
                    if (local_ret != DBERR_OK) {
                        #pragma omp cancel parallel
                        ret = local_ret;
                    }
                }

                // check response
                if (status.MPI_TAG != MSG_ACK) {
                    #pragma omp cancel parallel
                    logger::log_error(DBERR_PARTITIONING_FAILED, "Node", status.MPI_SOURCE, "did not finish partitioning successfully");
                    ret = DBERR_PARTITIONING_FAILED;
                }
            }

            return ret;
        }
    }
}