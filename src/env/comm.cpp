#include "env/comm.h"

namespace comm 
{
   
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

    template <typename T> 
    static DB_STATUS pullSerializedMessage(MPI_Status status, int tag, MPI_Comm &comm, SerializedMsgT<T> &msg) {
        // get message size 
        int mpi_ret = MPI_Get_count(&status, msg.type, &msg.count);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_COMM_GET_COUNT, "Failed when trying to get the message size");
            return DBERR_COMM_GET_COUNT;
        }

        // allocate space
        msg.data = (T*) malloc(msg.count * sizeof(T));
        
        DB_STATUS ret = recv::receiveSerializedMessage(status.MPI_SOURCE, tag, comm, status, msg);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Failed pulling serialized message");
            return ret;
        }

        return ret;
    }

    static DB_STATUS pullDatasetInfoPack(MPI_Status status, int tag, MPI_Comm &comm, SerializedMsgT<char> &datasetInfopack) {
        // info pack has been probed, so receive it
        DB_STATUS ret = recv::receiveMessagePack(status, datasetInfopack.type, comm, datasetInfopack);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Failed pulling the info pack");
            return ret;
        }

        return ret;
    }

    

    namespace agent
    {
        /**
         * @brief receives instruction message for init environment and performs the task
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullInitInstructionAndPerform(MPI_Status status) {
            // pull the message
            DB_STATUS ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
            if (ret != DBERR_OK) {
                // report error to controller
                ret = send::sendSingleIntMessage(ret, PARENT_RANK, MSG_ERR, g_local_comm);
                return ret;
            }
            // verify local directories
            ret = configure::verifySystemDirectories();
            if (ret != DBERR_OK) {
                // report error to controller
                ret = send::sendSingleIntMessage(ret, PARENT_RANK, MSG_ERR, g_local_comm);
                return ret ;
            }
            return ret;
        }

        static DB_STATUS unpackBatchMessageAndStore(char *buffer, int bufferSize, int &continueListening) {
            GeometryBatchT batch;
            // deserialize
            batch.deserialize(buffer, bufferSize);

            // do stuff
            // logger::log_success("Received batch with", batch.objectCount, "objects");


            // set flag
            if (batch.objectCount == 0) {
                continueListening = 0;
            }


            return DBERR_OK;
        }

        static DB_STATUS pullSerializedMessageAndHandle(MPI_Status status, int &continueListening) {
            int tag = status.MPI_TAG;
            SerializedMsgT<char> msg(MPI_CHAR);

            DB_STATUS ret = pullSerializedMessage(status, tag, g_local_comm, msg);
            if (ret != DBERR_OK) {
                return ret;
            }

            // logger::log_success("Pulled serialized message of size", bufferSize);

            // deserialize based on tag
            switch (tag) {
                case MSG_SINGLE_POINT:
                case MSG_SINGLE_LINESTRING:
                case MSG_SINGLE_POLYGON:
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    ret = unpackBatchMessageAndStore(msg.data, msg.count, continueListening);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;

                default:

                    break;
            }

            return ret;
        }

        static DB_STATUS listenForDataset(MPI_Status status) {
            SerializedMsgT<char> datasetInfoPack(MPI_CHAR);

            // retrieve dataset info pack
            DB_STATUS ret = pullDatasetInfoPack(status, status.MPI_TAG, g_local_comm, datasetInfoPack);
            if (ret != DBERR_OK) {
                return ret;
            }

            // new dataset
            spatial_lib::DatasetT dataset;
            // deserialize
            dataset.deserialize(datasetInfoPack.data, datasetInfoPack.count);
            // store in local g_config
            g_config.datasetInfo.addDataset(dataset);
            // free temp memory
            free(datasetInfoPack.data);

            // next, listen for the infoPacks and coordPacks
            int listen = 1;
            while(listen) {
                // proble blockingly for info pack
                ret = probeBlocking(PARENT_RANK, MPI_ANY_TAG, g_local_comm, status);
                if (ret != DBERR_OK) {
                    return ret;
                }
                // pull
                switch (status.MPI_TAG) {
                    case MSG_INSTR_FIN:
                        /* stop listening for instructions */
                        // pull the finalization message
                        ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
                        if (ret == DBERR_OK) {
                            // break the listening loop
                            return DB_FIN;
                        }
                        return ret;
                    case MSG_SINGLE_POINT:
                    case MSG_SINGLE_LINESTRING:
                    case MSG_SINGLE_POLYGON:
                        /* single geometry message */
                        ret = pullSerializedMessageAndHandle(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }                    
                        break;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = pullSerializedMessageAndHandle(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }  
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "After the dataset info pack, only geometry packs are expected");
                        return DBERR_COMM_WRONG_PACK_ORDER;
                }
            }


            // logger::log_success("Received all batches successfully");

            return ret;
        }

        /**
         * @brief pulls incoming message sent by the controller 
         * (the one probed last, whose info is stored in the status parameter)
         * Based on the tag of the message, it performs the corresponding request
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            switch (status.MPI_TAG) {
                case MSG_INSTR_INIT:
                    /* initialize environment */
                    // pull the message
                    ret = pullInitInstructionAndPerform(status);
                    if (ret != DBERR_OK) {
                        // report error to controller
                        ret = send::sendSingleIntMessage(ret, PARENT_RANK, MSG_ERR, g_local_comm);
                        return ret ;
                    }
                    break;
                case MSG_INSTR_FIN:
                    /* stop listening for instructions */
                    // pull the finalization message
                    ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
                    if (ret == DBERR_OK) {
                        // break the listening loop
                        return DB_FIN;
                    }
                    return ret;
                case MSG_DATASET_INFO:
                    /* dataset info pack*/
                    ret = listenForDataset(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                /* geometry messages must follow the dataset info message (listened for in listenForDataset) */
                case MSG_SINGLE_POINT:
                case MSG_SINGLE_LINESTRING:
                case MSG_SINGLE_POLYGON:
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "Geometry packs must follow a dataset info pack", status.MPI_TAG);
                    return DBERR_COMM_WRONG_PACK_ORDER;    // change to error codes to stop agent from working after errors
                default:
                    logger::log_error(DBERR_COMM_UNKNOWN_INSTR, "Unknown instruction type with tag", status.MPI_TAG);
                    // report error to controller
                    ret = send::sendSingleIntMessage(DBERR_COMM_UNKNOWN_INSTR, PARENT_RANK, MSG_ERR, g_local_comm);
                    return ret;    // change to error codes to stop agent from working after errors
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
            ret = comm::send::sendSerializedMessage(msg, destRank, tag, comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending serialized geometry batch failed");
                return ret;
            }
            
            return ret;
        }

        DB_STATUS broadcastDatasetInfo(spatial_lib::DatasetT* dataset) {
            SerializedMsgT<char> msgPack(MPI_CHAR);
            // pack the info
            // DB_STATUS ret = pack::packDatasetInfo(dataset, msgPack);
            // if (ret != DBERR_OK) {
            //     logger::log_error(ret, "Failed to pack dataset info");
            //     return ret;
            // }

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
            // printf("Controller %d forwarded the instruction with code %d to the children\n", g_node_rank, status.MPI_TAG);

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
            // pull serialized batch
            SerializedMsgT<char> msg(MPI_CHAR);
            ret = pullSerializedMessage(status, tag, g_global_comm, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forwarding geometry batch failed");
                return ret;
            } 
            // send the packs to the agent
            ret = send::sendSerializedMessage(msg, AGENT_RANK, status.MPI_TAG, g_local_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forwarding geometry batch failed");
                return ret;
            } 

            // logger::log_success("Forwarded serialized message to agent");



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
            SerializedMsgT<char> datasetInfoPack(MPI_CHAR);
            
            // pull 
            ret = pullDatasetInfoPack(status, tag, g_global_comm, datasetInfoPack);
            if (ret != DBERR_OK) {
                return ret;
            }

            // send
            ret = send::sendDatasetInfoMessage(datasetInfoPack, AGENT_RANK, status.MPI_TAG, g_local_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 

            // free memory
            free(datasetInfoPack.data);

            // logger::log_success("Successfully forwarded dataset info to agent");

            return ret;
        }

        static DB_STATUS listenForDataset(MPI_Status status) {
            SerializedMsgT<char> datasetInfoPack(MPI_CHAR);

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
                // pull
                switch (status.MPI_TAG) {
                    case MSG_INSTR_FIN:
                        /* stop listening for instructions */
                        // pull the finalization message
                        ret = recv::receiveInstructionMessage(PARENT_RANK, status.MPI_TAG, g_local_comm, status);
                        if (ret == DBERR_OK) {
                            // break the listening loop
                            return DB_FIN;
                        }
                        return ret;
                    case MSG_SINGLE_POINT:
                    case MSG_SINGLE_LINESTRING:
                    case MSG_SINGLE_POLYGON:
                        /* single geometry message */  
                        ret = forwardSerializedMessageToAgent(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }               
                        break;
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
                        logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "After the dataset info pack, only geometry packs are expected");
                        return DBERR_COMM_WRONG_PACK_ORDER;
                }
            }


            // logger::log_success("Received all batches successfully");

            return ret;
        }

        /**
         * @brief pulls incoming message sent by the controller 
         * (the one probed last, whose info is stored in the status parameter)
         * Based on the tag of the message, it performs the corresponding request
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS pullIncoming(MPI_Status status) {
            DB_STATUS ret = DBERR_OK;
            
            // check message tag
            switch (status.MPI_TAG) {
                case MSG_INSTR_FIN:
                    /* terminate */
                    // forward this instruction to the local agent
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
                case MSG_SINGLE_POINT:
                case MSG_SINGLE_LINESTRING:
                case MSG_SINGLE_POLYGON:
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "Geometry packs must follow a dataset info pack", status.MPI_TAG);
                    return DBERR_COMM_WRONG_PACK_ORDER;    // change to error codes to stop agent from working after errors
                    break;
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

                // check whether the host controller has sent a message (instruction)
                messageFound = probeNonBlocking(g_host_rank, MPI_ANY_TAG, g_global_comm, status);
                if (messageFound) {
                    // pull the message and perform its request
                    ret = controller::pullIncoming(status);
                    // true only if the termination instruction has been received
                    if (ret == DB_FIN) {
                        goto STOP_LISTENING;
                    }
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
            }
STOP_LISTENING:
            return DBERR_OK;
        }
    }
}