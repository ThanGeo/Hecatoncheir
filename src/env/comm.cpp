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

    /**
     * @brief Handles the complete communication for a single geometry message retrieval.
     * Such messages are comprised by 2 separate MPI messages (called packs):
     * INFO msg (int): recID | sectionID | vertexNum
     * COORDS msg (double): x | y | x | y | ...
     * 
     * @param status 
     * @return DB_STATUS 
     */
    static DB_STATUS pullGeometryPacks(MPI_Status status, int tag, MPI_Comm &comm, MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack) {
        // info pack has been probed, so receive it
        DB_STATUS ret = recv::receiveMessagePack(status, infoPack.type, comm, infoPack);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Failed pulling the info pack");
            return ret;
        }

        // probe for the coords pack (blocking probe for safety, same tag) 
        ret = probeBlocking(g_host_rank, tag, comm, status);
        if (ret != DBERR_OK) {
            return ret;
        }
        // receive the coords pack
        ret = recv::receiveMessagePack(status, coordsPack.type, comm, coordsPack);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Failed pulling the coords pack");
            return ret;
        }

        return ret;
    }

    
    
    static DB_STATUS pullDatasetInfoPack(MPI_Status status, int tag, MPI_Comm &comm, MsgPackT<char> &datasetInfopack) {
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

        static DB_STATUS pullGeometryPacksAndStore(MPI_Status status, int &continueListening) {
            MsgPackT<int> infoPack(MPI_INT);
            MsgPackT<double> coordsPack(MPI_DOUBLE);

            // retrieve
            DB_STATUS ret = pullGeometryPacks(status, status.MPI_TAG, g_local_comm, infoPack, coordsPack);
            if (ret != DBERR_OK) {
                return ret;
            }

            // save on disk
            logger::log_success("Successfully geometry pack with", infoPack.data[0], "objects and flag", infoPack.data[1]);

            // set flag
            continueListening = infoPack.data[1];

            // free memory
            free(infoPack.data);
            free(coordsPack.data);

            return ret;
        }

        static DB_STATUS listenForDataset(MPI_Status status) {
            MsgPackT<char> datasetInfoPack(MPI_CHAR);

            // retrieve dataset info pack
            DB_STATUS ret = pullDatasetInfoPack(status, status.MPI_TAG, g_local_comm, datasetInfoPack);
            if (ret != DBERR_OK) {
                return ret;
            }


            // unpack dataste info
            ret = unpack::unpackDatasetInfoPack(datasetInfoPack);
            if (ret != DBERR_OK) {
                return ret;
            }

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
                        ret = pullGeometryPacksAndStore(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }                    
                        break;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = pullGeometryPacksAndStore(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }  
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "After the dataset info pack, only geometry packs are expected");
                        return DBERR_COMM_WRONG_PACK_ORDER;
                }
            }


            logger::log_success("Received all batches successfully");

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

        DB_STATUS broadcastDatasetInfo(spatial_lib::DatasetT* dataset) {
            MsgPackT<char> msgPack(MPI_CHAR);
            // pack the info
            DB_STATUS ret = pack::packDatasetInfo(dataset, msgPack);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack dataset info");
                return ret;
            }

            // send the pack
            ret = broadcast::broadcastDatasetInfo(msgPack);
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
        
        DB_STATUS sendPolygonToNode(spatial_lib::PolygonT &polygon, int partitionID, int destRank, int tag) {
            // first pack the polygon to message packs
            // pack polygon to message
            MsgPackT<int> infoPack(MPI_INT);
            MsgPackT<double> coordsPack(MPI_DOUBLE);
            DB_STATUS ret = pack::packPolygon(polygon, partitionID, infoPack, coordsPack);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending single polygon failed");
                return ret;
            }

            // send packs
            ret = comm::send::sendGeometryMessage(infoPack, coordsPack, destRank, tag, g_global_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending single polygon failed");
                return ret;
            }

            // free pack allocated memory
            // logger::log_task("About to fre...");
            free(infoPack.data);
            free(coordsPack.data);
            // logger::log_success("Freed!");
            return ret;
        }

        DB_STATUS sendGeometryBatchToNode(BatchT &batch, int destRank, int tag) {
            MsgPackT<int> infoPack(MPI_INT);
            MsgPackT<double> coordsPack(MPI_DOUBLE);

            // pack
            DB_STATUS ret = pack::packGeometryArrays(batch.infoPack, batch.coordsPack, infoPack, coordsPack);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Packing geometry batch failed");
                return ret;
            }

            // send batch message
            ret = comm::send::sendGeometryMessage(infoPack, coordsPack, destRank, tag, g_global_comm);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Sending geometry batch failed");
                return ret;
            }
            
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
         * @brief Handles the complete communication for a single geometry message forwarding.
         * Such messages are comprised by 2 separate MPI messages (called packs):
         * INFO msg (int): recID | sectionID | vertexNum
         * COORDS msg (double): x | y | x | y | ...
         * The function first probes, counts and receives each message and then sends it to the agent.
         * 
         * @param status 
         * @return DB_STATUS 
         */
        static DB_STATUS forwardGeometryToAgent(MPI_Status status, int &continueListening) {
            DB_STATUS ret = DBERR_OK;
            int tag = status.MPI_TAG;
            MsgPackT<int> infoPack(MPI_INT); 
            MsgPackT<double> coordsPack(MPI_DOUBLE);
            // pull both packs for the geometry
            ret = pullGeometryPacks(status, tag, g_global_comm, infoPack, coordsPack);
            if (ret != DBERR_OK) {
                return ret;
            } 
            // send the packs to the agent
            ret = send::sendGeometryMessage(infoPack, coordsPack, AGENT_RANK, status.MPI_TAG, g_local_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 

            // update flag
            continueListening = infoPack.data[1];

            // free memory
            free(infoPack.data);
            free(coordsPack.data);

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
            MsgPackT<char> datasetInfoPack(MPI_CHAR);
            
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

        DB_STATUS sendGeometryBatchToAgent(BatchT &batch, int tag) {
            MsgPackT<int> infoPack(MPI_INT); 
            MsgPackT<double> coordsPack(MPI_DOUBLE);

            // pack
            DB_STATUS ret = pack::packGeometryArrays(batch.infoPack, batch.coordsPack, infoPack, coordsPack);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Packing geometry batch failed");
                return ret;
            }

            // send the packs to the agent
            ret = send::sendGeometryMessage(infoPack, coordsPack, AGENT_RANK, tag, g_local_comm);
            if (ret != DBERR_OK) {
                return ret;
            } 

            return ret;
        }

        static DB_STATUS listenForDataset(MPI_Status status) {
            MsgPackT<char> datasetInfoPack(MPI_CHAR);

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
                        ret = forwardGeometryToAgent(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        }               
                        break;
                    case MSG_BATCH_POINT:
                    case MSG_BATCH_LINESTRING:
                    case MSG_BATCH_POLYGON:
                        /* batch geometry message */
                        ret = forwardGeometryToAgent(status, listen);
                        if (ret != DBERR_OK) {
                            return ret;
                        } 
                        break;
                    default:
                        logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "After the dataset info pack, only geometry packs are expected");
                        return DBERR_COMM_WRONG_PACK_ORDER;
                }
            }


            logger::log_success("Received all batches successfully");

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
                    // ret = forwardDatasetInfoToAgent(status);
                    // if (ret != DBERR_OK) {
                    //     return ret;
                    // } 
                    ret = listenForDataset(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    break;
                // case MSG_SINGLE_POINT:
                // case MSG_SINGLE_LINESTRING:
                // case MSG_SINGLE_POLYGON:
                //     /* message containing a single geometry (point, linestring or polygon) */
                //     ret = forwardGeometryToAgent(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     } 
                //     break;
                // case MSG_BATCH_POINT:
                // case MSG_BATCH_LINESTRING:
                // case MSG_BATCH_POLYGON:
                //     /* message containing a batch of geometries */
                //     ret = forwardGeometryToAgent(status);
                //     if (ret != DBERR_OK) {
                //         return ret;
                //     } 

                //     break;
                case MSG_SINGLE_POINT:
                case MSG_SINGLE_LINESTRING:
                case MSG_SINGLE_POLYGON:
                case MSG_BATCH_POINT:
                case MSG_BATCH_LINESTRING:
                case MSG_BATCH_POLYGON:
                    logger::log_error(DBERR_COMM_WRONG_PACK_ORDER, "Geometry packs must follow a dataset info pack", status.MPI_TAG);
                    return DBERR_COMM_WRONG_PACK_ORDER;    // change to error codes to stop agent from working after errors
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