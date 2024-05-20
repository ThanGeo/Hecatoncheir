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
    static void probeBlocking(int sourceRank, int tag, MPI_Comm &comm, MPI_Status &status) {
        MPI_Probe(sourceRank, tag, comm, &status);
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
        probeBlocking(g_host_rank, tag, comm, status);
        // receive the coords pack
        ret = recv::receiveMessagePack(status, coordsPack.type, comm, coordsPack);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Failed pulling the coords pack");
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

        static DB_STATUS pullGeometryPacksAndPerform(MPI_Status status) {
            MsgPackT<int> infoPack(MPI_INT);
            MsgPackT<double> coordsPack(MPI_DOUBLE);

            // retrieve
            DB_STATUS ret = pullGeometryPacks(status, status.MPI_TAG, g_local_comm, infoPack, coordsPack);
            if (ret != DBERR_OK) {
                return ret;
            }

            // save on disk (or whatever)
            // pack::printMsgPack(infoPack);
            // pack::printMsgPack(coordsPack);

            // free memory
            free(infoPack.data);
            free(coordsPack.data);

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
                case MSG_SINGLE_POINT:
                case MSG_SINGLE_LINESTRING:
                case MSG_SINGLE_POLYGON:
                    /* single geometry message */
                    ret = pullGeometryPacksAndPerform(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    }                    
                    break;
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
                probeBlocking(PARENT_RANK, MPI_ANY_TAG, g_local_comm, status);
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
        static DB_STATUS forwardGeometryToAgent(MPI_Status status) {
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

            // free memory
            free(infoPack.data);
            free(coordsPack.data);

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
                case MSG_SINGLE_POINT:
                case MSG_SINGLE_LINESTRING:
                case MSG_SINGLE_POLYGON:
                    /* message containing a single geometry (point, linestring or polygon) */
                    ret = forwardGeometryToAgent(status);
                    if (ret != DBERR_OK) {
                        return ret;
                    } 
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