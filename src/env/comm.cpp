#include "env/comm.h"

namespace comm 
{
    /**
     * @brief probes for a message in the given communicator with the specified parameters.
     * if it exists, its info is stored in the status parameters.
     * returns true/false if a message that fits the parameters exists.
     * Does not receive the message, must call MPI_Recv for that.
     */
    int probeNonBlocking(int sourceRank, int tag, MPI_Comm comm, MPI_Status &status) {
        int messageExists = false;        
        MPI_Iprobe(sourceRank, tag, comm, &messageExists, &status);
        return messageExists;
    }

    /**
     * @brief probes for a message in the given communicator with the specified parameters
     * and blocks until such a message arrives. 
     * Does not receive the message, must call MPI_Recv for that.
     */
    void probeBlocking(int sourceRank, int tag, MPI_Comm comm, MPI_Status &status) {
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
    static DB_STATUS forwardInstructionMsg(int sourceRank, int sourceTag, MPI_Comm sourceComm, int destRank, MPI_Comm destComm) {
        MPI_Status status;
        // receive instruction
        DB_STATUS ret = recv::receiveInstructionMsg(sourceRank, sourceTag, sourceComm, status);
        if (ret != DBERR_OK) {
            log::log_error(DBERR_COMM_RECV, "Failed forwarding instruction");
            return DBERR_COMM_RECV;
        }
        // send it
        ret = send::sendInstructionMsg(destRank, status.MPI_TAG, destComm);
        if (ret != DBERR_OK) {
            log::log_error(DBERR_COMM_SEND, "Failed forwarding instruction");
            return DBERR_COMM_SEND;
        }

        return DBERR_OK;
    }

    namespace agent
    {
        static DB_STATUS performInstructionFromMessage(MPI_Status &status) {
            DB_STATUS ret = DBERR_OK;
            switch (status.MPI_TAG) {
                case MSG_INSTR_INIT:
                    /* initialize environment */
                    // pull the message
                    ret = recv::receiveInstructionMsg(PARENT_RANK, status.MPI_TAG, proc::g_intercomm, status);
                    if (ret != DBERR_OK) {
                        // report error to controller
                        send::sendSingleIntMessage(ret, PARENT_RANK, MSG_ERR, proc::g_intercomm);
                        return DBERR_OK;
                    }
                    // verify local directories
                    ret = configure::verifySystemDirectories();
                    if (ret != DBERR_OK) {
                        // report error to controller
                        send::sendSingleIntMessage(ret, PARENT_RANK, MSG_ERR, proc::g_intercomm);
                        return DBERR_OK;
                    }
                    break;
                case MSG_INSTR_FIN:
                    /* stop listening for instructions */
                    // pull the finalization message
                    ret = recv::receiveInstructionMsg(PARENT_RANK, status.MPI_TAG, proc::g_intercomm, status);
                    if (ret == DBERR_OK) {
                        // break the listening loop
                        return DB_FIN;
                    }
                    return ret;
                default:
                    log::log_error(DBERR_COMM_UNKNOWN_INSTR, "Unknown instruction type with tag", status.MPI_TAG);
                    // report error to controller
                    send::sendSingleIntMessage(DBERR_COMM_UNKNOWN_INSTR, PARENT_RANK, MSG_ERR, proc::g_intercomm);
                    return DBERR_OK;    // change to error codes to stop agent from working after errors
            }

            return ret;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            while(true){
                /* Do a blocking probe to wait for the next task/instruction by the local controller (parent) */
                probeBlocking(PARENT_RANK, MPI_ANY_TAG, proc::g_intercomm, status);
                ret = performInstructionFromMessage(status);
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
        DB_STATUS SendInstructionMessageToAgent(int tag) {
            // send it to agent
            DB_STATUS ret = send::sendInstructionMsg(AGENT_RANK, tag, proc::g_intercomm);
            if (ret != DBERR_OK) {
                log::log_error(DBERR_COMM_SEND, "Failed forwarding instruction to agent");
                return DBERR_COMM_SEND;
            }
            return ret;
        }

        DB_STATUS forwardInstructionMsgToAgent(int sourceRank, int sourceTag, MPI_Comm sourceComm) {

            MPI_Status status;
            // receive instruction msg
            DB_STATUS ret = recv::receiveInstructionMsg(sourceRank, sourceTag, sourceComm, status);
            if (ret != DBERR_OK) {
                log::log_error(DBERR_COMM_RECV, "Failed forwarding instruction to children");
                return DBERR_COMM_RECV;
            }
            // send it to agent
            ret = SendInstructionMessageToAgent(status.MPI_TAG);
            if (ret != DBERR_OK) {
                log::log_error(ret, "Failed forwarding instruction to children");
                return ret;
            }
            // printf("Controller %d forwarded the instruction with code %d to the children\n", g_node_rank, status.MPI_TAG);

            return DBERR_OK;
        }

        DB_STATUS listen() {
            MPI_Status status;
            DB_STATUS ret = DBERR_OK;
            int messageFound;
            while(true){
                /* controller probes non-blockingly for messages either by the agent or by other controllers */

                // check whether the agent has sent a message
                messageFound = probeNonBlocking(AGENT_RANK, MPI_ANY_TAG, proc::g_intercomm, status);
                if (messageFound) {
                    
                }

                // check whether the host controller has sent a message (instruction)
                messageFound = probeNonBlocking(g_host_rank, MPI_ANY_TAG, MPI_COMM_WORLD, status);
                if (messageFound) {
                    ret = task::controller::performInstructionFromMessage(status.MPI_SOURCE, status.MPI_TAG);
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