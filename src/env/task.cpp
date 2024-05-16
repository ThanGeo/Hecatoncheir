#include "env/task.h"

namespace task
{
    namespace controller
    {
        DB_STATUS performInstructionFromMessage(int sourceRank, int tag) {
            DB_STATUS ret = DBERR_OK;
            switch (tag) {
                case MSG_INSTR_FIN:
                    /* terminate */
                    // forward this instruction to the agent
                    ret = comm::controller::forwardInstructionMsgToAgent(sourceRank, tag, MPI_COMM_WORLD);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    return DB_FIN;
                default:
                    // unkown instruction
                    log::log_error(DBERR_COMM_UNKNOWN_INSTR, "Controller failed with unkown instruction");
                    return DBERR_COMM_UNKNOWN_INSTR;
            }

            return ret;
        }
    }
}