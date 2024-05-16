#ifndef D_COMM_DEF_H
#define D_COMM_DEF_H

#include "def.h"

typedef enum NodeType {
    NODE_HOST,
    NODE_WORKER
} NodeTypeE;

/* message types (used as MPI_TAG) */
#define MSG_BASE 100000
typedef enum MsgType {
    /* INSTRUCTIONS */
    MSG_INSTR_BEGIN = MSG_BASE,
    MSG_INSTR_INIT = MSG_INSTR_BEGIN,
    
    MSG_INSTR_FIN = MSG_BASE + 1000,
    MSG_INSTR_END,
    
    /* ERRORS */
    MSG_ERR_BEGIN = MSG_BASE + 10000,
    MSG_ERR = MSG_ERR_BEGIN,

    MSG_ERR_END,
};

#endif