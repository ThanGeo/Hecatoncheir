#ifndef D_COMM_DEF_H
#define D_COMM_DEF_H

#include "def.h"
#include <unordered_map>

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

    /* DATA */
    MSG_DATASET_INFO = MSG_BASE + 2000,
    MSG_DATA_BEGIN = MSG_BASE + 2001,
    MSG_SINGLE_POINT = MSG_DATA_BEGIN,
    MSG_SINGLE_LINESTRING = MSG_BASE + 2002,
    MSG_SINGLE_POLYGON = MSG_BASE + 2003,
    MSG_BATCH_POINT = MSG_BASE + 2004,
    MSG_BATCH_LINESTRING = MSG_BASE + 2005,
    MSG_BATCH_POLYGON = MSG_BASE + 2006,

    MSG_DATA_END,
    
    /* ERRORS */
    MSG_ERR_BEGIN = MSG_BASE + 10000,
    MSG_ERR = MSG_ERR_BEGIN,

    MSG_ERR_END,
};

inline std::unordered_map<MPI_Datatype, std::string> g_MPI_Datatype_map = {{MPI_INT, "MPI_INT"},
                                                                    {MPI_DOUBLE, "MPI_DOUBLE"},
                                                                    {MPI_CHAR, "MPI_CHAR"},
                                                                };

template <typename T> 
struct MsgPackT { 
    MPI_Datatype type;      // mpi datatype of message data
    int count = 0;             // number of elements in message
    T *data;                // pointer to the message data

    MsgPackT(MPI_Datatype type) {
        this->type = type;
    }
}; 



#endif