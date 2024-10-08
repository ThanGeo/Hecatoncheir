#ifndef D_COMM_DEF_H
#define D_COMM_DEF_H

#include "containers.h"
#include <unordered_map>

#define LISTENING_INTERVAL 1
#define MSG_BASE 0

/** @enum MsgType @brief Message types. They are used as tags (MPI_TAG values) in the MPI communication messages. */
typedef enum MsgType {
    /* BASE */
    MSG_ACK = MSG_BASE,
    MSG_NACK = MSG_BASE + 1,
    
    /* INSTRUCTIONS */
    MSG_INSTR_BEGIN = MSG_BASE + 2000,
    MSG_INSTR_FIN = MSG_INSTR_BEGIN,
    MSG_INSTR_PARTITIONING_INIT = MSG_BASE + 2001,
    
    MSG_INSTR_END,

    /* SETUP */
    MSG_SYS_INFO = MSG_BASE + 3000,

    /* BATCHES */
    MSG_BATCH_BEGIN = MSG_BASE + 4000,
    MSG_DATASET_METADATA = MSG_BATCH_BEGIN,
    MSG_SINGLE_POINT = MSG_BASE + 4001,
    MSG_SINGLE_LINESTRING = MSG_BASE + 4002,
    MSG_SINGLE_POLYGON = MSG_BASE + 4003,
    MSG_BATCH_POINT = MSG_BASE + 4004,
    MSG_BATCH_LINESTRING = MSG_BASE + 4005,
    MSG_BATCH_POLYGON = MSG_BASE + 4006,
    
    MSG_DATATYPE_END,

    /* APPROXIMATIONS */
    MSG_APPROXIMATION_BEGIN = MSG_BASE + 5000,
    MSG_APRIL_CREATE = MSG_APPROXIMATION_BEGIN,

    MSG_APPROXIMATION_END,

    /* QUERY */
    MSG_QUERY_INIT = MSG_BASE + 6000,
    MSG_QUERY_RESULT = MSG_BASE + 6001,

    /* DATA */
    MSG_LOAD_DATASET = MSG_BASE + 7000,
    MSG_UNLOAD_DATASET = MSG_BASE + 7001,
    MSG_LOAD_APRIL = MSG_BASE + 7002,
    MSG_UNLOAD_APRIL = MSG_BASE + 7003,
    
    /* ERRORS */
    MSG_ERR_BEGIN = MSG_BASE + 10000,
    MSG_ERR = MSG_ERR_BEGIN,

    MSG_ERR_END,
}MsgTypeE;

inline std::unordered_map<MPI_Datatype, std::string> g_MPI_Datatype_map = {{MPI_INT, "MPI_INT"},
                                                                    {MPI_DOUBLE, "MPI_DOUBLE"},
                                                                    {MPI_CHAR, "MPI_CHAR"},
                                                                };

/** @brief Serialized message of template type T that is used for MPI communication. */
template <typename T> 
struct SerializedMsg { 
    // mpi datatype of message data
    MPI_Datatype type;
    // number of elements in message
    int count = 0;
    // pointer to the message data
    T *data;

    SerializedMsg(MPI_Datatype type) {
        this->type = type;
    }

    SerializedMsg() {
        if constexpr (std::is_same<T,char>::value) {
            this->type = MPI_CHAR;
        } else if constexpr (std::is_same<T,int>::value) {
            this->type = MPI_INT;
        } else {
            printf("Error: unknown serialized msg type");
        }
    }

}; 

#endif