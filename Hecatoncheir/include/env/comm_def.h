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
    MSG_INSTR_BATCH_FINISHED = MSG_BASE + 2001,
    MSG_QUERY_DJ_FIN = MSG_BASE + 2002,

    
    MSG_INSTR_END,

    /* SETUP */
    MSG_SYS_INFO = MSG_BASE + 3000,

    /* BATCHES */
    MSG_BATCH_BEGIN = MSG_BASE + 4000,
    MSG_SINGLE_POINT = MSG_BATCH_BEGIN,
    MSG_SINGLE_LINESTRING = MSG_BASE + 4001,
    MSG_SINGLE_POLYGON = MSG_BASE + 4002,
    MSG_BATCH_POINT = MSG_BASE + 4003,
    MSG_BATCH_LINESTRING = MSG_BASE + 4004,
    MSG_BATCH_POLYGON = MSG_BASE + 4005,
    
    MSG_DATATYPE_END,

    /* APPROXIMATIONS */
    MSG_APPROXIMATION_BEGIN = MSG_BASE + 5000,
    MSG_APRIL_CREATE = MSG_APPROXIMATION_BEGIN,

    MSG_APPROXIMATION_END,

    /* QUERY */
    MSG_QUERY_INIT = MSG_BASE + 6000,
    MSG_QUERY = MSG_BASE + 6001,
    MSG_QUERY_RESULT = MSG_BASE + 6002,
    MSG_QUERY_BATCH_RANGE = MSG_BASE + 6003,
    MSG_QUERY_BATCH_KNN = MSG_BASE + 6004,
    MSG_QUERY_BATCH_RESULT = MSG_BASE + 6005,
    // distance join
    MSG_QUERY_DJ_INIT = MSG_BASE + 6006,
    MSG_QUERY_DJ_COUNT = MSG_BASE + 6007,
    MSG_QUERY_DJ_BATCH = MSG_BASE + 6008,
    MSG_QUERY_DJ_REQUEST_INIT = MSG_BASE + 6009,

    /* DATA */
    MSG_LOAD_DATASET = MSG_BASE + 7000,
    MSG_UNLOAD_DATASET = MSG_BASE + 7001,
    MSG_LOAD_APRIL = MSG_BASE + 7002,
    MSG_UNLOAD_APRIL = MSG_BASE + 7003,
    MSG_DATASET_INDEX = MSG_BASE + 7004,
    MSG_PREPARE_DATASET = MSG_BASE + 7005,
    MSG_PARTITION_DATASET = MSG_BASE + 7006,
    MSG_GLOBAL_DATASPACE = MSG_BASE + 7007,
    MSG_DATASET_METADATA = MSG_BASE + 7008,
    MSG_BUILD_INDEX = MSG_BASE + 7009,
    
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

    void clear() {
        type = -1;
        count = 0;
        if (data != nullptr){
            free(data);
            data = nullptr;
        }
    }

}; 

#endif