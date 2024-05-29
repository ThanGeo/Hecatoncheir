#ifndef D_COMM_DEF_H
#define D_COMM_DEF_H

#include "def.h"
#include <unordered_map>

#define LISTENING_INTERVAL 1

typedef enum NodeType {
    NODE_HOST,
    NODE_WORKER
} NodeTypeE;

/* message types (used as MPI_TAG) */
#define MSG_BASE 100000
typedef enum MsgType {
    /* BASE */
    MSG_ACK = MSG_BASE,
    MSG_NACK = MSG_BASE + 1,
    
    /* INSTRUCTIONS */
    MSG_INSTR_BEGIN = MSG_BASE + 2000,
    MSG_INSTR_INIT = MSG_INSTR_BEGIN,
    
    MSG_INSTR_FIN = MSG_BASE + 2001,
    MSG_INSTR_END,

    /* DATA */
    MSG_DATASET_INFO = MSG_BASE + 3000,

    MSG_DATATYPE_BEGIN = MSG_BASE + 3001,
    MSG_SINGLE_POINT = MSG_DATATYPE_BEGIN,
    MSG_SINGLE_LINESTRING = MSG_BASE + 3002,
    MSG_SINGLE_POLYGON = MSG_BASE + 3003,
    MSG_BATCH_POINT = MSG_BASE + 3004,
    MSG_BATCH_LINESTRING = MSG_BASE + 3005,
    MSG_BATCH_POLYGON = MSG_BASE + 3006,

    MSG_DATATYPE_END,
    
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
struct SerializedMsgT { 
    MPI_Datatype type;      // mpi datatype of message data
    int count = 0;             // number of elements in message
    T *data;                // pointer to the message data

    SerializedMsgT(MPI_Datatype type) {
        this->type = type;
    }
}; 

typedef struct Geometry {
    int recID;
    int partitionID;
    int vertexCount;
    std::vector<double> coords;

    Geometry(int recID, int partitionID, int vertexCount, std::vector<double> &coords) {
        this->recID = recID;
        this->partitionID = partitionID;
        this->vertexCount = vertexCount;
        this->coords = coords;
    }

    void setPartitionID(int partitionID) {
        this->partitionID = partitionID;
    }

} GeometryT;

typedef struct GeometryBatch {
    // serializable
    int objectCount = 0;
    std::vector<GeometryT> geometries;
    // unserializable/unclearable (todo: make const?)
    int destRank = -1;   // destination node rank
    int maxObjectCount; 
    MPI_Comm* comm; // communicator that the batch will be send through
    int tag = -1;        // MPI tag = indicates spatial data type

    bool isValid() {
        return !(destRank == -1 || tag == -1 || comm == nullptr);
    }

    void addGeometryToBatch(GeometryT &geometry) {
        geometries.emplace_back(geometry);
        objectCount += 1;
    }

    void setDestNodeRank(int destRank) {
        this->destRank = destRank;
    }

    // calculate the size needed for the serialization buffer
    int calculateBufferSize() {
        int size = 0;
        size += sizeof(int);                        // objectCount
        size += 3 * objectCount * sizeof(int);      // objectCount * (rec ID,partition ID,vertex count)
        
        for (auto &it: geometries) {
            size += it.vertexCount * 2 * sizeof(double);
        }
        
        return size;
    }

    void clear() {
        objectCount = 0;
        geometries.clear();
    }

    int serialize(char **buffer) {
        // calculate size
        int bufferSize = calculateBufferSize();
        // allocate space
        (*buffer) = (char*) malloc(bufferSize * sizeof(char));
        if (*buffer == NULL) {
            // malloc failed
            return -1;
        }
        char* localBuffer = *buffer;

        // add object count
        *reinterpret_cast<int*>(localBuffer) = objectCount;
        localBuffer += sizeof(int);

        // add batch geometry info
        for (auto &it : geometries) {
            *reinterpret_cast<int*>(localBuffer) = it.recID;
            localBuffer += sizeof(int);
            *reinterpret_cast<int*>(localBuffer) = it.partitionID;
            localBuffer += sizeof(int);
            *reinterpret_cast<int*>(localBuffer) = it.vertexCount;
            localBuffer += sizeof(int);

            // double* vertexDataPtr = reinterpret_cast<double*>(localBuffer);
            // for (int i=0; i<it.vertexCount*2; i++) {
            //     vertexDataPtr[i] = it.coords[i];
            // }
            // localBuffer += it.vertexCount * 2 * sizeof(double);

            std::memcpy(localBuffer, it.coords.data(), it.vertexCount * 2 * sizeof(double));
            localBuffer += it.vertexCount * 2 * sizeof(double);
        }

        return bufferSize;
    }

    /**
     * @brief fills the struct with data from the input serialized buffer
     * The caller must free the buffer memory
     * @param buffer 
     */
    void deserialize(const char *buffer, int bufferSize) {
        int recID, partitionID, vertexCount;
        const char *localBuffer = buffer;

        // get object count
        int objectCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        // extend reserve space
        geometries.reserve(geometries.size() + objectCount);

        for (int i=0; i<objectCount; i++) {
            // deserialize fields
            recID = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            partitionID = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            vertexCount = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);

            std::vector<double> coords(vertexCount * 2);
            const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
            for (size_t j = 0; j < vertexCount * 2; ++j) {
                coords[j] = vertexDataPtr[j];
            }
            localBuffer += vertexCount * 2 * sizeof(double);
            
            // add to batch
            GeometryT geometry(recID, partitionID, vertexCount, coords);
            this->addGeometryToBatch(geometry);
        }
    }

} GeometryBatchT;

#endif