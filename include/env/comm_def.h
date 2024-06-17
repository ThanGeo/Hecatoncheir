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
#define MSG_BASE 0
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
    MSG_DATASET_INFO = MSG_BATCH_BEGIN,
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

    /* DATA */
    MSG_LOAD_DATASETS = MSG_BASE + 7000,
    MSG_UNLOAD_DATASETS = MSG_BASE + 7001,
    MSG_LOAD_APRIL = MSG_BASE + 7003,
    MSG_UNLOAD_APRIL = MSG_BASE + 7004,
    
    /* ERRORS */
    MSG_ERR_BEGIN = MSG_BASE + 10000,
    MSG_ERR = MSG_ERR_BEGIN,

    MSG_ERR_END,
}MsgTypeE;

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

    SerializedMsgT() {
        if constexpr (std::is_same<T,char>::value) {
            this->type = MPI_CHAR;
        } else if constexpr (std::is_same<T,int>::value) {
            this->type = MPI_INT;
        } else {
            printf("Error: unknown serialized msg type");
        }
    }

}; 

typedef struct Geometry {
    int recID;
    int partitionCount;
    std::vector<int> partitionIDs;
    int vertexCount;
    std::vector<double> coords;

    Geometry(int recID, int vertexCount, std::vector<double> &coords) {
        this->recID = recID;
        this->vertexCount = vertexCount;
        this->coords = coords;
    }

    Geometry(int recID, std::vector<int> &partitionIDs, int vertexCount, std::vector<double> &coords) {
        this->recID = recID;
        this->partitionIDs = partitionIDs;
        this->partitionCount = partitionIDs.size(); 
        this->vertexCount = vertexCount;
        this->coords = coords;
    }


    void setPartitionIDs(std::vector<int> &ids) {
        this->partitionIDs = ids;
        this->partitionCount = ids.size();
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
        
        for (auto &it: geometries) {
            size += sizeof(it.recID);
            size += sizeof(int);    // partition count
            size += it.partitionIDs.size() * sizeof(int); // partition ids
            size += sizeof(it.vertexCount); // vertex count
            size += it.vertexCount * 2 * sizeof(double);    // vertices
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
            *reinterpret_cast<int*>(localBuffer) = it.partitionCount;
            localBuffer += sizeof(int);
            std::memcpy(localBuffer, it.partitionIDs.data(), it.partitionIDs.size() * sizeof(int));
            localBuffer += it.partitionIDs.size() * sizeof(int);
            *reinterpret_cast<int*>(localBuffer) = it.vertexCount;
            localBuffer += sizeof(int);
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
        int recID, vertexCount, partitionCount;
        const char *localBuffer = buffer;

        // get object count
        int objectCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        // extend reserve space
        geometries.reserve(geometries.size() + objectCount);

        // deserialize fields for each object in the batch
        for (int i=0; i<objectCount; i++) {
            // rec id
            recID = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            // partition count
            partitionCount = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            // partitions ids
            std::vector<int> partitionIDs(partitionCount);
            const int* partitionPtr = reinterpret_cast<const int*>(localBuffer);
            for (size_t j = 0; j < partitionCount; ++j) {
                partitionIDs[j] = partitionPtr[j];
            }
            localBuffer += partitionCount * sizeof(int);
            // vertex count
            vertexCount = *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);
            // vertices
            std::vector<double> coords(vertexCount * 2);
            const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
            for (size_t j = 0; j < vertexCount * 2; ++j) {
                coords[j] = vertexDataPtr[j];
            }
            localBuffer += vertexCount * 2 * sizeof(double);
            
            // add to batch
            GeometryT geometry(recID, partitionIDs, vertexCount, coords);
            this->addGeometryToBatch(geometry);
        }
    }

} GeometryBatchT;

#endif