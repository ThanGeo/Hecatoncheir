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
    int destRank;
    int maxObjectCount;

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

    /**
     * @brief serialize the batch into the given buffer.
     * returns the serialized buffer size
     * Caller is responsible for freeing the buffer memory
     * 
     * @param buffer 
     */
    int serialize(char **buffer) {
        int position = 0;
        // calculate size
        int bufferSize = calculateBufferSize();
        // allocate space
        char* localBuffer = (char*) malloc(bufferSize * sizeof(char));

        if (localBuffer == NULL) {
            // malloc failed
            return -1;
        }

        // add object count
        memcpy(localBuffer + position, &objectCount, sizeof(int));
        position += sizeof(int);

        // add batch geometry info
        for (auto &it : geometries) {
            memcpy(localBuffer + position, &it.recID, sizeof(int));
            position += sizeof(int);
            memcpy(localBuffer + position, &it.partitionID, sizeof(int));
            position += sizeof(int);
            memcpy(localBuffer + position, &it.vertexCount, sizeof(int));
            position += sizeof(int);
            memcpy(localBuffer + position, it.coords.data(), it.vertexCount * 2 * sizeof(double));
            position += it.vertexCount * 2 * sizeof(double);
        }

        // set and return
        (*buffer) = localBuffer;

        return bufferSize;
    }

    /**
     * @brief fills the struct with data from the input serialized buffer
     * The caller must free the buffer memory
     * @param buffer 
     */
    void deserialize(const char *buffer, int bufferSize) {
        int recID, partitionID, vertexCount;
        int position = 0;
        
        // get object count
        int objectCount = 0;
        memcpy(&objectCount, buffer + position, sizeof(int));
        position += sizeof(int);

        for (int i=0; i<objectCount; i++) {
            std::vector<double> coords;
            
            // deserialize fields
            memcpy(&recID, buffer + position, sizeof(int));
            position += sizeof(int);
            memcpy(&partitionID, buffer + position, sizeof(int));
            position += sizeof(int);
            memcpy(&vertexCount, buffer + position, sizeof(int));
            position += sizeof(int);

            int coordsLength = vertexCount * 2 * sizeof(double);
            coords.insert(coords.end(), buffer + position, buffer + position + coordsLength);
            position += coordsLength;

            // add to batch
            GeometryT geometry(recID, partitionID, vertexCount, coords);
            this->addGeometryToBatch(geometry);
        }
    }

} GeometryBatchT;

#endif