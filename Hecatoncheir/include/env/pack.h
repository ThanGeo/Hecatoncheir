#ifndef D_COMM_PACK
#define D_COMM_PACK

#include "containers.h"
#include "comm_def.h"
#include "../API/containers.h"

/** @brief Methods that pack information for MPI communication into serialized messages. */
namespace pack
{
    /** @brief Prints the contents of a message pack. */
    template <typename T> 
    void printMsgPack(SerializedMsg<T> &msgPack) {
        std::cout << "Message:" << std::endl;
        std::cout << "\ttype: " << g_MPI_Datatype_map[msgPack.type] << std::endl;
        std::cout << "\tsize: " << msgPack.count << std::endl;
        if (msgPack.count > 0) {
            std::cout << "\tdata: ";
            std::cout << msgPack.data[0];
            for(int i=1; i<msgPack.count; i++) {
                std::cout << "," << msgPack.data[i];
            }
            std::cout << std::endl;
        }
    }

    // Base function to calculate total size (terminates recursion)
    inline int calculateCountINT() {
        return 0;
    }

    // Variadic function to calculate the total size of integers
    template <typename... Args>
    int calculateCountINT(int first, Args... rest) {
        return 1 + calculateCountINT(rest...);
    }

    // Base function to add integers to the buffer (terminates recursion)
    inline void addToBuffer(int*&) {}

    // Variadic function to add integers to the buffer
    template <typename... Args>
    void addToBuffer(int*& buffer, int first, Args... rest) {
        *reinterpret_cast<int*>(buffer) = first; // Add the first integer
        buffer += 1;                            // Move to the next position
        addToBuffer(buffer, rest...);           // Recur for remaining integers
    }

    // The new method
    template <typename... Args>
    DB_STATUS packIntegers(SerializedMsg<int>& msg, Args... integers) {
        // Calculate the total size of all integers
        int count = calculateCountINT(integers...);
        // Allocate memory for the message
        msg.count = count;
        msg.data = (int*)malloc(msg.count * sizeof(int));
        if (msg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack dataset integers failed");
            return DBERR_MALLOC_FAILED;
        }

        int* localBuffer = msg.data;

        // Add the integers to the buffer
        addToBuffer(localBuffer, integers...);

        return DBERR_OK;
    }

    /** @brief Packs a vector of integers into a serialized message */
    DB_STATUS packIntegers(SerializedMsg<int>& msg, std::vector<int> integers);

    /** @brief Packs necessary system metadata for broadcast like number of partitions, system setup type etc. */
    DB_STATUS packSystemMetadata(SerializedMsg<char> &sysMetadataMsg);

    /** @brief Packs a vector of dataset indexes into a single message */
    DB_STATUS packDatasetIndexes(std::vector<int> indexes, SerializedMsg<int> &msg);

    DB_STATUS packShape(Shape *shape, SerializedMsg<char> &msg);

    /** @brief Packs the april configuration metadata into a serialized message. */
    DB_STATUS packAPRILMetadata(AprilConfig &aprilConfig, SerializedMsg<int> &aprilMetadataMsg);

    /** @brief Packs the 'load dataset' message information for the given dataset and index (R or S) */
    DB_STATUS packDatasetLoadMsg(Dataset *dataset, DatasetIndex datasetIndex, SerializedMsg<char> &msg);

    /** @brief Packs the query metadata into a serialized message. */
    DB_STATUS packQuery(hec::Query *query, SerializedMsg<char> &msg);

    /** @brief Packs a batch of queries into a serialized message. */
    DB_STATUS packQueryBatch(std::vector<hec::Query*> *batch, SerializedMsg<char> &batchMsg);

    /** @brief Packs a batch of results into a serialized message. */
    DB_STATUS packBatchResults(std::unordered_map<int, hec::QResultBase*> &batchResults, SerializedMsg<char> &batchMsg);

    DB_STATUS packBatchResults(std::unordered_map<int, std::unique_ptr<hec::QResultBase>> &batchResults, SerializedMsg<char> &msg);
}

/** @brief Methos that unpack serialized messages and extract their contents, based on message type. */
namespace unpack
{   
    /** @brief Unpacks a message containg integers to the given vector. The integer count is stored in the message count. */
    DB_STATUS unpackIntegers(SerializedMsg<int> &integersMsg, std::vector<int> &integers);

    /** @brief Unpacks a system metadata serialized message. */
    DB_STATUS unpackSystemMetadata(SerializedMsg<char> &sysMetadataMsg);

    /** @brief Unpacks an APRIL configuration metadata serialized message. */
    DB_STATUS unpackAPRILMetadata(SerializedMsg<int> &aprilMetadataMsg);

    /** @brief Unpacks an query metadata serialized message. */
    DB_STATUS unpackQueryMetadata(SerializedMsg<int> &queryMetadataMsg);

    /** @brief Unpacks a serialized message containing a single dataset index. */
    DB_STATUS unpackDatasetIndexes(SerializedMsg<int> &msg, std::vector<int> &datasetIndexes);

    /** @brief Unpacks a datasets' nicknames serialized message. */
    DB_STATUS unpackDatasetsNicknames(SerializedMsg<char> &msg, std::vector<std::string> &nicknames);

    /** @brief Unpacks a 'dataset load' message. Results are stored in the dataset and datasetIndex arguments. */
    DB_STATUS unpackDatasetLoadMsg(SerializedMsg<char> &msg, Dataset &dataset, DatasetIndex &datasetIndex);

    /** @brief Unpacks a query message. Results are stored in the pointer of the address, casted to the appropriate type. 
     * The caller is responsible to call delete for the queryPtr after they are done with it.
    */
    DB_STATUS unpackQuery(SerializedMsg<char> &msg, hec::Query** queryPtr);

    /** @brief Unpacks a query batch message. 
     */
    DB_STATUS unpackQueryBatch(SerializedMsg<char> &msg, std::vector<hec::Query*>* queryBatchPtr);

    /** @brief unpacks a shape appropriately based on its type. */
    DB_STATUS unpackShape(SerializedMsg<char> &msg, Shape &shape);


    /** @brief unpacks a message containg the results of a batch of queries. */
    DB_STATUS unpackBatchResults(SerializedMsg<char> &msg, std::unordered_map<int, hec::QResultBase*> &batchResults);
    
}

#endif 