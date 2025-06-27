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
    
    // Base case to calculate count
    template<typename T>
    inline int calculateCount() {
        return 0;
    }

    // Recursive variadic template to count arguments
    template<typename T, typename... Args>
    int calculateCount(const T&, const Args&... rest) {
        return 1 + calculateCount<T>(rest...);
    }

    // Base case for buffer writing
    template<typename T>
    inline void addToBuffer(char*&) {}

    // Recursive function to copy values into the buffer
    template<typename T, typename... Args>
    void addToBuffer(char*& buffer, const T& first, const Args&... rest) {
        *reinterpret_cast<T*>(buffer) = first;
        buffer += sizeof(T);
        addToBuffer<T>(buffer, rest...);
    }

    // Main generic pack function — now stores count as header
    template<typename T, typename... Args>
    DB_STATUS packValues(SerializedMsg<char>& msg, const T& first, const Args&... rest) {
        static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
        static_assert((std::is_same<T, Args>::value && ...), "All arguments must be of the same type");

        size_t count = 1 + sizeof...(rest);
        msg.count = sizeof(size_t) + count * sizeof(T);  // total buffer size in bytes

        msg.data = static_cast<char*>(malloc(msg.count));
        if (msg.data == nullptr) {
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack values failed");
            return DBERR_MALLOC_FAILED;
        }

        char* buffer = msg.data;

        // Store count at the beginning
        *reinterpret_cast<size_t*>(buffer) = count;
        buffer += sizeof(size_t);

        // Store the actual data
        addToBuffer<T>(buffer, first, rest...);

        return DBERR_OK;
    }


    /** @brief Packs a vector of type T values into a serialized message */
    template<typename T>
    DB_STATUS packValues(SerializedMsg<char>& msg, const std::vector<T>& values) {
        static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");

        size_t count = values.size();
        msg.count = sizeof(size_t) + count * sizeof(T);

        msg.data = static_cast<char*>(malloc(msg.count));
        if (msg.data == nullptr) {
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for packed values failed");
            return DBERR_MALLOC_FAILED;
        }

        char* buffer = msg.data;

        // Store count
        *reinterpret_cast<size_t*>(buffer) = count;
        buffer += sizeof(size_t);

        // Store values
        std::memcpy(buffer, values.data(), count * sizeof(T));

        return DBERR_OK;
    }


    /** @brief Packs necessary system metadata for broadcast like number of partitions, system setup type etc. */
    DB_STATUS packSystemMetadata(SerializedMsg<char> &sysMetadataMsg);

    DB_STATUS packShape(Shape *shape, SerializedMsg<char> &msg);

    /** @brief Packs the april configuration metadata into a serialized message. */
    DB_STATUS packAPRILMetadata(AprilConfig &aprilConfig, SerializedMsg<char> &aprilMetadataMsg);

    /** @brief Packs a batch of queries into a serialized message. */
    DB_STATUS packQueryBatch(std::vector<hec::Query*> *batch, SerializedMsg<char> &batchMsg);

    /** @brief Packs a batch of results into a serialized message. */
    DB_STATUS packBatchResults(std::unordered_map<int, hec::QResultBase*> &batchResults, SerializedMsg<char> &batchMsg);

    /** @brief Packs the sizes of a border object map for each node. (DISTANCE JOIN) 
     * @warning INVOKED BY AGENTS
    */
    DB_STATUS packBorderObjectSizes(std::unordered_map<int, DJBatch> &borderObjectsMap, SerializedMsg<char> &msg);
}

/** @brief Methos that unpack serialized messages and extract their contents, based on message type. */
namespace unpack
{   
    /** @brief Unpacks a message containg type T values to the given vector of the same type.*/
    template<typename T>
    DB_STATUS unpackValues(const SerializedMsg<char>& msg, std::vector<T>& values) {
        static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");

        if (msg.count < sizeof(size_t)) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Serialized message too small for unpacking");
            return DBERR_INVALID_PARAMETER;
        }

        const char* buffer = msg.data;
        size_t count = *reinterpret_cast<const size_t*>(buffer);
        buffer += sizeof(size_t);

        size_t expected_size = sizeof(size_t) + count * sizeof(T);
        if (msg.count != expected_size) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Serialized message size does not match expected size. Expected:", expected_size, "cnt:", msg.count);
            return DBERR_INVALID_PARAMETER;
        }

        values.resize(count);
        std::memcpy(values.data(), buffer, count * sizeof(T));

        return DBERR_OK;
    }

    /** @brief Unpacks a system metadata serialized message. */
    DB_STATUS unpackSystemMetadata(SerializedMsg<char> &sysMetadataMsg);

    /** @brief Unpacks an APRIL configuration metadata serialized message. */
    DB_STATUS unpackAPRILMetadata(SerializedMsg<char> &aprilMetadataMsg);

    /** @brief Unpacks a query batch message. 
     */
    DB_STATUS unpackQueryBatch(SerializedMsg<char> &msg, std::vector<hec::Query*>* queryBatchPtr);

    /** @brief unpacks a shape appropriately based on its type. */
    DB_STATUS unpackShape(SerializedMsg<char> &msg, Shape &shape);

    /** @brief unpacks a message containg the results of a batch of queries. */
    DB_STATUS unpackBatchResults(SerializedMsg<char> &msg, std::unordered_map<int, hec::QResultBase*> &batchResults);
    
}

#endif 