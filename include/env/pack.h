#ifndef D_COMM_PACK
#define D_COMM_PACK

#include "containers.h"
#include "comm_def.h"

/** @brief Methods that pack information for MPI communication into serialized messages. */
namespace pack
{
    /** @brief Packs necessary system info for broadcast like number of partitions, system setup type etc. */
    DB_STATUS packSystemInfo(SerializedMsg<char> &sysInfoMsg);

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
    
    /** @brief Packs the april configuration info into a serialized message. */
    DB_STATUS packAPRILInfo(AprilConfig &aprilConfig, SerializedMsg<int> &aprilInfoMsg);

    /** @brief Packs the query info into a serialized message. */
    DB_STATUS packQueryInfo(QueryInfo &queryInfo, SerializedMsg<int> &queryInfoMsg);

    /** @brief Packs the loaded dataset nicknames (both R and S, or only R if there's no S)
     * @note The datasets must be already loaded in the configuration.
     */
    DB_STATUS packDatasetsNicknames(SerializedMsg<char> &msg);

    /** @brief Packs the 'load dataset' message information for the given dataset and index (R or S) */
    DB_STATUS packDatasetLoadMsg(Dataset *dataset, DatasetIndexE datasetIndex, SerializedMsg<char> &msg);

    /** @brief Packs the query results based on query type.  */
    DB_STATUS packQueryResults(SerializedMsg<int> &msg, QueryOutput &queryOutput);
}

/** @brief Methos that unpack serialized messages and extract their contents, based on message type. */
namespace unpack
{   
    /** @brief Unpacks a system info serialized message. */
    DB_STATUS unpackSystemInfo(SerializedMsg<char> &sysInfoMsg);

    /** @brief Unpacks an APRIL configuration info serialized message. */
    DB_STATUS unpackAPRILInfo(SerializedMsg<int> &aprilInfoMsg);

    /** @brief Unpacks an query info serialized message. */
    DB_STATUS unpackQueryInfo(SerializedMsg<int> &queryInfoMsg);

    /** @brief Unpacks a query results serialized message. */
    DB_STATUS unpackQueryResults(SerializedMsg<int> &queryResultsMsg, QueryTypeE queryType, QueryOutput &queryOutput);

    /** @brief Unpacks a datasets' nicknames serialized message. */
    DB_STATUS unpackDatasetsNicknames(SerializedMsg<char> &msg, std::vector<std::string> &nicknames);

    /** @brief Unpacks a 'dataset load' message. Results are stored in the dataset and datasetIndex arguments. */
    DB_STATUS unpackDatasetLoadMsg(SerializedMsg<char> &msg, Dataset &dataset, DatasetIndexE &datasetIndex);
}

#endif 