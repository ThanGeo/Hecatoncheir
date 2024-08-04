#ifndef D_COMM_PACK
#define D_COMM_PACK

#include "containers.h"
#include "comm_def.h"


namespace pack
{
    DB_STATUS packSystemInfo(SerializedMsg<char> &sysInfoMsg);


    /**
     * @brief Prints a message pack
     * 
     * @tparam T 
     * @param msgPack 
     */
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
    
    /*
     * packs april info into a serialized message 
     */
    DB_STATUS packAPRILInfo(AprilConfig &aprilConfig, SerializedMsg<int> &aprilInfoMsg);

    /**
     * packs query info into a serialized message
     */
    DB_STATUS packQueryInfo(QueryInfo &queryInfo, SerializedMsg<int> &queryInfoMsg);

    /**
     * packs the loaded dataset nicknames into a serialized message 
     * (both R and S, or only R if there's no S)
     */
    DB_STATUS packDatasetsNicknames(SerializedMsg<char> &msg);

    /**
     * @brief packs the current contents of the query output global variable into a serialized message
     * based on query type
     */
    DB_STATUS packQueryResults(SerializedMsg<int> &msg, QueryOutput &queryOutput);
}

namespace unpack
{   

    DB_STATUS unpackSystemInfo(SerializedMsg<char> &sysInfoMsg);

    DB_STATUS unpackAPRILInfo(SerializedMsg<int> &aprilInfoMsg);

    DB_STATUS unpackQueryInfo(SerializedMsg<int> &queryInfoMsg);

    DB_STATUS unpackQueryResults(SerializedMsg<int> &queryResultsMsg, QueryTypeE queryType, QueryOutput &queryOutput);

    DB_STATUS unpackDatasetsNicknames(SerializedMsg<char> &msg, std::vector<std::string> &nicknames);
}

#endif 