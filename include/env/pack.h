#ifndef D_COMM_PACK
#define D_COMM_PACK

#include "def.h"
#include "comm_def.h"
#include "SpatialLib.h"

namespace pack
{
    DB_STATUS packSystemInfo(SerializedMsgT<char> &sysInfoMsg);


    /**
     * @brief Prints a message pack
     * 
     * @tparam T 
     * @param msgPack 
     */
    template <typename T> 
    void printMsgPack(SerializedMsgT<T> &msgPack) {
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
    DB_STATUS packAPRILInfo(spatial_lib::AprilConfigT &aprilConfig, SerializedMsgT<int> &aprilInfoMsg);

    /**
     * packs query info into a serialized message
     */
    DB_STATUS packQueryInfo(QueryInfoT &queryInfo, SerializedMsgT<int> &queryInfoMsg);

    /**
     * packs the loaded dataset nicknames into a serialized message 
     * (both R and S, or only R if there's no S)
     */
    DB_STATUS packDatasetsNicknames(SerializedMsgT<char> &msg);

    /**
     * @brief packs the current contents of the query output global variable into a serialized message
     * based on query type
     */
    DB_STATUS packQueryResults(SerializedMsgT<int> &msg);
}

namespace unpack
{   

    DB_STATUS unpackSystemInfo(SerializedMsgT<char> &sysInfoMsg);

    DB_STATUS unpackAPRILInfo(SerializedMsgT<int> &aprilInfoMsg);

    DB_STATUS unpackQueryInfo(SerializedMsgT<int> &queryInfoMsg);

    DB_STATUS unpackQueryResults(SerializedMsgT<int> &queryResultsMsg, spatial_lib::QueryTypeE queryType, spatial_lib::QueryOutputT &queryOutput);

    DB_STATUS unpackDatasetsNicknames(SerializedMsgT<char> &msg, std::vector<std::string> &nicknames);
}

#endif 