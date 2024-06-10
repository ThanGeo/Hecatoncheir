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
}

namespace unpack
{   

    DB_STATUS unpackSystemInfo(SerializedMsgT<char> &sysInfoMsg);

    DB_STATUS unpackAPRILInfo(SerializedMsgT<int> &aprilInfoMsg);
}

#endif 