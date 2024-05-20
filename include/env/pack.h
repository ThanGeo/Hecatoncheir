#ifndef D_COMM_PACK
#define D_COMM_PACK

#include "def.h"
#include "comm_def.h"
#include "SpatialLib.h"

namespace pack
{
    /**
     * @brief packs a single polygon of partition ID to packs (info and coords packs)
     * 
     * @param polygon 
     * @param partitionID 
     * @param infoPack 
     * @param coordsPack 
     * @return DB_STATUS 
     */
    DB_STATUS packPolygon(spatial_lib::PolygonT &polygon, int partitionID, MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack);



    /**
     * @brief Prints a message pack
     * 
     * @tparam T 
     * @param msgPack 
     */
    template <typename T> 
    void printMsgPack(MsgPackT<T> &msgPack) {
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
}



#endif 