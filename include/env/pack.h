#ifndef D_COMM_PACK
#define D_COMM_PACK

#include "def.h"
#include "comm_def.h"
#include "SpatialLib.h"

namespace pack
{
    /**
     * @brief serializes a vector of strings into a single string.
     * for each string in the vector, its size is also included in the serialized string.
     * each entry is delimited by a whitespace
     * 
     */
    std::string serializeStrings(std::vector<std::string> &strings);

    /**
     * @brief deserializes a string into a vector of its serialized strings
     * 
     * @param serializedString 
     * @param strings 
     * @return DB_STATUS 
     */
    DB_STATUS deserializeStrings(std::string &serializedString, std::vector<std::string> &strings);

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
     * @brief Packs the arrays for a geometry into the MsgPack for send
     * 
     */
    DB_STATUS packGeometryArrays(std::vector<int> &infoArray, std::vector<double> &coordsArray, MsgPackT<int> &infoPack, MsgPackT<double> &coordsPack);

    /**
     * @brief Packs all the necessary dataset info into a message pack for send
     * 
     */
    DB_STATUS packDatasetInfo(spatial_lib::DatasetT &dataset, MsgPackT<char> &datasetInfoPack);

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

namespace unpack
{   
    /**
     * @brief unpacks a dataset info pack and builds a new dataset in the local configuration
     * 
     * @param datasetInfoPack 
     * @return DB_STATUS 
     */
    DB_STATUS unpackDatasetInfoPack(MsgPackT<char> &datasetInfoPack);
}

#endif 