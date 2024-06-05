#include "env/pack.h"

namespace pack
{
    DB_STATUS packSystemInfo(SerializedMsgT<char> &sysInfoMsg) {
        sysInfoMsg.count = 0;
        sysInfoMsg.count += sizeof(SystemSetupTypeE);     // cluster or local
        sysInfoMsg.count += sizeof(int);                  // partitions per dimension
        sysInfoMsg.count += sizeof(int);                  // partitioning type
        sysInfoMsg.count += sizeof(int) + g_config.dirPaths.dataPath.length() * sizeof(char);   // data path length + string
        
        // allocate space
        (sysInfoMsg.data) = (char*) malloc(sysInfoMsg.count * sizeof(char));
        if (sysInfoMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system info failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = sysInfoMsg.data;

        // put objects in buffer
        *reinterpret_cast<SystemSetupTypeE*>(localBuffer) = g_config.options.setupType;
        localBuffer += sizeof(SystemSetupTypeE);
        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningInfo.partitionsPerDimension;
        localBuffer += sizeof(int);
        *reinterpret_cast<PartitioningTypeE*>(localBuffer) = g_config.partitioningInfo.type;
        localBuffer += sizeof(PartitioningTypeE);
        *reinterpret_cast<int*>(localBuffer) = g_config.dirPaths.dataPath.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, g_config.dirPaths.dataPath.data(), g_config.dirPaths.dataPath.length() * sizeof(char));
        localBuffer += g_config.dirPaths.dataPath.length() * sizeof(char);

        return DBERR_OK;
    }
}

namespace unpack
{
    DB_STATUS unpackSystemInfo(SerializedMsgT<char> &sysInfoMsg) {
        char *localBuffer = sysInfoMsg.data;
        // get system setup type
        g_config.options.setupType = *reinterpret_cast<const SystemSetupTypeE*>(localBuffer);
        localBuffer += sizeof(SystemSetupTypeE);
        // get partitions per dimension
        g_config.partitioningInfo.partitionsPerDimension = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // get partitioning type
        g_config.partitioningInfo.type = *reinterpret_cast<const PartitioningTypeE*>(localBuffer);
        localBuffer += sizeof(PartitioningTypeE);
        // get datapath length + string
        // and set the paths
        int length;
        length = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        std::string datapath(localBuffer, localBuffer + length);
        localBuffer += length * sizeof(char);
        g_config.dirPaths.setNodeDataDirectories(datapath);

        return DBERR_OK;
    }
}