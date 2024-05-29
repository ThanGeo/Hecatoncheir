#include "env/pack.h"

namespace pack
{
    DB_STATUS packSystemInfo(SerializedMsgT<char> &sysInfoMsg) {
        sysInfoMsg.count = 0;
        sysInfoMsg.count += sizeof(SystemSetupTypeE);     // cluster or local
        sysInfoMsg.count += sizeof(int);                  // partitions per dimension
        sysInfoMsg.count += sizeof(int);                  // partitioning type
        
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
        return DBERR_OK;
    }
}