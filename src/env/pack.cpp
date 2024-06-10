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


    DB_STATUS packAPRILInfo(spatial_lib::AprilConfigT &aprilConfig, SerializedMsgT<int> &aprilInfoMsg) {
        aprilInfoMsg.count = 0;
        aprilInfoMsg.count += 3;     // N, compression, partitions

        // allocate space
        (aprilInfoMsg.data) = (int*) malloc(aprilInfoMsg.count * sizeof(int));
        if (aprilInfoMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for april info failed");
            return DBERR_MALLOC_FAILED;
        }

        // put objects in buffer
        aprilInfoMsg.data[0] = aprilConfig.getN();
        aprilInfoMsg.data[1] = aprilConfig.compression;
        aprilInfoMsg.data[2] = aprilConfig.partitions;

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

    DB_STATUS unpackAPRILInfo(SerializedMsgT<int> &aprilInfoMsg) {
        // N
        int N = aprilInfoMsg.data[0];
        g_config.approximationInfo.aprilConfig.setN(N);
        // compression
        g_config.approximationInfo.aprilConfig.compression = aprilInfoMsg.data[1];
        // partitions
        g_config.approximationInfo.aprilConfig.partitions = aprilInfoMsg.data[2];
        // set type
        g_config.approximationInfo.type = spatial_lib::AT_APRIL;
        for (auto& it: g_config.datasetInfo.datasets) {
            it.second.approxType = spatial_lib::AT_APRIL;
            it.second.aprilConfig.setN(N);
            it.second.aprilConfig.compression = g_config.approximationInfo.aprilConfig.compression;
            it.second.aprilConfig.partitions = g_config.approximationInfo.aprilConfig.partitions;
        }

        return DBERR_OK;
    }
}