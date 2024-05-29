#include "storage/utils.h"

namespace storage
{
    DB_STATUS generatePartitionFilePath(spatial_lib::DatasetT &dataset) {
        if (dataset.nickname == "") {
            logger::log_error(DBERR_MISSING_DATASET_INFO, "Dataset nickname is missing, cannot generate partition filepath");
            return DBERR_MISSING_DATASET_INFO;
        }
        if (g_config.dirPaths.partitionsPath == "") {
            logger::log_error(DBERR_MISSING_PATH, "Partition path is missing");
            return DBERR_MISSING_PATH;
        }
        dataset.path = g_config.dirPaths.partitionsPath + dataset.nickname;
        if (g_config.options.setupType == SYS_CLUSTER) {
            // cluster
            dataset.path += "_" + std::to_string(g_config.partitioningInfo.partitionsPerDimension) + ".dat";
        } else {
            // local machine
            dataset.path += "_Node" + std::to_string(g_parent_original_rank) + "_" + std::to_string(g_config.partitioningInfo.partitionsPerDimension) + ".dat";
        }
        return DBERR_OK;
    }
}