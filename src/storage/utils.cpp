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

    DB_STATUS generateAPRILFilePath(spatial_lib::DatasetT &dataset) {
        if (dataset.nickname == "") {
            logger::log_error(DBERR_MISSING_DATASET_INFO, "Dataset nickname is missing, cannot generate partition filepath");
            return DBERR_MISSING_DATASET_INFO;
        }
        if (g_config.dirPaths.approximationPath == "") {
            logger::log_error(DBERR_MISSING_PATH, "Approximation path is missing");
            return DBERR_MISSING_PATH;
        }
        dataset.aprilConfig.ALL_intervals_path = g_config.dirPaths.approximationPath + dataset.nickname + "_A";
        dataset.aprilConfig.FULL_intervals_path = g_config.dirPaths.approximationPath + dataset.nickname + "_F";
        if (dataset.approxType == spatial_lib::AT_APRIL) {
            std::string aprilTail = "";
            aprilTail += "_" + std::to_string(dataset.aprilConfig.getN());
            aprilTail += "_" + std::to_string(dataset.aprilConfig.compression);
            aprilTail += "_" + std::to_string(dataset.aprilConfig.partitions) + ".dat";
            dataset.aprilConfig.ALL_intervals_path += aprilTail;
            dataset.aprilConfig.FULL_intervals_path += aprilTail;
        } else {
            logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Approximation type not supported:", dataset.approxType);
            return DBERR_FEATURE_UNSUPPORTED;
        }
        return DBERR_OK;
    }
}