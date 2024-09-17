#include "storage/utils.h"

namespace storage
{
    DB_STATUS generatePartitionFilePath(Dataset &dataset) {
        if (dataset.nickname == "") {
            logger::log_error(DBERR_MISSING_DATASET_METADATA, "Dataset nickname is missing, cannot generate partition filepath");
            return DBERR_MISSING_DATASET_METADATA;
        }
        if (g_config.dirPaths.partitionsPath == "") {
            logger::log_error(DBERR_MISSING_PATH, "Partition path is missing");
            return DBERR_MISSING_PATH;
        }
        dataset.path = g_config.dirPaths.partitionsPath + dataset.nickname;
        if (g_config.options.setupType == SYS_CLUSTER) {
            // cluster
            dataset.path += "_" + std::to_string(g_config.partitioningMethod->getGlobalPPD()) + ".dat";
        } else {
            // local machine
            dataset.path += "_Node" + std::to_string(g_parent_original_rank) + "_" + std::to_string(g_config.partitioningMethod->getGlobalPPD()) + ".dat";
        }
        return DBERR_OK;
    }

    DB_STATUS generateAPRILFilePath(Dataset &dataset) {
        if (dataset.nickname == "") {
            logger::log_error(DBERR_MISSING_DATASET_METADATA, "Dataset nickname is missing, cannot generate partition filepath");
            return DBERR_MISSING_DATASET_METADATA;
        }
        if (g_config.dirPaths.approximationPath == "") {
            logger::log_error(DBERR_MISSING_PATH, "Approximation path is missing");
            return DBERR_MISSING_PATH;
        }
        dataset.aprilConfig.filepath = g_config.dirPaths.approximationPath + dataset.nickname + "_APRIL";
        if (dataset.approxType == AT_APRIL) {
            std::string aprilTail = "";
            aprilTail += "_" + std::to_string(dataset.aprilConfig.getN());
            aprilTail += "_" + std::to_string(dataset.aprilConfig.compression);
            aprilTail += "_" + std::to_string(dataset.aprilConfig.partitions) + ".dat";
            dataset.aprilConfig.filepath += aprilTail;
        } else {
            logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Approximation type not supported:", dataset.approxType);
            return DBERR_FEATURE_UNSUPPORTED;
        }
        return DBERR_OK;
    }

    DB_STATUS countLinesInFile(std::string &filepath, size_t &lineCount) {
        if (!verifyFilepath(filepath)) {
            logger::log_error(DBERR_MISSING_FILE, "Count lines for file '", filepath, " ' failed. File is missing.");
            return DBERR_MISSING_FILE;
        }
        lineCount = 0;
        std::string line;
        std::ifstream fin(filepath);
        while (getline(fin, line)) {
            lineCount += 1;
        }
        fin.close();
        return DBERR_OK;
    }
}