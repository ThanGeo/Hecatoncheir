#include "storage/utils.h"

namespace storage
{
    /** @todo add a better way to generate names (user defined nickname?) */
    DB_STATUS generatePartitionFilePath(Dataset &dataset) {
        if (g_config.dirPaths.partitionsPath == "") {
            logger::log_error(DBERR_MISSING_PATH, "Partition path is missing");
            return DBERR_MISSING_PATH;
        }
        dataset.metadata.path = g_config.dirPaths.partitionsPath + std::to_string(dataset.metadata.internalID);
        if (g_config.options.setupType == SYS_CLUSTER) {
            // cluster
            dataset.metadata.path += "_" + std::to_string(g_config.partitioningMethod->getGlobalPPD()) + ".dat";
        } else {
            // local machine
            dataset.metadata.path += "_Node" + std::to_string(g_parent_original_rank) + "_" + std::to_string(g_config.partitioningMethod->getGlobalPPD()) + ".dat";
        }
        return DBERR_OK;
    }

    DB_STATUS generateAPRILFilePath(Dataset &dataset) {

        if (g_config.dirPaths.approximationPath == "") {
            logger::log_error(DBERR_MISSING_PATH, "Approximation path is missing");
            return DBERR_MISSING_PATH;
        }
        dataset.aprilConfig.filepath = g_config.dirPaths.approximationPath + std::to_string(dataset.metadata.internalID) + "_APRIL";
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
        DB_STATUS ret = verifyFilepath(filepath); 
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Count lines for file '", filepath, " ' failed. File is missing.");
            return ret;
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