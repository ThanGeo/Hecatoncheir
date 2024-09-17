#ifndef D_STORAGE_UTILS_H
#define D_STORAGE_UTILS_H

#include "containers.h"
#include "fstream"

/** @brief Disk-related methods for utility operations. */
namespace storage
{
    /**
    @brief Generates the partition filepath for the given dataset, from the dataset's nickname.
     * @warning The nickname field must be set. The path is stored in the path field of the dataset object.
     */
    DB_STATUS generatePartitionFilePath(Dataset &dataset);

    /**
    @brief Generates the APRIL file path for the given dataset from the dataset's nickname.
     * @warning The nickname field must be set. The APRIL path is stored in the AprilConfig field of the dataset object.
     */
    DB_STATUS generateAPRILFilePath(Dataset &dataset);


    /**
    @brief Counts the number of lines in the file of the given filepath.
     */
    DB_STATUS countLinesInFile(std::string &filepath, size_t &lineCount);
}


#endif