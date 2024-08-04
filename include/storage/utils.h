#ifndef D_STORAGE_UTILS_H
#define D_STORAGE_UTILS_H


#include "containers.h"

namespace storage
{
    /**
     * @brief generates the partition file path for the given dataset from the dataset's nickname.
     * @warning The nickname field must be set. The path is stored in the path field of the dataset object.
     */
    DB_STATUS generatePartitionFilePath(Dataset &dataset);

    /**
     * @brief generates the APRIL file path for the given dataset from the dataset's nickname.
     * @warning The nickname field must be set. The APRIL path is stored in the AprilConfig field of the dataset object.
     */
    DB_STATUS generateAPRILFilePath(Dataset &dataset);
}


#endif