#ifndef D_APRIL_STORAGE_H
#define D_APRIL_STORAGE_H

#include "containers.h"

namespace APRIL
{
    /** @brief Methods for storing APRIL on disk. */
    namespace writer
    {
        /** @brief Saves the APRIL data of an object on disk, using the given file pointer.
         * @param[in] pFile File pointer to the already opened APRIL file on disk.
         * @param[in] recID The object's ID.
         * @param[in] sectionID The object's section ID (APRIL partitions).
         * @param[in] aprilData The object's APRIL data.
         */
        DB_STATUS saveAPRIL(FILE* pFile, size_t recID, uint sectionID, AprilData* aprilData);

        /** @brief Saves the APRIL data of all objects in a dataset on disk, using the given file pointer.
         * @param[in] pFile File pointer to the already opened APRIL file on disk.
         * @param[in] dataset Dataset containing a filled section map with the APRIL data to save.
         */
        DB_STATUS saveAPRIL(FILE* pFile, Dataset &dataset);
    }

    /** @brief Methods for loading APRIL from disk. */
    namespace reader
    {
        /** @brief Loads all APRIL data for a dataset from disk. */
        DB_STATUS loadAPRIL(Dataset* dataset);
    }
}

#endif