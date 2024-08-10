#ifndef D_APRIL_STORAGE_H
#define D_APRIL_STORAGE_H

#include "containers.h"

namespace APRIL
{
    /** @brief Methods for storing APRIL on disk. */
    namespace writer
    {
        /** @brief Saves the APRIL data of an object on disk, using the given file pointers.
         * @param[in] pFileALL File pointer to the already opened A-list file.
         * @param[in] pFileFULL File pointer to the already opened F-list file.
         * @param[in] recID The object's ID.
         * @param[in] sectionID The object's section ID (APRIL partitions).
         * @param[in] aprilData The object's APRIL data.
         */
        DB_STATUS saveAPRILForObject(FILE* pFileALL, FILE* pFileFULL, size_t recID, uint sectionID, AprilData* aprilData);

        /** @brief Saves the APRIL data of an object on disk, using the given file pointers.
         * @param[in] pFileALL File pointer to the already opened A-list file.
         * @param[in] pFileFULL File pointer to the already opened F-list file.
         * @param[in] dataset Dataset containing a filled section map with the APRIL data to save.
         */
        DB_STATUS saveAPRILForDataset(FILE* pFileALL, FILE* pFileFULL, Dataset &dataset);
    }

    /** @brief Methods for loading APRIL from disk. */
    namespace reader
    {
        /** @brief Loads all APRIL data for a dataset from disk. */
        DB_STATUS loadAPRIL(Dataset &dataset);
    }
}

#endif