#ifndef D_APRIL_STORAGE_H
#define D_APRIL_STORAGE_H

#include "containers.h"

namespace APRIL
{
    namespace writer
    {
        DB_STATUS saveAPRIL(FILE* pFileALL, FILE* pFileFULL, size_t recID, uint sectionID, AprilData* aprilData);
    }

    namespace reader
    {
        DB_STATUS loadAPRIL(Dataset &dataset);
    }
}

#endif