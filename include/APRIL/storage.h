#ifndef D_APRIL_STORAGE_H
#define D_APRIL_STORAGE_H

#include "def.h"

namespace APRIL
{
    namespace writer
    {
        DB_STATUS saveAPRIL(FILE* pFileALL, FILE* pFileFULL, uint recID, uint sectionID, spatial_lib::AprilDataT* aprilData);
    }
}

#endif