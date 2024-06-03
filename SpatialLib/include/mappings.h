#ifndef SPATIAL_LIB_MAPPINGS_H
#define SPATIAL_LIB_MAPPINGS_H

#include "def.h"

namespace spatial_lib 
{
    std::string queryTypeIntToText(int val);

    std::string dataTypeIntToText(DataTypeE val);

    DataTypeE dataTypeTextToInt(std::string val);

    FileTypeE fileTypeTextToInt(std::string val);
    
}


#endif