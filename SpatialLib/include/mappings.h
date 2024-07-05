#ifndef SPATIAL_LIB_MAPPINGS_H
#define SPATIAL_LIB_MAPPINGS_H

#include "def.h"

namespace spatial_lib 
{
    namespace mapping
    {
        std::string queryTypeIntToStr(int val);
        int queryTypeStrToInt(std::string &str);

        std::string dataTypeIntToStr(DataTypeE val);

        DataTypeE dataTypeTextToInt(std::string str);

        FileTypeE fileTypeTextToInt(std::string str);

        std::string datatypeCombinationIntToStr(DatatypeCombinationE val);

        std::string relationIntToStr(int relation);
    }
    
}


#endif