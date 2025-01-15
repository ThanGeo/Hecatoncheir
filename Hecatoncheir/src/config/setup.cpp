#include "config/setup.h"

namespace setup
{
    DB_STATUS setupSystem(int argc, char* argv[]) {
        // parse
        DB_STATUS ret = parser::parseDefault();
        if (ret != DBERR_OK) {
            return ret;
        }

        // configure
        ret = configurer::createConfiguration();
        if (ret != DBERR_OK) {
            return ret;
        }
        
        return ret;
    }
}