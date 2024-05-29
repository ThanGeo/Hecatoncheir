#include "config/setup.h"

namespace setup
{
    DB_STATUS setupSystem(int argc, char* argv[]) {        
        // parse
        DB_STATUS ret = parser::parse(argc, argv);
        if (ret != DBERR_OK) {
            return ret;
        }

        // configure
        ret = configure::createConfiguration();
        if (ret != DBERR_OK) {
            return ret;
        }
        
        return DBERR_OK;
    }
}