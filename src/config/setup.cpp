#include "config/setup.h"

namespace setup
{
    DB_STATUS setupSystem(int argc, char* argv[]) {
        SystemOptionsT sysOps;
        
        // parse
        DB_STATUS ret = parser::parse(argc, argv, sysOps);
        if (ret != DBERR_OK) {
            return ret;
        }

        // configure
        ret = configure::createConfiguration(sysOps);
        if (ret != DBERR_OK) {
            return ret;
        }
        
        return DBERR_OK;
    }
}