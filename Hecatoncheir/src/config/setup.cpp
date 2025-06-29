#include "config/setup.h"
#include "env/send.h"

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

        // notify driver that all is well
        ret = comm::send::sendResponse(DRIVER_GLOBAL_RANK, MSG_ACK, g_global_intra_comm);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Responding to driver failed");
            return ret;
        }
        
        return ret;
    }
}