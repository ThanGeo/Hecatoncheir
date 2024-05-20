#include "config/configure.h"

namespace configure
{
    DB_STATUS verifySystemDirectories() {
        int ret;
        if (!verifyDirectoryExists(g_config.dirPaths.dataPath) ) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.dataPath.c_str(), 0777);
            if (ret) {
                return DBERR_CREATE_DIR;
            }
        }

        if (!verifyDirectoryExists(g_config.dirPaths.partitionsPath) ) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.partitionsPath.c_str(), 0777);
            if (ret) {
                return DBERR_CREATE_DIR;
            }
        }

        if (!verifyDirectoryExists(g_config.dirPaths.approximationPath) ) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.approximationPath.c_str(), 0777);
            if (ret) {
                return DBERR_CREATE_DIR;
            }
        }

        return DBERR_OK;
    }

    DB_STATUS createConfiguration(SystemOptionsT &sysOps) {
        DB_STATUS ret = DBERR_OK;
        // in a cluster, each node is responsible for its directories.
        if (sysOps.setupType == SYS_CLUSTER) {
            // broadcaste init instruction
            ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_INIT);
            if (ret != DBERR_OK) {
                return ret;
            }
        }
        // local machine or not, the host needs to create/verify directories in current machine
        ret = verifySystemDirectories();
        if (ret != DBERR_OK) {
            return ret;
        }

        // set to global variable
        g_config.options = sysOps;


        return DBERR_OK;
    }
}