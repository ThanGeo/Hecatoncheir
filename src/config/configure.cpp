#include "config/configure.h"

namespace configurer
{
    DB_STATUS initMPI(int argc, char* argv[]) {
        char c;
        int provided;
        int rank, wsize;
        // init MPI
        int mpi_ret = MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init MPI failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        // process rank
        mpi_ret = MPI_Comm_rank(g_global_comm, &rank);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm rank failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_node_rank = rank;
        // get parent process intercomm (must be null)
        MPI_Comm_get_parent(&g_local_comm);
        if (g_local_comm != MPI_COMM_NULL) {
            // logger::log_error(DBERR_PROC_INIT_FAILED, "Controllers must be parentless");
            // return DBERR_PROC_INIT_FAILED;
        }
        g_parent_original_rank = PARENTLESS_RANK;
        // world size
        mpi_ret = MPI_Comm_size(g_global_comm, &wsize);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm size failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_world_size = wsize;
        return DBERR_OK;
    }

    DB_STATUS verifySystemDirectories() {
        int ret;
        if (!verifyDirectory(g_config.dirPaths.dataPath) ) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.dataPath.c_str(), 0777);
            if (ret) {
                logger::log_error(DBERR_CREATE_DIR, "Error creating node data directory. Path:", g_config.dirPaths.dataPath);
                return DBERR_CREATE_DIR;
            }
        }

        if (!verifyDirectory(g_config.dirPaths.partitionsPath) ) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.partitionsPath.c_str(), 0777);
            if (ret) {
                logger::log_error(DBERR_CREATE_DIR, "Error creating partitioned data directory");
                return DBERR_CREATE_DIR;
            }
        }
        if (!verifyDirectory(g_config.dirPaths.approximationPath) ) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.approximationPath.c_str(), 0777);
            if (ret) {
                logger::log_error(DBERR_CREATE_DIR, "Error creating approximations data directory");
                return DBERR_CREATE_DIR;
            }
        }
        return DBERR_OK;
    }

    DB_STATUS createConfiguration() {
        DB_STATUS ret = DBERR_OK;
        logger::log_task("*** Action: SETUP SYSTEM");
        // broadcast system info
        ret = comm::controller::broadcastSysInfo();
        if (ret != DBERR_OK) {
            return ret;
        }

        // wait for response by workers+agent that all is ok
        ret = comm::controller::host::gatherResponses();
        if (ret != DBERR_OK) {
            return ret;
        }

        return DBERR_OK;
    }


    DB_STATUS setDatasetInfo(DatasetStatement* datasetStmt) {
        if (datasetStmt->datasetCount == 0) {
            // no datasets
            g_config.datasetInfo.clear();
            return DBERR_OK;
        } else {
            // at least one dataset
            Dataset R;
            // set dataspace info
            if (datasetStmt->boundsSet) {
                g_config.datasetInfo.dataspaceInfo.xMinGlobal = datasetStmt->xMinGlobal;
                g_config.datasetInfo.dataspaceInfo.yMinGlobal = datasetStmt->yMinGlobal;
                g_config.datasetInfo.dataspaceInfo.xMaxGlobal = datasetStmt->xMaxGlobal;
                g_config.datasetInfo.dataspaceInfo.yMaxGlobal = datasetStmt->yMaxGlobal;
                g_config.datasetInfo.dataspaceInfo.xExtent = g_config.datasetInfo.dataspaceInfo.xMaxGlobal - g_config.datasetInfo.dataspaceInfo.xMinGlobal;
                g_config.datasetInfo.dataspaceInfo.yExtent = g_config.datasetInfo.dataspaceInfo.yMaxGlobal - g_config.datasetInfo.dataspaceInfo.yMinGlobal;
                g_config.datasetInfo.dataspaceInfo.boundsSet = true;
            }
            
            // build dataset R objects
            R.dataspaceInfo = g_config.datasetInfo.dataspaceInfo;
            R.dataType = datasetStmt->datatypeR;

            DB_STATUS ret = statement::getFiletype(datasetStmt->filetypeR, R.fileType);
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_INVALID_FILETYPE, "Unknown filetype for dataset", datasetStmt->datasetNicknameR);
                return DBERR_INVALID_FILETYPE;
            }

            R.path = datasetStmt->datasetPathR;
            R.nickname = datasetStmt->datasetNicknameR;
            R.datasetName = getFileNameFromPath(R.path);
            // add to config
            g_config.datasetInfo.addDataset(R);
            
            if (datasetStmt->datasetCount == 2) {
                Dataset S;
                // build dataset S objects (inherit the same dataspace info)
                S.dataspaceInfo = g_config.datasetInfo.dataspaceInfo;
                S.dataType = datasetStmt->datatypeS;

                ret = statement::getFiletype(datasetStmt->filetypeS, S.fileType);
                if (ret != DBERR_OK) {
                    logger::log_error(DBERR_INVALID_FILETYPE, "Unknown filetype for dataset", datasetStmt->datasetNicknameS);
                    return DBERR_INVALID_FILETYPE;
                }

                S.path = datasetStmt->datasetPathS;
                S.nickname = datasetStmt->datasetNicknameS;
                S.datasetName = getFileNameFromPath(S.path);
                // add to config
                g_config.datasetInfo.addDataset(S);
            }
        }
        return DBERR_OK;
    }
}