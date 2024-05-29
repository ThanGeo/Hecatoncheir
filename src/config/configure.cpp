#include "config/configure.h"

namespace configure
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
        mpi_ret = MPI_Comm_size(g_global_comm, &wsize);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm size failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        mpi_ret = MPI_Comm_rank(g_global_comm, &rank);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm rank failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_world_size = wsize;
        g_node_rank = rank;
        return DBERR_OK;
    }

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

    DB_STATUS createConfiguration() {
        DB_STATUS ret = DBERR_OK;
        // in a cluster, each node is responsible to verify its own directories.
        if (g_config.options.setupType == SYS_CLUSTER) {
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

        // broadcast system info
        ret = comm::controller::broadcastSysInfo();
        if (ret != DBERR_OK) {
            return ret;
        }

        return DBERR_OK;
    }


    DB_STATUS setDatasetInfo(DatasetStatementT* datasetStmt) {
        if (datasetStmt->datasetCount == 0) {
            // no datasets
            g_config.datasetInfo.numberOfDatasets = 0;
            return DBERR_OK;
        } else {
            // at least one dataset
            spatial_lib::DatasetT R;
            // get dataspace info
            g_config.datasetInfo.dataspaceInfo.xMinGlobal = datasetStmt->xMinGlobal;
            g_config.datasetInfo.dataspaceInfo.yMinGlobal = datasetStmt->yMinGlobal;
            g_config.datasetInfo.dataspaceInfo.xMaxGlobal = datasetStmt->xMaxGlobal;
            g_config.datasetInfo.dataspaceInfo.yMaxGlobal = datasetStmt->yMaxGlobal;
            g_config.datasetInfo.dataspaceInfo.xExtent = g_config.datasetInfo.dataspaceInfo.xMaxGlobal - g_config.datasetInfo.dataspaceInfo.xMinGlobal;
            g_config.datasetInfo.dataspaceInfo.yExtent = g_config.datasetInfo.dataspaceInfo.yMaxGlobal - g_config.datasetInfo.dataspaceInfo.yMinGlobal;
            
            // build dataset R objects
            R.dataspaceInfo = g_config.datasetInfo.dataspaceInfo;
            R.dataType = datasetStmt->datatypeR;

            auto it = parser::fileTypeStrToIntMap.find(datasetStmt->filetypeR);
            if (it == parser::fileTypeStrToIntMap.end()) {
                logger::log_error(DBERR_INVALID_FILETYPE, "Unknown filetype for dataset", datasetStmt->datasetNicknameR, ":", datasetStmt->filetypeR);
                return DBERR_INVALID_FILETYPE;
            }
            R.fileType = it->second;

            R.path = datasetStmt->datasetPathR;
            R.nickname = datasetStmt->datasetNicknameR;
            R.offsetMapPath = datasetStmt->offsetMapPathR;
            R.datasetName = getDatasetNameFromPath(R.path);
            // add to config
            g_config.datasetInfo.addDataset(R);
            
            if (datasetStmt->datasetCount == 2) {
                spatial_lib::DatasetT S;
                // build dataset S objects
                S.dataspaceInfo = g_config.datasetInfo.dataspaceInfo;
                S.dataType = datasetStmt->datatypeS;

                auto it = parser::fileTypeStrToIntMap.find(datasetStmt->filetypeS);
                if (it == parser::fileTypeStrToIntMap.end()) {
                    logger::log_error(DBERR_INVALID_FILETYPE, "Unknown filetype for dataset", datasetStmt->datasetNicknameS, ":", datasetStmt->filetypeS);
                    return DBERR_INVALID_FILETYPE;
                }
                S.fileType = it->second;

                S.path = datasetStmt->datasetPathS;
                S.nickname = datasetStmt->datasetNicknameS;
                S.offsetMapPath = datasetStmt->offsetMapPathS;
                S.datasetName = getDatasetNameFromPath(S.path);
                // add to config
                g_config.datasetInfo.addDataset(S);
            }
        }
        return DBERR_OK;
    }
}