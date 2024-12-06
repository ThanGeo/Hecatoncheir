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
        DB_STATUS dir_ret = verifyDirectory(g_config.dirPaths.dataPath);
        if (dir_ret != DBERR_OK) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.dataPath.c_str(), 0777);
            if (ret) {
                logger::log_error(DBERR_CREATE_DIR, "Error creating node data directory. Path:", g_config.dirPaths.dataPath);
                return DBERR_CREATE_DIR;
            }
        }

        dir_ret = verifyDirectory(g_config.dirPaths.partitionsPath);
        if (dir_ret != DBERR_OK) {
            // if dataset config directory doesn't exist, create
            ret = mkdir(g_config.dirPaths.partitionsPath.c_str(), 0777);
            if (ret) {
                logger::log_error(DBERR_CREATE_DIR, "Error creating partitioned data directory");
                return DBERR_CREATE_DIR;
            }
        }
        dir_ret = verifyDirectory(g_config.dirPaths.approximationPath);
        if (dir_ret != DBERR_OK) {
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
        // broadcast system metadata
        ret = comm::controller::broadcastSysMetadata();
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


    DB_STATUS setDatasetMetadata(DatasetStatement* datasetStmt) {
        if (datasetStmt->datasetCount == 0) {
            // no datasets
            g_config.datasetMetadata.clear();
            return DBERR_OK;
        } else {
            // at least one dataset
            Dataset R;
            // set dataspace metadata
            if (datasetStmt->boundsSet) {
                g_config.datasetMetadata.dataspaceMetadata.xMinGlobal = datasetStmt->xMinGlobal;
                g_config.datasetMetadata.dataspaceMetadata.yMinGlobal = datasetStmt->yMinGlobal;
                g_config.datasetMetadata.dataspaceMetadata.xMaxGlobal = datasetStmt->xMaxGlobal;
                g_config.datasetMetadata.dataspaceMetadata.yMaxGlobal = datasetStmt->yMaxGlobal;
                g_config.datasetMetadata.dataspaceMetadata.xExtent = g_config.datasetMetadata.dataspaceMetadata.xMaxGlobal - g_config.datasetMetadata.dataspaceMetadata.xMinGlobal;
                g_config.datasetMetadata.dataspaceMetadata.yExtent = g_config.datasetMetadata.dataspaceMetadata.yMaxGlobal - g_config.datasetMetadata.dataspaceMetadata.yMinGlobal;
                g_config.datasetMetadata.dataspaceMetadata.boundsSet = true;
            }
            
            // fill dataset R fields
            R.dataspaceMetadata = g_config.datasetMetadata.dataspaceMetadata;
            R.dataType = datasetStmt->datatypeR;
            R.path = datasetStmt->datasetPathR;
            R.nickname = datasetStmt->datasetNicknameR;
            R.datasetName = getFileNameFromPath(R.path);
            R.fileType = (FileTypeE) mapping::fileTypeTextToInt(datasetStmt->filetypeR);
            
            // add to config
            DB_STATUS ret = g_config.datasetMetadata.addDataset(DATASET_R, R);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to add dataset R in configuration. Dataset nickname:", datasetStmt->datasetNicknameR, "Dataset idx: DATASET_R");
                return ret;
            }
            
            if (datasetStmt->datasetCount == 2) {
                Dataset S;
                // build dataset S objects (inherit the same dataspace metadata)
                S.dataspaceMetadata = g_config.datasetMetadata.dataspaceMetadata;
                S.dataType = datasetStmt->datatypeS;
                S.path = datasetStmt->datasetPathS;
                S.nickname = datasetStmt->datasetNicknameS;
                S.datasetName = getFileNameFromPath(S.path);
                S.fileType = (FileTypeE) mapping::fileTypeTextToInt(datasetStmt->filetypeS);
                // add to config
                DB_STATUS ret = g_config.datasetMetadata.addDataset(DATASET_S, S);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to add dataset S in configuration. Dataset nickname:", datasetStmt->datasetNicknameS, "Dataset idx: DATASET_S");
                    return ret;
                }
            }
        }
        return DBERR_OK;
    }
}