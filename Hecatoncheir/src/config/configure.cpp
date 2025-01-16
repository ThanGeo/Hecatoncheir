#include "config/configure.h"

namespace configurer
{
    DB_STATUS initMPIController(int argc, char* argv[]) {
        int rank, wsize;
        int provided;
        // type
        g_proc_type = CONTROLLER;
        // init MPI
        int mpi_ret = MPI_Init_thread(nullptr, nullptr, MPI_THREAD_MULTIPLE, &provided);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init MPI failed.");
            return DBERR_MPI_INIT_FAILED;
        }

        // get inter comm with parent
        g_parent_original_rank = PARENT_RANK;
        mpi_ret = MPI_Comm_get_parent(&g_global_inter_comm);
        if (g_global_inter_comm == MPI_COMM_NULL || mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "No parent process (driver) found.");
            return DBERR_MPI_INIT_FAILED;
        }

        // merge inter-comm to intra-comm
        MPI_Intercomm_merge(g_global_inter_comm, 1, &g_global_intra_comm);

        // get rank in global group
        mpi_ret = MPI_Comm_rank(g_global_intra_comm, &rank);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm rank failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_global_rank = rank;

        // split intra-comm to form the group
        mpi_ret = MPI_Comm_split(g_global_intra_comm, 1, g_global_rank-1, &g_controller_comm);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Comm split failed. MPI ret code:", mpi_ret);
            return DBERR_MPI_INIT_FAILED;
        }

        if (g_controller_comm == MPI_COMM_NULL) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "WTF, controller comm is NULL?");
        }

        // get rank in group of controllers
        mpi_ret = MPI_Comm_rank(g_controller_comm, &rank);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm rank failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_node_rank = rank;

        // world size
        mpi_ret = MPI_Comm_size(g_controller_comm, &wsize);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm size failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_world_size = wsize;

        // release inter-comm
        mpi_ret = MPI_Comm_free(&g_global_inter_comm);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Freeing obsolete inter-comm failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        return DBERR_OK;
    }

    DB_STATUS initMPIAgent(int argc, char* argv[]) {
        int rank, wsize;
        int provided;
        // init MPI
        int mpi_ret = MPI_Init_thread(nullptr, nullptr, MPI_THREAD_MULTIPLE, &provided);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init MPI failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        // type
        g_proc_type = AGENT;

        // get parent process intercomm
        MPI_Comm_get_parent(&g_agent_comm);
        if (g_agent_comm == MPI_COMM_NULL) {
            logger::log_error(DBERR_PROC_INIT_FAILED, "The agent can't be parentless");
            return DBERR_PROC_INIT_FAILED;
        }

        // process rank
        mpi_ret = MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm rank failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_node_rank = rank;
        
        // world size
        mpi_ret = MPI_Comm_size(g_agent_comm, &wsize);
        if (mpi_ret != MPI_SUCCESS) {
            logger::log_error(DBERR_MPI_INIT_FAILED, "Init comm size failed.");
            return DBERR_MPI_INIT_FAILED;
        }
        g_world_size = wsize;

        // receive parent's original rank from the parent
        // mpi_ret = MPI_Bcast(&g_parent_original_rank, 1, MPI_INT, PARENT_RANK, g_agent_comm);
        MPI_Status status;
        mpi_ret = MPI_Recv(&g_parent_original_rank, 1, MPI_INT, PARENT_RANK, 0, g_agent_comm, &status);
        if (mpi_ret) {
            logger::log_error(DBERR_COMM_BCAST, "Receiving parent rank failed", DBERR_COMM_BCAST);
            return DBERR_COMM_BCAST;
        }
        // print cpu
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
        // broadcast system metadata
        ret = comm::controller::broadcastSysMetadata();
        if (ret != DBERR_OK) {
            return ret;
        }

        // wait for response by workers+agent that all is ok
        ret = comm::host::gatherResponses();
        if (ret != DBERR_OK) {
            return ret;
        }

        return DBERR_OK;
    }


    DB_STATUS setDatasetMetadata(DatasetStatement* datasetStmt) {
        if (datasetStmt->datasetCount == 0) {
            // no datasets
            g_config.datasetOptions.clear();
            return DBERR_OK;
        } else {
            // at least one dataset
            Dataset R;
            // set dataspace metadata
            if (datasetStmt->boundsSet) {
                g_config.datasetOptions.dataspaceMetadata.xMinGlobal = datasetStmt->xMinGlobal;
                g_config.datasetOptions.dataspaceMetadata.yMinGlobal = datasetStmt->yMinGlobal;
                g_config.datasetOptions.dataspaceMetadata.xMaxGlobal = datasetStmt->xMaxGlobal;
                g_config.datasetOptions.dataspaceMetadata.yMaxGlobal = datasetStmt->yMaxGlobal;
                g_config.datasetOptions.dataspaceMetadata.xExtent = g_config.datasetOptions.dataspaceMetadata.xMaxGlobal - g_config.datasetOptions.dataspaceMetadata.xMinGlobal;
                g_config.datasetOptions.dataspaceMetadata.yExtent = g_config.datasetOptions.dataspaceMetadata.yMaxGlobal - g_config.datasetOptions.dataspaceMetadata.yMinGlobal;
                g_config.datasetOptions.dataspaceMetadata.boundsSet = true;
            }
            
            // fill dataset R fields
            R.metadata.dataspaceMetadata = g_config.datasetOptions.dataspaceMetadata;
            R.metadata.dataType = datasetStmt->datatypeR;
            R.metadata.path = datasetStmt->datasetPathR;
            R.metadata.datasetName = getFileNameFromPath(R.metadata.path);
            R.metadata.fileType = (FileType) mapping::fileTypeTextToInt(datasetStmt->filetypeR);
            
            // add to config
            DB_STATUS ret = g_config.datasetOptions.addDataset(DATASET_R, R);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to add dataset R in configuration. Dataset nickname:", datasetStmt->datasetNicknameR, "Dataset idx: DATASET_R");
                return ret;
            }
            
            if (datasetStmt->datasetCount == 2) {
                Dataset S;
                // build dataset S objects (inherit the same dataspace metadata)
                S.metadata.dataspaceMetadata = g_config.datasetOptions.dataspaceMetadata;
                S.metadata.dataType = datasetStmt->datatypeS;
                S.metadata.path = datasetStmt->datasetPathS;
                S.metadata.datasetName = getFileNameFromPath(S.metadata.path);
                S.metadata.fileType = (FileType) mapping::fileTypeTextToInt(datasetStmt->filetypeS);
                // add to config
                DB_STATUS ret = g_config.datasetOptions.addDataset(DATASET_S, S);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to add dataset S in configuration. Dataset nickname:", datasetStmt->datasetNicknameS, "Dataset idx: DATASET_S");
                    return ret;
                }
            }
        }
        return DBERR_OK;
    }
}