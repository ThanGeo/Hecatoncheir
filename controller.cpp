
#include "SpatialLib.h"

#include "def.h"
#include "proc.h"
#include "config/setup.h"
#include "env/comm.h"
#include "env/pack.h"

static DB_STATUS terminateAllWorkers() {
    // broadcast finalization instruction
    DB_STATUS ret = comm::broadcast::broadcastInstructionMessage(MSG_INSTR_FIN);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Broadcasting termination instruction failed");
        return ret;
    }

    // perform the instruction locally as well
    ret = comm::controller::sendInstructionToAgent(MSG_INSTR_FIN);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Sending to local children failed");
        return ret;
    }

    return DBERR_OK;
}

static void hostTerminate() {
    // broadcast finalization instruction
    DB_STATUS ret = terminateAllWorkers();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Worker termination broadcast failed");
    }
    
}

void tempBatch() {
    // imitate load 2 polygons from disk
    // std::vector<double> coordsA = {-87.906508,32.896858,-87.906483,32.896926,-87.906396,32.897053,-87.906301,32.897177,-87.906245,32.897233,-87.906180,32.897281,-87.906101,32.897308,-87.906015,32.897303,-87.905940,32.897270,-87.905873,32.897224,-87.905743,32.897125,-87.905635,32.897007,-87.905563,32.896877,-87.905502,32.896744,-87.905431,32.896614,-87.905322,32.896490,-87.905645,32.896543,-87.905901,32.896577,-87.906161,32.896583,-87.906318,32.896631,-87.906392,32.896670,-87.906454,32.896721,-87.906497,32.896785,-87.906508,32.896858};
    // int vertexNumA = coordsA.size() / 2;
    // GeometryT geometryA(420, 69, vertexNumA, coordsA);

    // std::vector<double> coordsB = {-88.134417,33.014831,-88.133958,33.014830,-88.133970,33.014192,-88.134429,33.014193,-88.134417,33.014831};
    // int vertexNumB = coordsB.size() / 2;
    // GeometryT geometryB(1080, 72, vertexNumB, coordsB);

    // logger::log_task("Polygon A has ", vertexNumA, "vertices");
    // logger::log_task("Polygon B has ", vertexNumB, "vertices");

    // // prepare batch
    // GeometryBatchT batch;
    // batch.addGeometryToBatch(geometryA);
    // batch.addGeometryToBatch(geometryB);

    // // serialize
    // size_t bufferSize = batch.calculateBufferSize();
    // char *buffer = (char*) malloc (bufferSize * sizeof(char));
    
    // batch.serialize(buffer);

    // logger::log_success("Serialized!");

    // deserialize
    // GeometryBatchT newBatch;
    // newBatch.deserialize(buffer, bufferSize);
    // logger::log_success("Deserialized!");

    // logger::log_task("Total objects in deserialized batch:", newBatch.objectCount);
    // logger::log_task("Second object's id in deserialized batch:", newBatch.geometries[1].recID);

    // Send the serialized object
    // int mpi_ret = MPI_Send(buffer, bufferSize, MPI_PACKED, 7, MSG_BATCH_POLYGON, g_global_comm);
    // if (mpi_ret != MPI_SUCCESS) {
    //     return;
    // }


}

static DB_STATUS performActions() {
    DB_STATUS ret = DBERR_OK;
    // printf("I have %d datasets: %s and %s\n", g_config.datasetInfo.numberOfDatasets, g_config.datasetInfo.getDatasetByIdx(0)->nickname.c_str(), g_config.datasetInfo.getDatasetByIdx(1)->nickname.c_str());


    // perform the user-requested actions in order
    for(int i=0;i <g_config.actions.size(); i++) {
        // logger::log_task("Performing action", i, "/", g_config.actions.size(), "of type", g_config.actions.at(i).type);

        switch(g_config.actions.at(i).type) {
            case ACTION_PERFORM_PARTITIONING:
                // logger::log_task("size:", g_config.datasetInfo.datasets.size());
                for (int i=0; i<g_config.datasetInfo.numberOfDatasets; i++) {
                    // ret = partitioning::partitionDataset(g_config.datasetInfo.datasets.at(i));
                    ret = partitioning::partitionDataset(g_config.datasetInfo.getDatasetByIdx(i));
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                
                break;
            case ACTION_SERIALIZED_TEST:
                // logger::log_task("hey");
                tempBatch();
                break;
            default:
                logger::log_error(DBERR_INVALID_PARAMETER, "Unknown action. Type:",g_config.actions.at(i).type);
                return ret;
                break;
        }


    }


    return DBERR_OK;
}

int main(int argc, char* argv[]) {
    // initialize MPI environment
    DB_STATUS ret = configure::initMPI(argc, argv);
    if (ret != DBERR_OK) {
        goto EXIT_SAFELY;
    }

    logger::log_success("Initialized successfully");
       
    // get parent process intercomm (must be null)
    MPI_Comm_get_parent(&g_local_comm);
    if (g_local_comm != MPI_COMM_NULL) {
        logger::log_error(DBERR_PROC_INIT_FAILED, "Controllers must be parentless");
        goto EXIT_SAFELY;
    }
    g_parent_original_rank = PARENTLESS_RANK;

    // print cpu
    // logger::log_task("Runs on cpu", sched_getcpu());

    // all nodes create their agent process
    ret = proc::setupProcesses();
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Setup host process environment failed");
        hostTerminate();
        goto EXIT_SAFELY;
    }

    if (g_node_rank == g_host_rank) {
        // host controller has to handle setup/user input etc.
        ret = setup::setupSystem(argc, argv);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "System setup failed");
            hostTerminate();
            goto EXIT_SAFELY;
        }

        // perform the user-requested actions in order
        ret = performActions();
        if (ret != DBERR_OK) {
            hostTerminate();
            goto EXIT_SAFELY;
        }

        // temp();
        // tempBatch();



        // terminate
        hostTerminate();
    } else {
        // worker controllers go directly to listening
        // printf("Controller %d listening...\n", g_node_rank);
        ret = comm::controller::listen();
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Controller failed while listening for messages");
            goto EXIT_SAFELY;
        }
    }

EXIT_SAFELY:
    // Wait for the children processes to finish
    MPI_Barrier(g_local_comm);

    // Wait for all the controllers to finish
    MPI_Barrier(g_global_comm);

    // Finalize the MPI environment.
    MPI_Finalize();
    if (g_node_rank == g_host_rank) {
        logger::log_success("System finalized successfully");
    }
    return 0;
}