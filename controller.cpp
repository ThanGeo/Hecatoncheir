
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

void temp() {
    // imitate load polygon from disk
    int id = 420;
    int partitionID = 69;
    std::vector<double> coords = {-87.906508,32.896858,-87.906483,32.896926,-87.906396,32.897053,-87.906301,32.897177,-87.906245,32.897233,-87.906180,32.897281,-87.906101,32.897308,-87.906015,32.897303,-87.905940,32.897270,-87.905873,32.897224,-87.905743,32.897125,-87.905635,32.897007,-87.905563,32.896877,-87.905502,32.896744,-87.905431,32.896614,-87.905322,32.896490,-87.905645,32.896543,-87.905901,32.896577,-87.906161,32.896583,-87.906318,32.896631,-87.906392,32.896670,-87.906454,32.896721,-87.906497,32.896785,-87.906508,32.896858};
    int vertexNum = coords.size() / 2;
    spatial_lib::PolygonT polygon;
    polygon.recID = id;
    for(int i=0; i<coords.size(); i+=2) {
        spatial_lib::PointT p;
        p.x = coords.at(i);
        p.y = coords.at(i+1);
        polygon.vertices.emplace_back(p);
    }

    // send to controller 7
    DB_STATUS ret = comm::controller::sendPolygonToNode(polygon, partitionID, 7, MSG_SINGLE_POLYGON);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed sending polygon to node");
    }

}
void tempBatch() {
    // imitate load polygon from disk
    int idA = 420;
    int partitionIDA = 69;
    std::vector<double> coordsA = {-87.906508,32.896858,-87.906483,32.896926,-87.906396,32.897053,-87.906301,32.897177,-87.906245,32.897233,-87.906180,32.897281,-87.906101,32.897308,-87.906015,32.897303,-87.905940,32.897270,-87.905873,32.897224,-87.905743,32.897125,-87.905635,32.897007,-87.905563,32.896877,-87.905502,32.896744,-87.905431,32.896614,-87.905322,32.896490,-87.905645,32.896543,-87.905901,32.896577,-87.906161,32.896583,-87.906318,32.896631,-87.906392,32.896670,-87.906454,32.896721,-87.906497,32.896785,-87.906508,32.896858};
    int vertexNumA = coordsA.size() / 2;
    spatial_lib::PolygonT polygonA;
    polygonA.recID = idA;
    for(int i=0; i<coordsA.size(); i+=2) {
        spatial_lib::PointT p;
        p.x = coordsA.at(i);
        p.y = coordsA.at(i+1);
        polygonA.vertices.emplace_back(p);
    }
    int idB = 1080;
    int partitionIDB = 72;
    std::vector<double> coordsB = {-88.134417,33.014831,-88.133958,33.014830,-88.133970,33.014192,-88.134429,33.014193,-88.134417,33.014831};
    int vertexNumB = coordsB.size() / 2;
    spatial_lib::PolygonT polygonB;
    polygonB.recID = idB;
    for(int i=0; i<coordsB.size(); i+=2) {
        spatial_lib::PointT p;
        p.x = coordsB.at(i);
        p.y = coordsB.at(i+1);
        polygonB.vertices.emplace_back(p);
    }


    // send to controller 7
    // DB_STATUS ret = comm::controller::sendPolygonToNode(polygon, partitionID, 7, MSG_SINGLE_POLYGON);
    // if (ret != DBERR_OK) {
    //     logger::log_error(ret, "Failed sending polygon to node");
    // }

}

static DB_STATUS performActions() {
    // perform the user-requested actions in order
    for(int i=0;i <g_config.actions.size(); i++) {
        // logger::log_task("Performing action", i, "of type", g_config.actions.at(i).type);

        switch(g_config.actions.at(i).type) {
            case ACTION_PERFORM_PARTITIONING:
                
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