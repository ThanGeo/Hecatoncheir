#include "def.h"

ConfigT g_config;

int g_world_size;
int g_node_rank;
int g_host_rank = HOST_RANK;
int g_parent_original_rank;

MPI_Comm g_global_comm = MPI_COMM_WORLD;
MPI_Comm g_local_comm;

std::string actionIntToStr(ActionTypeE action) {
    switch (action) {
        case ACTION_NONE:
            return "NONE";
        case ACTION_LOAD_DATASETS:
            return "LOAD DATASETS";
        case ACTION_PERFORM_PARTITIONING:
            return "PERFORM PARTITIONING";
        case ACTION_CREATE_APRIL:
            return "CREATE APRIL";
        case ACTION_PERFORM_VERIFICATION:
            return "PERFORM VERIFICATION";
        case ACTION_QUERY:
            return "QUERY";
        case ACTION_LOAD_APRIL:
            return "LOAD APRIL";
        default:
            return "";
    }
}

void queryResultReductionFunc(spatial_lib::QueryOutputT &in, spatial_lib::QueryOutputT &out) {
    out.queryResults += in.queryResults;
    out.postMBRFilterCandidates += in.postMBRFilterCandidates;
    out.refinementCandidates += in.refinementCandidates;
    switch (g_config.queryInfo.type) {
        case spatial_lib::Q_DISJOINT:
        case spatial_lib::Q_INTERSECT:
        case spatial_lib::Q_INSIDE:
        case spatial_lib::Q_CONTAINS:
        case spatial_lib::Q_COVERS:
        case spatial_lib::Q_COVERED_BY:
        case spatial_lib::Q_MEET:
        case spatial_lib::Q_EQUAL:
            out.trueHits += in.trueHits;
            out.trueNegatives += in.trueNegatives;
            break;
        case spatial_lib::Q_FIND_RELATION:
            for (auto &it : in.topologyRelationsResultMap) {
                out.topologyRelationsResultMap[it.first] += it.second;
            }
            break;
    }
}
