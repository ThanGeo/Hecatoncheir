#include "env/pack.h"

namespace pack
{
    DB_STATUS packSystemInfo(SerializedMsg<char> &sysInfoMsg) {
        sysInfoMsg.count = 0;
        sysInfoMsg.count += sizeof(SystemSetupType);      // cluster or local
        sysInfoMsg.count += 2*sizeof(int);                  // dist + part partitions per dimension
        sysInfoMsg.count += sizeof(int);                  // partitioning type
        sysInfoMsg.count += sizeof(int) + g_config.dirPaths.dataPath.length() * sizeof(char);   // data directory path length + string
        sysInfoMsg.count += 3 * sizeof(int);              // MBRFilter, IFilter, Refinement
        
        // allocate space
        sysInfoMsg.data = (char*) malloc(sysInfoMsg.count * sizeof(char));
        if (sysInfoMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system info failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = sysInfoMsg.data;

        // put objects in buffer
        *reinterpret_cast<SystemSetupType*>(localBuffer) = g_config.options.setupType;
        localBuffer += sizeof(SystemSetupType);

        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->getDistributionPPD();
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->getPartitioningPPD();
        localBuffer += sizeof(int);

        *reinterpret_cast<PartitioningType*>(localBuffer) = g_config.partitioningMethod->getType();
        localBuffer += sizeof(PartitioningType);

        *reinterpret_cast<int*>(localBuffer) = g_config.dirPaths.dataPath.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, g_config.dirPaths.dataPath.data(), g_config.dirPaths.dataPath.length() * sizeof(char));
        localBuffer += g_config.dirPaths.dataPath.length() * sizeof(char);

        *reinterpret_cast<int*>(localBuffer) = g_config.queryInfo.MBRFilter;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.queryInfo.IntermediateFilter;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.queryInfo.Refinement;
        localBuffer += sizeof(int);

        return DBERR_OK;
    }


    DB_STATUS packAPRILInfo(AprilConfig &aprilConfig, SerializedMsg<int> &aprilInfoMsg) {
        aprilInfoMsg.count = 0;
        aprilInfoMsg.count += 3;     // N, compression, partitions

        // allocate space
        aprilInfoMsg.data = (int*) malloc(aprilInfoMsg.count * sizeof(int));
        if (aprilInfoMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for april info failed");
            return DBERR_MALLOC_FAILED;
        }

        // put objects in buffer
        aprilInfoMsg.data[0] = aprilConfig.getN();
        aprilInfoMsg.data[1] = aprilConfig.compression;
        aprilInfoMsg.data[2] = aprilConfig.partitions;

        return DBERR_OK;
    }

    DB_STATUS packQueryInfo(QueryInfo &queryInfo, SerializedMsg<int> &queryInfoMsg) {
        queryInfoMsg.count = 0;
        queryInfoMsg.count += 4;    // query type, MBR, intermediatefilter, refinement

        // allocate space
        queryInfoMsg.data = (int*) malloc(queryInfoMsg.count * sizeof(int));
        if (queryInfoMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for query info failed");
            return DBERR_MALLOC_FAILED;
        }

        // put objects in buffer
        queryInfoMsg.data[0] = queryInfo.type;
        queryInfoMsg.data[1] = queryInfo.MBRFilter;
        queryInfoMsg.data[2] = queryInfo.IntermediateFilter;
        queryInfoMsg.data[3] = queryInfo.Refinement;

        return DBERR_OK;
    }

    DB_STATUS packDatasetsNicknames(SerializedMsg<char> &msg) {
        std::string nicknameR = "";
        std::string nicknameS = "";
        msg.count = 0;

        // get dataset R
        Dataset* R = g_config.datasetInfo.getDatasetR();
        if (R == nullptr) {
            return DBERR_OK;
        }
        // count for buffer size
        msg.count += sizeof(int) + R->nickname.length() * sizeof(int);
        // keep nickname
        nicknameR = R->nickname;
        // get dataset S
        Dataset* S = g_config.datasetInfo.getDatasetS();
        if (S != nullptr) {
            // count for buffer size
            msg.count += sizeof(int) + S->nickname.length() * sizeof(int);
            // keep nickname
            nicknameS = S->nickname;
        }
        // allocate space
        msg.data = (char*) malloc(msg.count * sizeof(char));
        if (msg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system info failed");
            return DBERR_MALLOC_FAILED;
        }
        
        char* localBuffer = msg.data;
        // store in buffer
        *reinterpret_cast<int*>(localBuffer) = nicknameR.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, nicknameR.data(), nicknameR.length() * sizeof(char));
        localBuffer += nicknameR.length() * sizeof(char);

        *reinterpret_cast<int*>(localBuffer) = nicknameS.length();
        localBuffer += sizeof(int);
        if (nicknameS.length() > 0) {
            std::memcpy(localBuffer, nicknameS.data(), nicknameS.length() * sizeof(char));
            localBuffer += nicknameS.length() * sizeof(char);
        }

        return DBERR_OK;
    }

    DB_STATUS packQueryResults(SerializedMsg<int> &msg, QueryOutput &queryOutput) {
        DB_STATUS ret = DBERR_OK;
        // serialize based on query type
        switch (g_config.queryInfo.type) {
            case Q_RANGE:
            case Q_DISJOINT:
            case Q_INTERSECT:
            case Q_INSIDE:
            case Q_CONTAINS:
            case Q_COVERS:
            case Q_COVERED_BY:
            case Q_MEET:
            case Q_EQUAL:
                // mbr results, accept, reject, inconclusive, result count
                msg.count += 5;
                // allocate space
                msg.data = (int*) malloc(msg.count * sizeof(int));
                if (msg.data == NULL) {
                    // malloc failed
                    logger::log_error(DBERR_MALLOC_FAILED, "Malloc for query results msg failed");
                    return DBERR_MALLOC_FAILED;
                }
                // put objects in buffer
                msg.data[0] = queryOutput.queryResults;
                msg.data[1] = queryOutput.postMBRFilterCandidates;
                msg.data[2] = queryOutput.trueHits;
                msg.data[3] = queryOutput.trueNegatives;
                msg.data[4] = queryOutput.refinementCandidates;
                break;
            case Q_FIND_RELATION:
                // total results, mbr results, inconclusive, disjoint, intersect, inside, contains, covers, covered by, meet, equal
                msg.count += 11;
                // allocate space
                msg.data = (int*) malloc(msg.count * sizeof(int));
                if (msg.data == NULL) {
                    // malloc failed
                    logger::log_error(DBERR_MALLOC_FAILED, "Malloc for query results msg failed");
                    return DBERR_MALLOC_FAILED;
                }
                // put objects in buffer
                msg.data[0] = queryOutput.queryResults;
                msg.data[1] = queryOutput.postMBRFilterCandidates;
                msg.data[2] = queryOutput.refinementCandidates;
                msg.data[3] = queryOutput.getResultForTopologyRelation(TR_DISJOINT);
                msg.data[4] = queryOutput.getResultForTopologyRelation(TR_INTERSECT);
                msg.data[5] = queryOutput.getResultForTopologyRelation(TR_INSIDE);
                msg.data[6] = queryOutput.getResultForTopologyRelation(TR_CONTAINS);
                msg.data[7] = queryOutput.getResultForTopologyRelation(TR_COVERS);
                msg.data[8] = queryOutput.getResultForTopologyRelation(TR_COVERED_BY);
                msg.data[9] = queryOutput.getResultForTopologyRelation(TR_MEET);
                msg.data[10] = queryOutput.getResultForTopologyRelation(TR_EQUAL);
                break;
            default:
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", g_config.queryInfo.type);
                return DBERR_QUERY_INVALID_TYPE;
        }
        return ret;
    }

}

namespace unpack
{
    DB_STATUS unpackSystemInfo(SerializedMsg<char> &sysInfoMsg) {
        PartitioningType partitioningType;
        int partPartitionsPerDim, distPartitionsPerDim;
        char *localBuffer = sysInfoMsg.data;
        // get system setup type
        g_config.options.setupType = *reinterpret_cast<const SystemSetupType*>(localBuffer);
        localBuffer += sizeof(SystemSetupType);
        // get dist+part partitions per dimension
        distPartitionsPerDim = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        partPartitionsPerDim = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // get dist+part partitioning type
        partitioningType = *reinterpret_cast<const PartitioningType*>(localBuffer);
        localBuffer += sizeof(PartitioningType);
        // set partitioning method
        if (partitioningType == PARTITIONING_ROUND_ROBIN) {
            // batch size to zero, since its irrelevant
            g_config.partitioningMethod = new RoundRobinPartitioning(partitioningType, 0, partPartitionsPerDim);
        } else if (partitioningType == PARTITIONING_TWO_GRID) {
            // batch size to zero and distribution ppd to zero, both irrelevant to worker
            g_config.partitioningMethod = new TwoGridPartitioning(partitioningType, 0, distPartitionsPerDim, partPartitionsPerDim);
        } else {
            logger::log_error(DBERR_INVALID_PARAMETER, "Unknown partitioning type while unpacking system info:", partitioningType);
            return DBERR_INVALID_PARAMETER;
        }
        // get datapath length + string
        // and set the paths
        int length;
        length = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        std::string datapath(localBuffer, localBuffer + length);
        localBuffer += length * sizeof(char);
        g_config.dirPaths.setNodeDataDirectories(datapath);
        // pipeline
        g_config.queryInfo.MBRFilter = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        g_config.queryInfo.IntermediateFilter = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        g_config.queryInfo.Refinement = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        return DBERR_OK;
    }

    DB_STATUS unpackAPRILInfo(SerializedMsg<int> &aprilInfoMsg) {
        // N
        int N = aprilInfoMsg.data[0];
        g_config.approximationInfo.aprilConfig.setN(N);
        // compression
        g_config.approximationInfo.aprilConfig.compression = aprilInfoMsg.data[1];
        // partitions
        g_config.approximationInfo.aprilConfig.partitions = aprilInfoMsg.data[2];
        // set type
        g_config.approximationInfo.type = AT_APRIL;
        for (auto& it: g_config.datasetInfo.datasets) {
            it.second.approxType = AT_APRIL;
            it.second.aprilConfig.setN(N);
            it.second.aprilConfig.compression = g_config.approximationInfo.aprilConfig.compression;
            it.second.aprilConfig.partitions = g_config.approximationInfo.aprilConfig.partitions;
        }

        return DBERR_OK;
    }

    DB_STATUS unpackQueryInfo(SerializedMsg<int> &queryInfoMsg) {
        g_config.queryInfo.type = (QueryType) queryInfoMsg.data[0];
        g_config.queryInfo.MBRFilter = queryInfoMsg.data[1];
        g_config.queryInfo.IntermediateFilter = queryInfoMsg.data[2];
        g_config.queryInfo.Refinement = queryInfoMsg.data[3];
        
        return DBERR_OK;
    }

    DB_STATUS unpackQueryResults(SerializedMsg<int> &queryResultsMsg, QueryType queryType, QueryOutput &queryOutput) {
        // total results and mbr results is common
        queryOutput.queryResults = queryResultsMsg.data[0];   
        queryOutput.postMBRFilterCandidates = queryResultsMsg.data[1];    
        // unpack based on query type
        switch (queryType) {
            case Q_RANGE:
            case Q_DISJOINT:
            case Q_INTERSECT:
            case Q_INSIDE:
            case Q_CONTAINS:
            case Q_COVERS:
            case Q_COVERED_BY:
            case Q_MEET:
            case Q_EQUAL:
                // accept, reject, inconclusive, result count
                queryOutput.trueHits = queryResultsMsg.data[2];
                queryOutput.trueNegatives = queryResultsMsg.data[3];
                queryOutput.refinementCandidates = queryResultsMsg.data[4];
                break;
            case Q_FIND_RELATION:
                // inconclusive, disjoint, intersect, inside, contains, covers, covered by, meet, equal
                queryOutput.refinementCandidates = queryResultsMsg.data[2];
                queryOutput.setTopologyRelationResult(TR_DISJOINT, queryResultsMsg.data[3]);
                queryOutput.setTopologyRelationResult(TR_INTERSECT, queryResultsMsg.data[4]);
                queryOutput.setTopologyRelationResult(TR_INSIDE, queryResultsMsg.data[5]);
                queryOutput.setTopologyRelationResult(TR_CONTAINS, queryResultsMsg.data[6]);
                queryOutput.setTopologyRelationResult(TR_COVERS, queryResultsMsg.data[7]);
                queryOutput.setTopologyRelationResult(TR_COVERED_BY, queryResultsMsg.data[8]);
                queryOutput.setTopologyRelationResult(TR_MEET, queryResultsMsg.data[9]);
                queryOutput.setTopologyRelationResult(TR_EQUAL, queryResultsMsg.data[10]);
                break;
            default:
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", g_config.queryInfo.type);
                return DBERR_QUERY_INVALID_TYPE;
        }
        return DBERR_OK;
    }

    DB_STATUS unpackDatasetsNicknames(SerializedMsg<char> &msg, std::vector<std::string> &nicknames) {
        char *localBuffer = msg.data;
        int length;
        // R
        length = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        std::string nickname(localBuffer, localBuffer + length);
        localBuffer += length * sizeof(char);
        // append
        nicknames.emplace_back(nickname);
        // S
        length = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        if (length > 0) {
            std::string nickname(localBuffer, localBuffer + length);
            localBuffer += length * sizeof(char);
            // append
            nicknames.emplace_back(nickname);
        }

        return DBERR_OK;
    }
}