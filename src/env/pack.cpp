#include "env/pack.h"

namespace pack
{
    DB_STATUS packSystemMetadata(SerializedMsg<char> &sysMetadataMsg) {
        sysMetadataMsg.count = 0;
        sysMetadataMsg.count += sizeof(SystemSetupTypeE);      // cluster or local
        sysMetadataMsg.count += 2*sizeof(int);                  // dist + part partitions per dimension
        sysMetadataMsg.count += sizeof(int);                  // partitioning type
        sysMetadataMsg.count += sizeof(int) + g_config.dirPaths.dataPath.length() * sizeof(char);   // data directory path length + string
        sysMetadataMsg.count += 3 * sizeof(int);              // MBRFilter, IFilter, Refinement
        
        // allocate space
        sysMetadataMsg.data = (char*) malloc(sysMetadataMsg.count * sizeof(char));
        if (sysMetadataMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = sysMetadataMsg.data;

        // put objects in buffer
        *reinterpret_cast<SystemSetupTypeE*>(localBuffer) = g_config.options.setupType;
        localBuffer += sizeof(SystemSetupTypeE);

        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->getDistributionPPD();
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->getPartitioningPPD();
        localBuffer += sizeof(int);

        *reinterpret_cast<PartitioningTypeE*>(localBuffer) = g_config.partitioningMethod->getType();
        localBuffer += sizeof(PartitioningTypeE);

        *reinterpret_cast<int*>(localBuffer) = g_config.dirPaths.dataPath.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, g_config.dirPaths.dataPath.data(), g_config.dirPaths.dataPath.length() * sizeof(char));
        localBuffer += g_config.dirPaths.dataPath.length() * sizeof(char);

        *reinterpret_cast<int*>(localBuffer) = g_config.queryMetadata.MBRFilter;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.queryMetadata.IntermediateFilter;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.queryMetadata.Refinement;
        localBuffer += sizeof(int);

        return DBERR_OK;
    }


    DB_STATUS packAPRILMetadata(AprilConfig &aprilConfig, SerializedMsg<int> &aprilMetadataMsg) {
        aprilMetadataMsg.count = 0;
        aprilMetadataMsg.count += 3;     // N, compression, partitions

        // allocate space
        aprilMetadataMsg.data = (int*) malloc(aprilMetadataMsg.count * sizeof(int));
        if (aprilMetadataMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for april metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        // put objects in buffer
        aprilMetadataMsg.data[0] = aprilConfig.getN();
        aprilMetadataMsg.data[1] = aprilConfig.compression;
        aprilMetadataMsg.data[2] = aprilConfig.partitions;

        return DBERR_OK;
    }

    DB_STATUS packQueryMetadata(QueryMetadata &queryMetadata, SerializedMsg<int> &queryMetadataMsg) {
        queryMetadataMsg.count = 0;
        queryMetadataMsg.count += 4;    // query type, MBR, intermediatefilter, refinement

        // allocate space
        queryMetadataMsg.data = (int*) malloc(queryMetadataMsg.count * sizeof(int));
        if (queryMetadataMsg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for query metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        // put objects in buffer
        queryMetadataMsg.data[0] = queryMetadata.type;
        queryMetadataMsg.data[1] = queryMetadata.MBRFilter;
        queryMetadataMsg.data[2] = queryMetadata.IntermediateFilter;
        queryMetadataMsg.data[3] = queryMetadata.Refinement;

        return DBERR_OK;
    }

    DB_STATUS packDatasetsNicknames(SerializedMsg<char> &msg) {
        std::string nicknameR = "";
        std::string nicknameS = "";
        msg.count = 0;

        // get dataset R
        Dataset* R = g_config.datasetMetadata.getDatasetR();
        if (R == nullptr) {
            return DBERR_OK;
        }
        // count for buffer size
        msg.count += sizeof(int) + R->nickname.length() * sizeof(int);
        // keep nickname
        nicknameR = R->nickname;
        // get dataset S
        Dataset* S = g_config.datasetMetadata.getDatasetS();
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
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
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

    DB_STATUS packDatasetLoadMsg(Dataset *dataset, DatasetIndexE datasetIndex, SerializedMsg<char> &msg) {
        msg.count = 0;

        // count for buffer size
        msg.count += sizeof(int);           // dataset index
        msg.count += sizeof(int) + dataset->nickname.length() * sizeof(int);     // dataset nikcname length (int) + nickname string

        // allocate space
        msg.data = (char*) malloc(msg.count * sizeof(char));
        if (msg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }
        
        char* localBuffer = msg.data;
        // store in buffer
        // dataset index
        *reinterpret_cast<int*>(localBuffer) = datasetIndex;
        localBuffer += sizeof(int);
        // dataset nickname length
        *reinterpret_cast<int*>(localBuffer) = dataset->nickname.length();
        localBuffer += sizeof(int);
        // dataset nickname string
        std::memcpy(localBuffer, dataset->nickname.data(), dataset->nickname.length() * sizeof(char));
        localBuffer += dataset->nickname.length() * sizeof(char);

        return DBERR_OK;
    }

    DB_STATUS packQueryResults(SerializedMsg<int> &msg, QueryOutput &queryOutput) {
        DB_STATUS ret = DBERR_OK;
        // serialize based on query type
        switch (g_config.queryMetadata.type) {
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
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", g_config.queryMetadata.type);
                return DBERR_QUERY_INVALID_TYPE;
        }
        return ret;
    }

}

namespace unpack
{
    DB_STATUS unpackSystemMetadata(SerializedMsg<char> &sysMetadataMsg) {
        PartitioningTypeE partitioningType;
        int partPartitionsPerDim, distPartitionsPerDim;
        char *localBuffer = sysMetadataMsg.data;
        // get system setup type
        g_config.options.setupType = *reinterpret_cast<const SystemSetupTypeE*>(localBuffer);
        localBuffer += sizeof(SystemSetupTypeE);
        // get dist+part partitions per dimension
        distPartitionsPerDim = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        partPartitionsPerDim = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // get dist+part partitioning type
        partitioningType = *reinterpret_cast<const PartitioningTypeE*>(localBuffer);
        localBuffer += sizeof(PartitioningTypeE);
        // set partitioning method
        if (partitioningType == PARTITIONING_ROUND_ROBIN) {
            // batch size to zero, since its irrelevant
            g_config.partitioningMethod = new RoundRobinPartitioning(partitioningType, 0, partPartitionsPerDim);
        } else if (partitioningType == PARTITIONING_TWO_GRID) {
            // batch size to zero and distribution ppd to zero, both irrelevant to worker
            g_config.partitioningMethod = new TwoGridPartitioning(partitioningType, 0, distPartitionsPerDim, partPartitionsPerDim);
        } else {
            logger::log_error(DBERR_INVALID_PARAMETER, "Unknown partitioning type while unpacking system metadata:", partitioningType);
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
        g_config.queryMetadata.MBRFilter = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        g_config.queryMetadata.IntermediateFilter = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        g_config.queryMetadata.Refinement = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        return DBERR_OK;
    }

    DB_STATUS unpackAPRILMetadata(SerializedMsg<int> &aprilMetadataMsg) {
        // N
        int N = aprilMetadataMsg.data[0];
        g_config.approximationMetadata.aprilConfig.setN(N);
        // compression
        g_config.approximationMetadata.aprilConfig.compression = aprilMetadataMsg.data[1];
        // partitions
        g_config.approximationMetadata.aprilConfig.partitions = aprilMetadataMsg.data[2];
        // set type
        g_config.approximationMetadata.type = AT_APRIL;
        for (auto& it: g_config.datasetMetadata.datasets) {
            it.second.approxType = AT_APRIL;
            it.second.aprilConfig.setN(N);
            it.second.aprilConfig.compression = g_config.approximationMetadata.aprilConfig.compression;
            it.second.aprilConfig.partitions = g_config.approximationMetadata.aprilConfig.partitions;
        }

        return DBERR_OK;
    }

    DB_STATUS unpackQueryMetadata(SerializedMsg<int> &queryMetadataMsg) {
        g_config.queryMetadata.type = (QueryTypeE) queryMetadataMsg.data[0];
        g_config.queryMetadata.MBRFilter = queryMetadataMsg.data[1];
        g_config.queryMetadata.IntermediateFilter = queryMetadataMsg.data[2];
        g_config.queryMetadata.Refinement = queryMetadataMsg.data[3];
        
        return DBERR_OK;
    }

    DB_STATUS unpackQueryResults(SerializedMsg<int> &queryResultsMsg, QueryTypeE queryType, QueryOutput &queryOutput) {
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
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", g_config.queryMetadata.type);
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

    DB_STATUS unpackDatasetLoadMsg(SerializedMsg<char> &msg, Dataset &dataset, DatasetIndexE &datasetIndex) {
        char *localBuffer = msg.data;
        int length;
        // dataset index
        datasetIndex = (DatasetIndexE) *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // dataset nickname length
        length = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // dataset nickname 
        std::string nickname(localBuffer, localBuffer + length);
        localBuffer += length * sizeof(char);
        dataset.nickname = nickname;
        return DBERR_OK;
    }
}