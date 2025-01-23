#include "env/pack.h"

namespace pack
{
    DB_STATUS packIntegers(SerializedMsg<int>& msg, std::vector<int> integers) {
        // Calculate the total size of all integers
        int count = integers.size();
        // Allocate memory for the message
        msg.count = count;
        msg.data = (int*)malloc(msg.count * sizeof(int));
        if (msg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack dataset integers failed");
            return DBERR_MALLOC_FAILED;
        }

        int* localBuffer = msg.data;

        // Add the integers to the buffer
        for (auto &integer : integers) {
            *reinterpret_cast<int*>(localBuffer) = integer;
            localBuffer += 1;    
        }

        return DBERR_OK;
    }

    DB_STATUS packDatasetIndexes(std::vector<int> indexes, SerializedMsg<int> &msg) {
        msg.count = 1 + indexes.size();
        msg.data = (int*) malloc(msg.count * sizeof(int));
        if (msg.data == NULL) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack dataset index failed");
            return DBERR_MALLOC_FAILED;
        }
        int* localBuffer = msg.data;

        // put objects in buffer
        *reinterpret_cast<int*>(localBuffer) = (int) indexes.size();
        localBuffer += 1;
        for (int i=0; i<indexes.size(); i++) {
            *reinterpret_cast<int*>(localBuffer) = indexes[i];
            localBuffer += 1;
        }
        return DBERR_OK;
    }

    DB_STATUS packSystemMetadata(SerializedMsg<char> &sysMetadataMsg) {
        sysMetadataMsg.count = 0;
        sysMetadataMsg.count += sizeof(SystemSetupType);      // cluster or local
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

    DB_STATUS packDatasetLoadMsg(Dataset *dataset, DatasetIndex datasetIndex, SerializedMsg<char> &msg) {
        msg.count = 0;

        // count for buffer size
        msg.count += sizeof(int);           // dataset index

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

        return DBERR_OK;
    }

    static DB_STATUS packRangeQuery(hec::RangeQuery *query, SerializedMsg<char> &msg) {
        msg.count = 0;
        msg.count += sizeof(int);               // query id
        msg.count += sizeof(hec::QueryType);         // query type
        msg.count += sizeof(hec::QueryResultType);   // result type
        msg.count += sizeof(DataType);          // window type
        msg.count += sizeof(hec::DatasetID);    // dataset id
        std::vector<double> coords = query->getCoords();
        msg.count += sizeof(int);               // number of coord values
        msg.count += coords.size() * sizeof(double);    // coord values
        
        // allocate space
        msg.data = (char*) malloc(msg.count * sizeof(char));
        if (msg.data == nullptr) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = msg.data;

        // put objects in buffer
        *reinterpret_cast<int*>(localBuffer) = (int) query->getQueryID();
        localBuffer += sizeof(int);
        *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) query->getQueryType();
        localBuffer += sizeof(hec::QueryType);
        *reinterpret_cast<hec::QueryResultType*>(localBuffer) = query->getResultType();
        localBuffer += sizeof(hec::QueryResultType);
        *reinterpret_cast<DataType*>(localBuffer) = (DataType) query->getWindowType();
        localBuffer += sizeof(DataType);
        *reinterpret_cast<int*>(localBuffer) = (int) query->getDatasetID();
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = (int) coords.size();
        localBuffer += sizeof(int);
        for (auto &val : coords) {
            *reinterpret_cast<double*>(localBuffer) = val;
            localBuffer += sizeof(double);
        }
        return DBERR_OK;
    }


    static DB_STATUS packJoinQuery(hec::JoinQuery *query, SerializedMsg<char> &msg) {
        msg.count = 0;
        msg.count += sizeof(int);               // query id
        msg.count += sizeof(hec::QueryType);         // query type
        msg.count += sizeof(hec::QueryResultType);   // result type
        msg.count += 2 * sizeof(hec::DatasetID);   // dataset R and S ids
        // allocate space
        msg.data = (char*) malloc(msg.count * sizeof(char));
        if (msg.data == nullptr) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = msg.data;

        // put objects in buffer
        *reinterpret_cast<int*>(localBuffer) = (int) query->getQueryID();
        localBuffer += sizeof(int);
        *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) query->getQueryType();
        localBuffer += sizeof(hec::QueryType);
        *reinterpret_cast<hec::QueryResultType*>(localBuffer) = query->getResultType();
        localBuffer += sizeof(hec::QueryResultType);
        *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) query->getDatasetRid();
        localBuffer += sizeof(hec::DatasetID);
        *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) query->getDatasetSid();
        localBuffer += sizeof(hec::DatasetID);

        return DBERR_OK;
    }

    DB_STATUS packQuery(hec::Query *query, SerializedMsg<char> &msg) {
        DB_STATUS ret = DBERR_OK;
        if (auto rangeQuery = dynamic_cast<hec::RangeQuery*>(query)){
            // range query
            ret = packRangeQuery(rangeQuery, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack range query.");
                return ret;
            }
        } else if(auto joinQuery = dynamic_cast<hec::JoinQuery*>(query)) {
            // join query
            ret = packJoinQuery(joinQuery, msg);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed to pack range query.");
                return ret;
            }
        } else {
            logger::log_error(DBERR_INVALID_DATATYPE, "Invalid query type");
            return DBERR_INVALID_DATATYPE;
        }
        return ret;
    }
}

namespace unpack
{
    DB_STATUS unpackIntegers(SerializedMsg<int> &integersMsg, std::vector<int> &integers) {
        integers.reserve(integersMsg.count);
        for (int i=0; i<integersMsg.count; i++) {
            integers.push_back(integersMsg.data[i]);
        }
        return DBERR_OK;
    }

    DB_STATUS unpackSystemMetadata(SerializedMsg<char> &sysMetadataMsg) {
        PartitioningType partitioningType;
        int partPartitionsPerDim, distPartitionsPerDim;
        char *localBuffer = sysMetadataMsg.data;
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
        for (auto& it: g_config.datasetOptions.datasets) {
            it.second.approxType = AT_APRIL;
            it.second.aprilConfig.setN(N);
            it.second.aprilConfig.compression = g_config.approximationMetadata.aprilConfig.compression;
            it.second.aprilConfig.partitions = g_config.approximationMetadata.aprilConfig.partitions;
        }

        return DBERR_OK;
    }

    DB_STATUS unpackDatasetIndexes(SerializedMsg<int> &msg, std::vector<int> &datasetIndexes) {
        int count = msg.data[0];
        datasetIndexes.clear();
        datasetIndexes.reserve(count);
        for (int i=1; i<=count; i++) {
            datasetIndexes.push_back(msg.data[i]);
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

    DB_STATUS unpackDatasetLoadMsg(SerializedMsg<char> &msg, Dataset &dataset, DatasetIndex &datasetIndex) {
        char *localBuffer = msg.data;
        // dataset index
        datasetIndex = (DatasetIndex) *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        return DBERR_OK;
    }

    DB_STATUS unpackQuery(SerializedMsg<char> &msg, hec::Query** queryPtr) {
        DB_STATUS ret = DBERR_OK;

        char *localBuffer = msg.data;
        int length;
        // query id
        int id = (int) *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // query type
        hec::QueryType queryType = (hec::QueryType) *reinterpret_cast<const hec::QueryType*>(localBuffer);
        localBuffer += sizeof(hec::QueryType);
        // query result type
        hec::QueryResultType queryResultType = (hec::QueryResultType) *reinterpret_cast<const hec::QueryResultType*>(localBuffer);
        localBuffer += sizeof(hec::QueryResultType);

        switch (queryType) {
            case hec::Q_RANGE:
                {
                    // unpack the rest of the info
                    int windowType = (DataType) *reinterpret_cast<const DataType*>(localBuffer);
                    localBuffer += sizeof(DataType);
                    hec::DatasetID datasetID = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(localBuffer);
                    localBuffer += sizeof(hec::DatasetID);
                    int coordsSize = (int) *reinterpret_cast<const int*>(localBuffer);
                    localBuffer += sizeof(int);
                    std::vector<double> coords(coordsSize);
                    for (int i=0; i<coordsSize; i++) {
                        coords[i] = (double) *reinterpret_cast<const double*>(localBuffer);
                        localBuffer += sizeof(double);
                    }
                    // the caller is responsible for freeing this memory
                    hec::RangeQuery* rangeQuery = new hec::RangeQuery(datasetID, id, windowType, coords, queryResultType);
                    (*queryPtr) = rangeQuery;
                }
                break;
            case hec::Q_DISJOINT_JOIN:
            case hec::Q_INTERSECTION_JOIN:
            case hec::Q_INSIDE_JOIN:
            case hec::Q_CONTAINS_JOIN:
            case hec::Q_COVERS_JOIN:
            case hec::Q_COVERED_BY_JOIN:
            case hec::Q_MEET_JOIN:
            case hec::Q_EQUAL_JOIN:
            case hec::Q_FIND_RELATION_JOIN:
                {
                    hec::DatasetID datasetRid = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(localBuffer);
                    localBuffer += sizeof(hec::DatasetID);
                    hec::DatasetID datasetSid = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(localBuffer);
                    localBuffer += sizeof(hec::DatasetID);
                    // the caller is responsible for freeing this memory
                    hec::JoinQuery* joinQuery = new hec::JoinQuery(datasetRid, datasetSid, id, queryType, queryResultType);
                    (*queryPtr) = joinQuery;
                }
                break;
            default:
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type in message.");
                return DBERR_QUERY_INVALID_TYPE;
        }

        return ret;
    }
}