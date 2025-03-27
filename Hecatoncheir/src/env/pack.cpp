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

    DB_STATUS packShape(Shape *shape, SerializedMsg<char> &msg) {
        DB_STATUS ret = DBERR_OK;
        // calculate size
        msg.count = 0;
        msg.count += sizeof(size_t);                // ID
        msg.count += sizeof(DataType);              // DataType
        msg.count += 4 * sizeof(double);            // MBR
        msg.count += sizeof(int);                   // vertex count
        msg.count += shape->getVertexCount() * sizeof(double) * 2;  // vertices

        // allocate space
        msg.data = (char*) malloc(msg.count * sizeof(char));
        if (msg.data == NULL) {
            // malloc failed
            return DBERR_MALLOC_FAILED;
        }
        char* localBuffer = msg.data;

        // add data type
        *reinterpret_cast<DataType*>(localBuffer) = shape->getSpatialType();
        localBuffer += sizeof(DataType);
        // id
        *reinterpret_cast<size_t*>(localBuffer) = shape->recID;
        localBuffer += sizeof(size_t);
        // mbr
        *reinterpret_cast<double*>(localBuffer) = shape->mbr.pMin.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = shape->mbr.pMin.y;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = shape->mbr.pMax.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = shape->mbr.pMax.y;
        localBuffer += sizeof(double);
        // vertex count
        *reinterpret_cast<int*>(localBuffer) = shape->getVertexCount();
        localBuffer += sizeof(int);
        // serialize and copy coordinates to buffer
        double* bufferPtr;
        int bufferElementCount;
        ret = shape->serializeCoordinates(&bufferPtr, bufferElementCount);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Serializing coordinates failed.");
            return ret;
        }
        std::memcpy(localBuffer, bufferPtr, bufferElementCount * sizeof(double));
        localBuffer += bufferElementCount * sizeof(double);
        return ret;
    }

    DB_STATUS packSystemMetadata(SerializedMsg<char> &sysMetadataMsg) {
        sysMetadataMsg.count = 0;
        sysMetadataMsg.count += sizeof(SystemSetupType);      // cluster or local
        sysMetadataMsg.count += sizeof(int);                    // world size
        sysMetadataMsg.count += 2*sizeof(int);                  // dist + part partitions per dimension
        sysMetadataMsg.count += sizeof(int);                  // partitioning type
        sysMetadataMsg.count += sizeof(int);                  // send/no send
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

        *reinterpret_cast<int*>(localBuffer) = g_world_size;
        localBuffer += sizeof(int);

        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->getDistributionPPD();
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->getPartitioningPPD();
        localBuffer += sizeof(int);

        *reinterpret_cast<PartitioningType*>(localBuffer) = g_config.partitioningMethod->getType();
        localBuffer += sizeof(PartitioningType);

        *reinterpret_cast<int*>(localBuffer) = g_config.partitioningMethod->send;
        localBuffer += sizeof(int);

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
        msg.count += sizeof(hec::DatasetID);    // dataset id
        msg.count += sizeof(int);               // wkt text length
        msg.count += query->getWKT().length() * sizeof(char);    // wkt string

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
        *reinterpret_cast<int*>(localBuffer) = (int) query->getDatasetID();
        localBuffer += sizeof(int);
        
        *reinterpret_cast<int*>(localBuffer) = query->getWKT().length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, query->getWKT().data(), query->getWKT().length() * sizeof(char));
        localBuffer += query->getWKT().length() * sizeof(char);

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

    DB_STATUS packQueryBatch(std::vector<hec::Query*> *batch, SerializedMsg<char> &batchMsg) {
        DB_STATUS ret = DBERR_OK;
        // count total size
        batchMsg.count = 0;

        // total queries
        batchMsg.count += sizeof(int);     

        for (int i=0; i<batch->size(); i++) {
            if (auto rangeQuery = dynamic_cast<hec::RangeQuery*>(batch->at(i))){
                batchMsg.count += sizeof(int);                                        // query id
                batchMsg.count += sizeof(hec::QueryType);                             // query type
                batchMsg.count += sizeof(hec::QueryResultType);                       // result type
                batchMsg.count += sizeof(hec::DatasetID);                             // dataset id
                batchMsg.count += sizeof(int);                                        // wkt text length
                batchMsg.count += rangeQuery->getWKT().length() * sizeof(char);       // wkt string
            } else if (auto joinQuery = dynamic_cast<hec::JoinQuery*>(batch->at(i))) {
                batchMsg.count += sizeof(int);                                       // query id
                batchMsg.count += sizeof(hec::QueryType);                            // query type
                batchMsg.count += sizeof(hec::QueryResultType);                      // result type
                batchMsg.count += 2 * sizeof(hec::DatasetID);                        // dataset R and S ids
            } else {
                logger::log_error(DBERR_INVALID_DATATYPE, "Invalid query type");
                return DBERR_INVALID_DATATYPE;
            }
        }
        
        // allocate space
        batchMsg.data = (char*) malloc(batchMsg.count * sizeof(char));
        if (batchMsg.data == nullptr) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = batchMsg.data;

        // put query count in buffer
        *reinterpret_cast<int*>(localBuffer) = (int) batch->size();
        localBuffer += sizeof(int);

        // put queries in buffer
        for (int i=0; i<batch->size(); i++) {
            if (auto rangeQuery = dynamic_cast<hec::RangeQuery*>(batch->at(i))){
                *reinterpret_cast<int*>(localBuffer) = (int) rangeQuery->getQueryID();
                localBuffer += sizeof(int);
                *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) rangeQuery->getQueryType();
                localBuffer += sizeof(hec::QueryType);
                *reinterpret_cast<hec::QueryResultType*>(localBuffer) = rangeQuery->getResultType();
                localBuffer += sizeof(hec::QueryResultType);
                *reinterpret_cast<int*>(localBuffer) = (int) rangeQuery->getDatasetID();
                localBuffer += sizeof(int); 
                *reinterpret_cast<int*>(localBuffer) = rangeQuery->getWKT().length();
                localBuffer += sizeof(int);
                std::memcpy(localBuffer, rangeQuery->getWKT().data(), rangeQuery->getWKT().length() * sizeof(char));
                localBuffer += rangeQuery->getWKT().length() * sizeof(char);
            } else if (auto joinQuery = dynamic_cast<hec::JoinQuery*>(batch->at(i))) {
                *reinterpret_cast<int*>(localBuffer) = (int) joinQuery->getQueryID();
                localBuffer += sizeof(int);
                *reinterpret_cast<hec::QueryType*>(localBuffer) = (hec::QueryType) joinQuery->getQueryType();
                localBuffer += sizeof(hec::QueryType);
                *reinterpret_cast<hec::QueryResultType*>(localBuffer) = joinQuery->getResultType();
                localBuffer += sizeof(hec::QueryResultType);
                *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) joinQuery->getDatasetRid();
                localBuffer += sizeof(hec::DatasetID);
                *reinterpret_cast<hec::DatasetID*>(localBuffer) = (hec::DatasetID) joinQuery->getDatasetSid();
                localBuffer += sizeof(hec::DatasetID);
            } else {
                logger::log_error(DBERR_INVALID_DATATYPE, "Invalid query type");
                return DBERR_INVALID_DATATYPE;
            }
        }
        
        
        return ret;
    }

    DB_STATUS packBatchResults(std::unordered_map<int, hec::QueryResult> *batchResults, SerializedMsg<char> &msg) {
        DB_STATUS ret = DBERR_OK;
        // count total size
        msg.count = 0;

        // total query results in batch
        msg.count += sizeof(int);     

        for (auto &it : *batchResults) {
            // calculate size
            msg.count += it.second.calculateBufferSize();    // optimization: this could be performed only once if every query is the same
        }
        // allocate space
        msg.data = (char*) malloc(msg.count * sizeof(char));
        if (msg.data == nullptr) {
            // malloc failed
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        char* localBuffer = msg.data;

        // add batch size
        *reinterpret_cast<int*>(localBuffer) = batchResults->size();
        localBuffer += sizeof(int);

        // logger::log_success("Packed header", batchResults->size(), "queries.");

        
        for (auto &it : *batchResults) {
            // add query id
            *reinterpret_cast<int*>(localBuffer) = it.second.getID();
            localBuffer += sizeof(int);

            // add query type
            *reinterpret_cast<hec::QueryType*>(localBuffer) = it.second.getQueryType();
            localBuffer += sizeof(hec::QueryType);

            // add query result type
            *reinterpret_cast<hec::QueryResultType*>(localBuffer) = it.second.getResultType();
            localBuffer += sizeof(hec::QueryResultType);

            if (it.second.getResultType() == hec::QR_COUNT) {
                // result count
                *reinterpret_cast<size_t*>(localBuffer) = it.second.getResultCount();
                localBuffer += sizeof(size_t);

                // relation map relation counts
                auto relationMap = it.second.getTopologyResultCount();
                for (int i=0; i<8; i++) {
                    *reinterpret_cast<size_t*>(localBuffer) = relationMap[i];
                    localBuffer += sizeof(size_t);
                }
            } else if (it.second.getResultType() == hec::QR_COLLECT) {
                // collect
                // pairs vector size
                auto pairs = it.second.getResultPairs();
                size_t pairsSize = pairs->size();
                *reinterpret_cast<size_t*>(localBuffer) = pairsSize;
                localBuffer += sizeof(size_t);
                // pairs vector
                for (auto &it : *pairs) {
                    *reinterpret_cast<size_t*>(localBuffer) = it.first;
                    localBuffer += sizeof(size_t);
                    *reinterpret_cast<size_t*>(localBuffer) = it.second;
                    localBuffer += sizeof(size_t);
                }
                // pairs map
                // for (int i=0; i<8; i++) {
                //     // relation pairs size
                //     auto collectRelationMap = queryResult->get
                //     size_t relationPairsSize = collectRelationMap[i].size();
                //     *reinterpret_cast<size_t*>(localBuffer) = relationPairsSize;
                //     localBuffer += sizeof(size_t);
                //     // relation pairs
                //     for (auto &pairIT: collectRelationMap[i]) {
                //         *reinterpret_cast<size_t*>(localBuffer) = pairIT.first;
                //         localBuffer += sizeof(size_t);
                //         *reinterpret_cast<size_t*>(localBuffer) = pairIT.second;
                //         localBuffer += sizeof(size_t);
                //     }
                // }
                return DBERR_FEATURE_UNSUPPORTED;
            } else {
                // error, unknown
                logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type.");
                return DBERR_QUERY_RESULT_INVALID_TYPE;
            }
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
        g_world_size = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
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
        // get send value
        g_config.partitioningMethod->send = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
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
                    hec::DatasetID datasetID = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(localBuffer);
                    localBuffer += sizeof(hec::DatasetID);
                    // wkt text length + string
                    int length;
                    length = *reinterpret_cast<const int*>(localBuffer);
                    localBuffer += sizeof(int);
                    std::string wktText(localBuffer, localBuffer + length);
                    localBuffer += length * sizeof(char);
                    
                    // the caller is responsible for freeing this memory
                    hec::RangeQuery* rangeQuery = new hec::RangeQuery(datasetID, id, wktText, queryResultType);
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

    DB_STATUS unpackQueryBatch(SerializedMsg<char> &msg, std::vector<hec::Query*>* queryBatchPtr) {
        DB_STATUS ret = DBERR_OK;

        char *localBuffer = msg.data;

        // total queries
        int queryCount = (int) *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        for (int i=0; i<queryCount; i++) {
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
                        hec::DatasetID datasetID = (hec::DatasetID) *reinterpret_cast<const hec::DatasetID*>(localBuffer);
                        localBuffer += sizeof(hec::DatasetID);
                        // wkt text length + string
                        int length;
                        length = *reinterpret_cast<const int*>(localBuffer);
                        localBuffer += sizeof(int);
                        std::string wktText(localBuffer, localBuffer + length);
                        localBuffer += length * sizeof(char);
                        
                        // the caller is responsible for freeing this memory
                        hec::RangeQuery* rangeQuery = new hec::RangeQuery(datasetID, id, wktText, queryResultType);
                        queryBatchPtr->emplace_back(rangeQuery);
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
                        queryBatchPtr->emplace_back(joinQuery);
                    }
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type in message.");
                    return DBERR_QUERY_INVALID_TYPE;
            }
        }
        return ret;
    }

    DB_STATUS unpackShape(SerializedMsg<char> &msg, Shape &shape) {
        DB_STATUS ret = DBERR_OK;

        char *localBuffer = msg.data;

        // id
        shape.recID = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
        localBuffer += sizeof(size_t);
        // data type
        DataType dataType = (DataType) *reinterpret_cast<const DataType*>(localBuffer);
        localBuffer += sizeof(DataType);
        // create object
        ret = shape_factory::createEmpty(dataType, shape);
        if (ret != DBERR_OK) {
            return ret;
        }
        double xMin, yMin, xMax, yMax;
        xMin = (double) *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMin = (double) *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        xMax = (double) *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMax = (double) *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        shape.setMBR(xMin, yMin, xMax, yMax);

        // vertex count
        int vertexCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // vertices
        const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
        for (size_t j = 0; j < vertexCount * 2; j+=2) {
            shape.addPoint(vertexDataPtr[j], vertexDataPtr[j+1]);
        }
        localBuffer += vertexCount * 2 * sizeof(double);

        return ret;
    }

    DB_STATUS unpackBatchResults(SerializedMsg<char> &msg, std::unordered_map<int, hec::QueryResult> &batchResults) {
        DB_STATUS ret = DBERR_OK;

        char *localBuffer = msg.data;

        // batch size
        int resultCount = (int) *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        // logger::log_success("Unpacked header", resultCount, "queries.");

        for (int i=0; i<resultCount; i++) {
            // query id
            int queryID = (int) *reinterpret_cast<const int*>(localBuffer);
            localBuffer += sizeof(int);

            // query type
            hec::QueryType queryType = (hec::QueryType) *reinterpret_cast<const hec::QueryType*>(localBuffer);
            localBuffer += sizeof(hec::QueryType);

            // query result type
            hec::QueryResultType queryResultType = (hec::QueryResultType) *reinterpret_cast<const hec::QueryResultType*>(localBuffer);
            localBuffer += sizeof(hec::QueryResultType);

            // create new query result element
            hec::QueryResult queryResult(queryID, queryType, queryResultType);

            if (queryResultType == hec::QR_COUNT) {
                // result count
                size_t resultCount = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                localBuffer += sizeof(size_t);
                queryResult.setResultCount(resultCount);

                // relation map relation counts
                size_t results;
                size_t countRelationMap[8];
                for (int i=0; i<8; i++) {
                    results = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                    localBuffer += sizeof(size_t);
                    countRelationMap[i] = results;
                }
                queryResult.setTopologyResultCount(countRelationMap);
            } else if (queryResultType == hec::QR_COLLECT) {
                // result pair vector size
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "COLLECT feature unsupported: query unpacking.");
                return DBERR_FEATURE_UNSUPPORTED;
            } else {
                // unknown
                logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type.");
                return DBERR_QUERY_RESULT_INVALID_TYPE;
            }

            // add to map
            auto it = batchResults.find(queryResult.getID());
            if (it != batchResults.end()) {
                // exists alread, merge
                it->second.mergeResults(queryResult);
            } else {
                // new entry
                batchResults[queryResult.getID()] = queryResult;
            }
        }

        return ret;
    }
}