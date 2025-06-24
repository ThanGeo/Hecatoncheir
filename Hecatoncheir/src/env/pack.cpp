#include "env/pack.h"

namespace pack
{
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

        *reinterpret_cast<int*>(localBuffer) = g_config.dirPaths.dataPath.length();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, g_config.dirPaths.dataPath.data(), g_config.dirPaths.dataPath.length() * sizeof(char));
        localBuffer += g_config.dirPaths.dataPath.length() * sizeof(char);

        *reinterpret_cast<int*>(localBuffer) = g_config.queryPipeline.MBRFilter;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.queryPipeline.IntermediateFilter;
        localBuffer += sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = g_config.queryPipeline.Refinement;
        localBuffer += sizeof(int);

        return DBERR_OK;
    }

    DB_STATUS packAPRILMetadata(AprilConfig &aprilConfig, SerializedMsg<char> &aprilMetadataMsg) {
        aprilMetadataMsg.count = 3 * sizeof(int);  // 3 integers: N, compression, partitions

        // Allocate raw byte buffer
        aprilMetadataMsg.data = (char*) malloc(aprilMetadataMsg.count);
        if (aprilMetadataMsg.data == nullptr) {
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for april metadata failed");
            return DBERR_MALLOC_FAILED;
        }

        // Write values into buffer
        char* buffer = aprilMetadataMsg.data;
        *reinterpret_cast<int*>(buffer) = aprilConfig.getN();
        buffer += sizeof(int);
        *reinterpret_cast<int*>(buffer) = aprilConfig.compression;
        buffer += sizeof(int);
        *reinterpret_cast<int*>(buffer) = aprilConfig.partitions;

        return DBERR_OK;
    }

    DB_STATUS packQueryBatch(std::vector<hec::Query*>* batch, SerializedMsg<char>& batchMsg) {
        DB_STATUS ret = DBERR_OK;
        // calculate total size: sizeof(int) for query count + serialized size of each query
        batchMsg.count = sizeof(int);
        for (auto& query : *batch) {
            batchMsg.count += query->calculateBufferSize();
        }

        // Allocate memory for total batch buffer
        batchMsg.data = (char*) malloc(batchMsg.count);
        if (batchMsg.data == nullptr) {
            logger::log_error(DBERR_MALLOC_FAILED, "Malloc for pack system metadata failed");
            return DBERR_MALLOC_FAILED;
        }
        char* writePtr = batchMsg.data;
        // Write query count at the beginning
        *reinterpret_cast<int*>(writePtr) = (int)batch->size();
        writePtr += sizeof(int);

        // Serialize each query individually into a temp buffer and copy it in
        for (auto& query : *batch) {
            char* tempBuffer = nullptr;
            int tempSize = 0;
            // tempSize = query->calculateBufferSize(); // get size
            int res = query->serialize(&tempBuffer, tempSize); // allocates + writes
            if (res < 0 || tempBuffer == nullptr) {
                logger::log_error(DBERR_SERIALIZE_FAILED, "Query batch serialization failed for query", query->getQueryID());
                free(batchMsg.data); // cleanup
                return DBERR_SERIALIZE_FAILED;
            }

            std::memcpy(writePtr, tempBuffer, tempSize);  // copy serialized data
            writePtr += tempSize;

            free(tempBuffer); // cleanup temp buffer
        }

        return ret;
    }

    DB_STATUS packBatchResults(std::unordered_map<int, hec::QResultBase*> &batchResults, SerializedMsg<char> &msg) {       
        DB_STATUS ret = DBERR_OK;
        // count total size
        msg.count = 0;
        // total query results in batch
        msg.count += sizeof(int);     
        for (auto &it : batchResults) {
            // calculate size
            msg.count += it.second->calculateBufferSize();    // optimization: this could be performed only once if every query is the same
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
        *reinterpret_cast<int*>(localBuffer) = batchResults.size();
        localBuffer += sizeof(int);
        // for each result
        for (auto &it : batchResults) {
            // add query id
            *reinterpret_cast<int*>(localBuffer) = it.second->getQueryID();
            localBuffer += sizeof(int);
            // add query type
            *reinterpret_cast<hec::QueryType*>(localBuffer) = it.second->getQueryType();
            localBuffer += sizeof(hec::QueryType);
            // add query result type
            *reinterpret_cast<hec::QueryResultType*>(localBuffer) = it.second->getResultType();
            localBuffer += sizeof(hec::QueryResultType);
            
            // cast appropriately
            switch (it.second->getQueryType()) {
                case hec::Q_RANGE:
                    switch (it.second->getResultType()) {
                        case hec::QR_COLLECT:
                        {
                            auto castedPtr = dynamic_cast<hec::QResultCollect*>(it.second);
                            // total object ids
                            *reinterpret_cast<size_t*>(localBuffer) = castedPtr->getResultCount();
                            localBuffer += sizeof(size_t);
                            // object ids
                            std::vector<size_t> ids = castedPtr->getResultList();
                            std::memcpy(localBuffer, ids.data(), ids.size() * sizeof(size_t));
                            localBuffer += ids.size() * sizeof(size_t);
                        }
                            break;
                        case hec::QR_COUNT:
                            *reinterpret_cast<size_t*>(localBuffer) = it.second->getResultCount();
                            localBuffer += sizeof(size_t);
                            break;
                        default:
                            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Unknown query result type:", it.second->getResultType());
                            return DBERR_QUERY_RESULT_INVALID_TYPE;
                    }
                    break;
                case hec::Q_INTERSECTION_JOIN:
                case hec::Q_INSIDE_JOIN:
                case hec::Q_DISJOINT_JOIN:
                case hec::Q_EQUAL_JOIN:
                case hec::Q_MEET_JOIN:
                case hec::Q_CONTAINS_JOIN:
                case hec::Q_COVERS_JOIN:
                case hec::Q_COVERED_BY_JOIN:
                    switch (it.second->getResultType()) {
                        case hec::QR_COLLECT:
                            {
                                auto castedPtr = dynamic_cast<hec::QPairResultCollect*>(it.second);
                                // total object ids
                                *reinterpret_cast<size_t*>(localBuffer) = castedPtr->getResultCount();
                                localBuffer += sizeof(size_t);
                                // object ids
                                std::vector<size_t> ids = castedPtr->getResultList();
                                std::memcpy(localBuffer, ids.data(), ids.size() * sizeof(size_t));
                                localBuffer += ids.size() * sizeof(size_t);
                            }
                            break;
                        case hec::QR_COUNT:
                            *reinterpret_cast<size_t*>(localBuffer) = it.second->getResultCount();
                            localBuffer += sizeof(size_t);
                            break;
                        default:
                            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Unknown query result type:", it.second->getResultType());
                            return DBERR_QUERY_RESULT_INVALID_TYPE;
                    }
                    break;
                case hec::Q_FIND_RELATION_JOIN:
                    switch (it.second->getResultType()) {
                        case hec::QR_COLLECT:
                            {
                                auto castedPtr = dynamic_cast<hec::QTopologyResultCollect*>(it.second);
                                std::vector<std::vector<size_t>> results = castedPtr->getTopologyResultList();
                                for (int i=0; i<castedPtr->TOTAL_RELATIONS; i++) {
                                    // total object ids
                                    *reinterpret_cast<size_t*>(localBuffer) = results[i].size();
                                    localBuffer += sizeof(size_t);
                                    // object ids
                                    std::memcpy(localBuffer, results[i].data(), results[i].size() * sizeof(size_t));
                                    localBuffer += results[i].size() * sizeof(size_t);
                                }
                            }
                            break;
                        case hec::QR_COUNT:
                        {
                            auto castedPtr = dynamic_cast<hec::QTopologyResultCount*>(it.second);
                            std::vector<size_t> results = castedPtr->getResultList();
                            for (int i=0; i<castedPtr->TOTAL_RELATIONS; i++) {
                                *reinterpret_cast<size_t*>(localBuffer) = results[i];
                                localBuffer += sizeof(size_t);
                            }
                        }
                            break;
                        default:
                            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Unknown query result type:", it.second->getResultType());
                            return DBERR_QUERY_RESULT_INVALID_TYPE;
                    }
                    break;
                case hec::Q_KNN:
                    {
                        auto castedPtr = dynamic_cast<hec::QResultkNN*>(it.second);
                        // total object ids
                        *reinterpret_cast<size_t*>(localBuffer) = castedPtr->getResultCount();
                        localBuffer += sizeof(size_t);
                        // object ids
                        std::vector<std::pair<double, size_t>> heap = castedPtr->getHeap();
                        std::memcpy(localBuffer, heap.data(), heap.size() * sizeof(std::pair<double,int>));
                        localBuffer += heap.size() * sizeof(std::pair<double,int>);
                    }
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", it.second->getQueryType());
                    return DBERR_QUERY_INVALID_TYPE;
            }
        }
        return ret;
    }

}

namespace unpack
{
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
        // get datapath length + string
        // and set the paths
        int length;
        length = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        std::string datapath(localBuffer, localBuffer + length);
        localBuffer += length * sizeof(char);
        // pipeline
        g_config.queryPipeline.MBRFilter = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        g_config.queryPipeline.IntermediateFilter = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        g_config.queryPipeline.Refinement = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        return DBERR_OK;
    }

    DB_STATUS unpackAPRILMetadata(SerializedMsg<char>& aprilMetadataMsg) {
        if (aprilMetadataMsg.count != 3 * sizeof(int)) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid APRIL metadata size");
            return DBERR_INVALID_PARAMETER;
        }

        char* buffer = aprilMetadataMsg.data;

        int N = *reinterpret_cast<int*>(buffer);
        buffer += sizeof(int);

        int compression = *reinterpret_cast<int*>(buffer);
        buffer += sizeof(int);

        int partitions = *reinterpret_cast<int*>(buffer);

        // Update global config
        g_config.approximationMetadata.aprilConfig.setN(N);
        g_config.approximationMetadata.aprilConfig.compression = compression;
        g_config.approximationMetadata.aprilConfig.partitions = partitions;
        g_config.approximationMetadata.type = AT_APRIL;

        if (auto* datasetR = g_config.datasetOptions.getDatasetR()) {
            datasetR->approxType = AT_APRIL;
            datasetR->aprilConfig.setN(N);
            datasetR->aprilConfig.compression = compression;
            datasetR->aprilConfig.partitions = partitions;
        }

        if (auto* datasetS = g_config.datasetOptions.getDatasetS()) {
            datasetS->approxType = AT_APRIL;
            datasetS->aprilConfig.setN(N);
            datasetS->aprilConfig.compression = compression;
            datasetS->aprilConfig.partitions = partitions;
        }

        return DBERR_OK;
    }

    DB_STATUS unpackQueryBatch(SerializedMsg<char>& msg, std::vector<hec::Query*>* queryBatchPtr) {
        DB_STATUS ret = DBERR_OK;

        char* localBuffer = msg.data;

        // Total query count
        int queryCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);

        for (int i = 0; i < queryCount; ++i) {
            hec::Query* query = hec::Query::createFromBuffer(localBuffer);
            if (query == nullptr) {
                logger::log_error(DBERR_DESERIALIZE_FAILED, "Failed to deserialize query object in batch");
                return DBERR_DESERIALIZE_FAILED;
            }
            queryBatchPtr->emplace_back(query);
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

    DB_STATUS unpackBatchResults(SerializedMsg<char> &msg, std::unordered_map<int, hec::QResultBase*> &batchResults) {
        DB_STATUS ret = DBERR_OK;
        char *localBuffer = msg.data;
        // batch size
        int resultCount = (int) *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // for each object in the batch
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
            hec::QResultBase* queryResult;
            int res = qresult_factory::createNew(queryID, queryType, queryResultType, &queryResult);
            if (res != 0) {
                logger::log_error(DBERR_OBJ_CREATION_FAILED, "Failed to create query result object.");
                return DBERR_OBJ_CREATION_FAILED;
            }

            // cast appropriately
            switch (queryType) {
                case hec::Q_RANGE:
                    switch (queryResultType) {
                        case hec::QR_COLLECT:
                        {
                            // read total ids count
                            size_t idCount = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                            localBuffer += sizeof(size_t);
                            // read the ids
                            std::vector<size_t> ids(idCount);
                            std::memcpy(ids.data(), localBuffer, idCount * sizeof(size_t));
                            localBuffer += idCount * sizeof(size_t);
                            // cast and set to object
                            auto castedPtr = dynamic_cast<hec::QResultCollect*>(queryResult);
                            castedPtr->setResult(ids);
                        }
                            break;
                        case hec::QR_COUNT:
                        {
                            size_t result = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                            localBuffer += sizeof(size_t);
                            auto castedPtr = dynamic_cast<hec::QResultCount*>(queryResult);
                            castedPtr->setResult(result);
                        }
                            break;
                        default:
                            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Unknown query result type:", queryResultType);
                            return DBERR_QUERY_RESULT_INVALID_TYPE;
                    }
                    break;
                case hec::Q_INTERSECTION_JOIN:
                case hec::Q_INSIDE_JOIN:
                case hec::Q_DISJOINT_JOIN:
                case hec::Q_EQUAL_JOIN:
                case hec::Q_MEET_JOIN:
                case hec::Q_CONTAINS_JOIN:
                case hec::Q_COVERS_JOIN:
                case hec::Q_COVERED_BY_JOIN:
                    switch (queryResultType) {
                        case hec::QR_COLLECT:
                            {
                                // read total ids count
                                size_t idsCount = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                                localBuffer += sizeof(size_t);
                                // read the ids
                                std::vector<size_t> ids(idsCount);
                                std::memcpy(ids.data(), localBuffer, idsCount * sizeof(size_t));
                                localBuffer += idsCount * sizeof(size_t);
                                // cast and set to object
                                auto castedPtr = dynamic_cast<hec::QPairResultCollect*>(queryResult);
                                castedPtr->setResult(ids);
                            }
                            break;
                        case hec::QR_COUNT:
                        {
                            size_t result = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                            localBuffer += sizeof(size_t);
                            auto castedPtr = dynamic_cast<hec::QResultCount*>(queryResult);
                            castedPtr->setResult(result);
                        }
                            break;
                        default:
                            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Unknown query result type:", queryResultType);
                            return DBERR_QUERY_RESULT_INVALID_TYPE;
                    }
                    break;
                case hec::Q_FIND_RELATION_JOIN:
                    switch (queryResultType) {
                        case hec::QR_COLLECT:
                            {
                                auto castedPtr = dynamic_cast<hec::QTopologyResultCollect*>(queryResult);
                                for (int i=0; i<castedPtr->TOTAL_RELATIONS; i++) {
                                    // read total ids count
                                    size_t idsCount = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                                    localBuffer += sizeof(size_t);
                                    // read the ids
                                    std::vector<size_t> ids(idsCount);
                                    std::memcpy(ids.data(), localBuffer, idsCount * sizeof(size_t));
                                    localBuffer += idsCount * sizeof(size_t);
                                    // cast and set to object
                                    castedPtr->setResult(i, ids);
                                }
                            }
                            break;
                        case hec::QR_COUNT:
                        {
                            auto castedPtr = dynamic_cast<hec::QTopologyResultCount*>(queryResult);
                            for (int i=0; i<castedPtr->TOTAL_RELATIONS; i++) {
                                size_t result = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                                localBuffer += sizeof(size_t);
                                castedPtr->setResult(i, result);
                            }
                        }
                            break;
                        default:
                            logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Unknown query result type:", queryResultType);
                            return DBERR_QUERY_RESULT_INVALID_TYPE;
                    }
                    break;
                case hec::Q_KNN:
                    {
                        // read total ids count
                        size_t idCount = (size_t) *reinterpret_cast<const size_t*>(localBuffer);
                        localBuffer += sizeof(size_t);
                        // read the ids
                        std::vector<std::pair<double,int>> heapElements(idCount);
                        std::memcpy(heapElements.data(), localBuffer, idCount * sizeof(std::pair<double,int>));
                        localBuffer += idCount * sizeof(std::pair<double,int>);
                        // create the heap
                        std::priority_queue<std::pair<double, size_t>> heap;
                        for (auto &it: heapElements) {
                            heap.emplace(it);
                        }
                        // cast and set to object
                        auto castedPtr = dynamic_cast<hec::QResultkNN*>(queryResult);
                        castedPtr->setResult(heap);
                    }
                    break;
                default:
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type:", queryType);
                    return DBERR_QUERY_INVALID_TYPE;
            }
            // add to map 
            auto it = batchResults.find(queryResult->getQueryID());
            if (it != batchResults.end()) {
                // exists already, merge into existing object
                it->second->mergeResults(queryResult);
                delete queryResult;  // safe to delete after merge
            } else {
                // new entry: take ownership directly
                batchResults[queryResult->getQueryID()] = queryResult;
            }
        }

        return ret;
    }
}