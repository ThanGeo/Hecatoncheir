#include "containers.h"

Config g_config;

QueryOutput g_queryOutput;

DB_STATUS queryResultReductionFunc(QueryOutput &in, QueryOutput &out) {
    DB_STATUS ret = DBERR_OK;
    out.queryResults += in.queryResults;
    out.postMBRFilterCandidates += in.postMBRFilterCandidates;
    out.refinementCandidates += in.refinementCandidates;
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
            out.trueHits += in.trueHits;
            out.trueNegatives += in.trueNegatives;
            return ret;
        case Q_FIND_RELATION:
            for (auto &it : in.topologyRelationsResultMap) {
                out.topologyRelationsResultMap[it.first] += it.second;
            }
            return ret;
        default:
            logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query type:", g_config.queryMetadata.type);
            return DBERR_QUERY_INVALID_TYPE;            
    }
}

QueryOutput::QueryOutput() {
    // result
    this->reset();
}

void QueryOutput::reset() {
    // result
    this->queryResults = 0;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    this->topologyRelationsResultMap[TR_DISJOINT] = 0;
    this->topologyRelationsResultMap[TR_EQUAL] = 0;
    this->topologyRelationsResultMap[TR_MEET] = 0;
    this->topologyRelationsResultMap[TR_CONTAINS] = 0;
    this->topologyRelationsResultMap[TR_COVERS] = 0;
    this->topologyRelationsResultMap[TR_COVERED_BY] = 0;
    this->topologyRelationsResultMap[TR_INSIDE] = 0;
    this->topologyRelationsResultMap[TR_INTERSECT] = 0;
    // statistics
    this->postMBRFilterCandidates = 0;
    this->refinementCandidates = 0;
    this->trueHits = 0;
    this->trueNegatives = 0;
    
    // time
    this->mbrFilterTime = 0;
    this->iFilterTime = 0;
    this->refinementTime = 0;
}

void QueryOutput::countAPRILresult(int result) {
    switch (result) {
        case TRUE_HIT:
            this->trueHits += 1;
            this->queryResults += 1;
            break;
        case TRUE_NEGATIVE:
            this->trueNegatives += 1;
            break;
        case INCONCLUSIVE:
            this->refinementCandidates += 1;
            break;
    }
}

void QueryOutput::countResult(){
    this->queryResults += 1;
}

void QueryOutput::countMBRresult(){
    this->postMBRFilterCandidates += 1;
}

void QueryOutput::countRefinementCandidate(){
    this->refinementCandidates += 1;
}

void QueryOutput::countTopologyRelationResult(int relation) {
    this->topologyRelationsResultMap[relation] += 1;
}

int QueryOutput::getResultForTopologyRelation(TopologyRelationE relation) {
    auto it = topologyRelationsResultMap.find(relation);
    if (it != topologyRelationsResultMap.end()) {
        return it->second;
    }
    return -1;
}

void QueryOutput::setTopologyRelationResult(int relation, int result) {
    topologyRelationsResultMap[relation] = result;
}

void QueryOutput::shallowCopy(QueryOutput &other) {
    // result
    this->queryResults = other.queryResults;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    for (auto &it: other.topologyRelationsResultMap) {
        this->topologyRelationsResultMap[it.first] = it.second;
    }
    // statistics
    this->postMBRFilterCandidates = other.postMBRFilterCandidates;
    this->refinementCandidates = other.refinementCandidates;
    this->trueHits = other.trueHits;
    this->trueNegatives = other.trueNegatives;
    
    // time
    this->mbrFilterTime = other.mbrFilterTime;
    this->iFilterTime = other.iFilterTime;
    this->refinementTime = other.refinementTime;
}

void AprilData::printALLintervals() {
    for (int i=0; i<intervalsALL.size(); i+=2) {
        printf("[%u,%u)\n", intervalsALL[i], intervalsALL[i+1]);
    }
}

void AprilData::printALLcells(uint32_t n) {
    uint32_t x,y,d;
    for (int i=0; i<intervalsALL.size(); i+=2) {
        for (int j=intervalsALL[i]; j<intervalsALL[i+1]; j++) {
            hilbert::d2xy(n, j, x, y);
            printf("(%u,%u),", x, y);
        }
    }
    printf("\n");
}

void AprilData::printFULLintervals() {
    for (int i=0; i<intervalsFULL.size(); i+=2) {
        printf("[%u,%u)\n", intervalsFULL[i], intervalsFULL[i+1]);
    }
}

void AprilConfig::setN(int N) {
    this->N = N;
    this->cellsPerDim = pow(2,N);
}

int AprilConfig::getN() {
    return N;
}

int AprilConfig::getCellsPerDim() {
    return cellsPerDim;
}

std::vector<Shape*>* Partition::getContainerClassContents(TwoLayerClassE classType) {
    if (classType < CLASS_A || classType > CLASS_D) {
        logger::log_error(DBERR_OUT_OF_BOUNDS, "class type index out of bounds");
        return nullptr;
    }
    return &classIndex[classType];
}

void Partition::addObjectOfClass(Shape *objectRef, TwoLayerClassE classType) {
    classIndex[classType].push_back(objectRef);
    // if (this->partitionID == 289605) {
    //     printf("partition %d class %s new size after insertion of %ld: %ld\n", this->partitionID, mapping::twoLayerClassIntToStr(classType).c_str(), objectRef->recID, classIndex[classType].size());
    // }
    // printf("Adding object of type %s and class %s in partition %ld\n", objectRef->getShapeType().c_str(), mapping::twoLayerClassIntToStr(classType).c_str(), this->partitionID);
}

Shape* Dataset::getObject(size_t recID) {
    auto it = objects.find(recID);
    if (it == objects.end()) {
        return nullptr;
    }
    return &it->second;
}

DB_STATUS Dataset::addObject(Shape &object) {
    // add object to the objects map
    objects[object.recID] = object;
    // get object reference
    Shape* objectRef = this->getObject(object.recID);
    if (objectRef == nullptr) {
        logger::log_error(DBERR_INVALID_KEY, "Object with id", object.recID, "does not exist in the object map.");
        return DBERR_INVALID_KEY;
    }
    // insert reference to partition index
    for (int i=0; i<object.getPartitionCount(); i++) {
        this->twoLayerIndex.addObject(object.getPartitionID(i), object.getPartitionClass(i), objectRef);
    }
    // keep the ID in the list
    objectIDs.push_back(object.recID);
    /** add object to section map (section 0) @warning do not remove, will break the parallel in-memory APRIL generation */
    this->addObjectToSectionMap(0, object.recID);

    return DBERR_OK;
}

int Dataset::calculateBufferSize() {
    int size = 0;
    // dataset data type
    size += sizeof(DataTypeE);
    // dataset nickname: length + string
    size += sizeof(int) + (nickname.length() * sizeof(char));     
    // dataset's dataspace metadata (MBR)
    size += 4 * sizeof(double);
    
    return size;
}

/**
 * serialize dataset metadata (only specific stuff)
 */
DB_STATUS Dataset::serialize(char **buffer, int &bufferSize) {
    int position = 0;
    // calculate size
    int bufferSizeRet = calculateBufferSize();
    // allocate space
    char* localBuffer = (char*) malloc(bufferSizeRet * sizeof(char));

    if (localBuffer == NULL) {
        // malloc failed
        return DBERR_MALLOC_FAILED;
    }

    // add datatype
    memcpy(localBuffer + position, &dataType, sizeof(DataTypeE));
    position += sizeof(DataTypeE);
    // add dataset nickname length + string
    int nicknameLength = nickname.length();
    memcpy(localBuffer + position, &nicknameLength, sizeof(int));
    position += sizeof(int);
    memcpy(localBuffer + position, nickname.data(), nicknameLength);
    position += nicknameLength * sizeof(char);
    // add dataset's dataspace MBR
    memcpy(localBuffer + position, &dataspaceMetadata.xMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceMetadata.yMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceMetadata.xMaxGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceMetadata.yMaxGlobal, sizeof(double));
    position += sizeof(double);
    // set and return
    (*buffer) = localBuffer;
    bufferSize = bufferSizeRet;
    return DBERR_OK;
}

DB_STATUS Dataset::deserialize(const char *buffer, int bufferSize) {
    int nicknameLength;
    int position = 0;
    double xMin, yMin, xMax, yMax;
    
    // dataset data type
    memcpy(&dataType, buffer + position, sizeof(DataTypeE));
    position += sizeof(DataTypeE);
    // dataset nickname length + string
    memcpy(&nicknameLength, buffer + position, sizeof(int));
    position += sizeof(int);
    nickname.assign(buffer + position, nicknameLength);
    position += nicknameLength * sizeof(char);
    // dataset dataspace MBR
    memcpy(&xMin, buffer + position, sizeof(double));
    position += sizeof(double);
    memcpy(&yMin, buffer + position, sizeof(double));
    position += sizeof(double);
    memcpy(&xMax, buffer + position, sizeof(double));
    position += sizeof(double);
    memcpy(&yMax, buffer + position, sizeof(double));
    position += sizeof(double);
    // set
    dataspaceMetadata.set(xMin, yMin, xMax, yMax);

    if (position == bufferSize) {
        // all is well
        return DBERR_OK;
    }
    else{
        return DBERR_DESERIALIZE;
    }
}

void Dataset::addAprilDataToApproximationDataMap(const uint sectionID, const size_t recID, const AprilData &aprilData) {
    // store april data
    this->sectionMap[sectionID].aprilData[recID] = aprilData;
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists, append
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap[recID] = sectionIDs;
    }
}

void Dataset::addObjectToSectionMap(const uint sectionID, const size_t recID) {
    AprilData aprilData;
    this->sectionMap[sectionID].aprilData[recID] = aprilData;
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap[recID] = sectionIDs;
    }
}

DB_STATUS Dataset::addIntervalsToAprilData(const uint sectionID, const size_t recID, const int numIntervals, const std::vector<uint32_t> &intervals, const bool ALL) {
    // retrieve section
    auto secIT = this->sectionMap.find(sectionID);
    if (secIT == this->sectionMap.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find section ID", sectionID, "in section map of dataset", this->nickname);
        return DBERR_INVALID_PARAMETER;
    }
    // retrieve object
    auto it = secIT->second.aprilData.find(recID);
    if (it == secIT->second.aprilData.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find recID", recID, "in section map of dataset", this->nickname);
        return DBERR_INVALID_PARAMETER;
    }
    // replace intervals in april data
    if (ALL) {
        it->second.numIntervalsALL = numIntervals;
        it->second.intervalsALL = intervals;
    } else {
        it->second.numIntervalsFULL = numIntervals;
        it->second.intervalsFULL = intervals;
    }
    return DBERR_OK;
}

void Dataset::addAprilData(const uint sectionID, const size_t recID, const AprilData &aprilData) {
    this->sectionMap[sectionID].aprilData[recID] = aprilData;
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap[recID] = sectionIDs;
    }
}

AprilData* Dataset::getAprilDataBySectionAndObjectID(uint sectionID, size_t recID) {
    auto sec = this->sectionMap.find(sectionID);
    if (sec != this->sectionMap.end()){
        auto obj = sec->second.aprilData.find(recID);
        if (obj != sec->second.aprilData.end()) {
            return &(obj->second);
        }
    }
    return nullptr;
}

DB_STATUS Dataset::setAprilDataForSectionAndObjectID(uint sectionID, size_t recID, AprilData &aprilData) {
    auto sec = this->sectionMap.find(sectionID);
    if (sec == this->sectionMap.end()){
        // add new section
        this->addObjectToSectionMap(sectionID, recID);
        sec = this->sectionMap.find(sectionID);
        logger::log_warning("Added new section in the section map. This shouldn't happen in parallel APRIL generation.");
    }
    // section exists
    auto obj = sec->second.aprilData.find(recID);
    if (obj != sec->second.aprilData.end()) {
        // object exists already
        obj->second = aprilData;
    } else {
        // add april data for the object, in section map
        sec->second.aprilData[recID] = aprilData;
        logger::log_warning("Added new object in the section map. This shouldn't happen in parallel APRIL generation.");
    }
    return DBERR_OK;
}

void Dataset::printObjectsGeometries() {
    for (auto &it : objectIDs) {
        printf("MBR: (%f,%f),(%f,%f)\n", this->objects[it].mbr.pMin.x, this->objects[it].mbr.pMin.y, this->objects[it].mbr.pMax.x, this->objects[it].mbr.pMax.y);
        this->objects[it].printGeometry();
    }
}

void Dataset::printObjectsPartitions() {
    for (auto &it : objectIDs) {
        printf("id: %zu, type %s, Partitions:", it, objects[it].getShapeType().c_str());
        for (int i=0; i<objects[it].getPartitionCount(); i++) {
            printf("(%d,%s),", objects[it].getPartitionID(i), mapping::twoLayerClassIntToStr((TwoLayerClassE) objects[it].getPartitionClass(i)).c_str());
        }
        printf("\n");
    }
}

void Dataset::printPartitions() {
    for (auto it : this->twoLayerIndex.partitions) {
        printf("Partition %d\n", it.partitionID);
        for (int c=CLASS_A; c<=CLASS_D; c++) {
            printf("Class %s:\n", mapping::twoLayerClassIntToStr((TwoLayerClassE) c).c_str());
            std::vector<Shape*>* container = it.getContainerClassContents((TwoLayerClassE) c);
            if (container == nullptr) {
                printf("    nullptr\n");
            } else {
                printf("    size %ld, content ids:\n", container->size());
                printf("        ");
                for (auto contIT : *container) {
                    printf("%ld,", contIT->recID);
                    // printf("%ld (%s),", contIT->recID, contIT->getShapeType().c_str());
                }
                printf("\n");
            }
        }
    }
}

Dataset* DatasetMetadata::getDatasetByNickname(std::string &nickname) {
    auto it = datasets.find(nickname);
    if (it == datasets.end()) {
        return nullptr;
    }        
    return &it->second;
}

DataspaceMetadata::DataspaceMetadata() {
    xMinGlobal = std::numeric_limits<int>::max();
    yMinGlobal = std::numeric_limits<int>::max();
    xMaxGlobal = -std::numeric_limits<int>::max();
    yMaxGlobal = -std::numeric_limits<int>::max();
}

void DataspaceMetadata::set(double xMinGlobal, double yMinGlobal, double xMaxGlobal, double yMaxGlobal) {
    this->xMinGlobal = xMinGlobal - EPS;
    this->yMinGlobal = yMinGlobal - EPS;
    this->xMaxGlobal = xMaxGlobal + EPS;
    this->yMaxGlobal = yMaxGlobal + EPS;
    this->xExtent = this->xMaxGlobal - this->xMinGlobal;
    this->yExtent = this->yMaxGlobal - this->yMinGlobal;
    this->maxExtent = std::max(this->xExtent, this->yExtent);
    // printf("-------------------------\n");
    // printf("Dataspace bounds: (%f,%f),(%f,%f)\n", this->xMinGlobal, this->yMinGlobal, this->xMaxGlobal, this->yMaxGlobal);
    // printf("xExtent: %f, yExtent: %f\n", this->xExtent, this->yExtent);
    // printf("Max extent: %f\n", this->maxExtent);
    // printf("-------------------------\n");
}

void DataspaceMetadata::clear() {
    xMinGlobal = 0;
    yMinGlobal = 0;
    xMaxGlobal = 0;
    yMaxGlobal = 0;
    xExtent = 0;
    yExtent = 0;
}

Partition* TwoLayerIndex::getOrCreatePartition(int partitionID) {
    // return &it->second;
    auto it = partitionMap.find(partitionID);
    if (it == partitionMap.end()) {
        // new partition
        Partition partition(partitionID);
        partitions.push_back(partition);
        size_t newIndex = partitions.size() - 1;
        partitionMap[partitionID] = newIndex;
        return &partitions[newIndex];
    }
    // existing partition
    return &partitions[it->second];
}

void TwoLayerIndex::addObject(int partitionID, TwoLayerClassE classType, Shape* objectRef) {
    auto it = partitionMap.find(partitionID);
    if (it == partitionMap.end()) {
        // new partition
        Partition partition(partitionID);
        // add object reference
        partition.addObjectOfClass(objectRef, classType);
        // save partition
        partitions.push_back(partition);
        int newIndex = partitions.size() - 1;
        partitionMap[partitionID] = newIndex;
    } else {
        // existing partition
        partitions[it->second].addObjectOfClass(objectRef, classType);
    }
}

Partition* TwoLayerIndex::getPartition(int partitionID) {
    auto it = partitionMap.find(partitionID);
    if (it == partitionMap.end()) {
        // does not exist
        return nullptr;
    } 
    // exists
    return &partitions[it->second];
}

void TwoLayerIndex::sortPartitionsOnY() {
    for (auto &it: partitions) {
        // sort A
        std::vector<Shape*>* objectsA = it.getContainerClassContents(CLASS_A);
        if (objectsA != nullptr) {
            std::sort(objectsA->begin(), objectsA->end(), compareByY);
        }
        // sort C
        std::vector<Shape*>* objectsC = it.getContainerClassContents(CLASS_C);
        if (objectsC != nullptr) {
            std::sort(objectsC->begin(), objectsC->end(), compareByY);
        }
    }
}

int DatasetMetadata::getNumberOfDatasets() {
    return numberOfDatasets;
}

void DatasetMetadata::clear() {
    numberOfDatasets = 0;
    R = nullptr;
    S = nullptr;
    datasets.clear();
    dataspaceMetadata.clear();
}

Dataset* DatasetMetadata::getDatasetR() {
    return R;
}

Dataset* DatasetMetadata::getDatasetS() {
    return S;
}

DB_STATUS DatasetMetadata::getDatasetByIdx(DatasetIndexE datasetIndex, Dataset **datasetRef) {
    switch (datasetIndex) {
        case DATASET_R:
            (*datasetRef) = R;
            break;
        case DATASET_S:
            (*datasetRef) = S;
            break;
        default:
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid dataset index:", datasetIndex);
            (*datasetRef) = nullptr;
            return DBERR_INVALID_PARAMETER;
    }
    return DBERR_OK;
    
}
/**
@brief adds a Dataset to the configuration's dataset metadata
 * @warning it has to be an empty dataset BUT its nickname needs to be set
 */
DB_STATUS DatasetMetadata::addDataset(DatasetIndexE datasetIdx, Dataset &dataset) {
    // add to datasets struct
    datasets[dataset.nickname] = dataset;
    switch (datasetIdx) {
        case DATASET_R:
            // R is being added
            R = &datasets[dataset.nickname];
            break;
        case DATASET_S:
            // S is being added
            S = &datasets[dataset.nickname];
            break;
        default:
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid dataset index. Use only DATASET_R or DATASET_S.");
            return DBERR_INVALID_PARAMETER;
    }
    numberOfDatasets++;
    return DBERR_OK;
}

void DatasetMetadata::updateDataspace() {
    // find the bounds that enclose both datasets
    for (auto &it: datasets) {
        dataspaceMetadata.xMinGlobal = std::min(dataspaceMetadata.xMinGlobal, it.second.dataspaceMetadata.xMinGlobal);
        dataspaceMetadata.yMinGlobal = std::min(dataspaceMetadata.yMinGlobal, it.second.dataspaceMetadata.yMinGlobal);
        dataspaceMetadata.xMaxGlobal = std::max(dataspaceMetadata.xMaxGlobal, it.second.dataspaceMetadata.xMaxGlobal);
        dataspaceMetadata.yMaxGlobal = std::max(dataspaceMetadata.yMaxGlobal, it.second.dataspaceMetadata.yMaxGlobal);
    }
    dataspaceMetadata.xExtent = dataspaceMetadata.xMaxGlobal - dataspaceMetadata.xMinGlobal;
    dataspaceMetadata.yExtent = dataspaceMetadata.yMaxGlobal - dataspaceMetadata.yMinGlobal;
    // set as both datasets' bounds
    for (auto &it: datasets) {
        it.second.dataspaceMetadata = dataspaceMetadata;
    }
}

Batch::Batch(){}

bool Batch::isValid() {
    return !(destRank == -1 || tag == -1 || comm == nullptr);
}

void Batch::addObjectToBatch(Shape &object) {
    objects.emplace_back(object);
    objectCount += 1;
}

void Batch::setDestNodeRank(int destRank) {
    this->destRank = destRank;
}

// calculate the size needed for the serialization buffer
int Batch::calculateBufferSize() {
    int size = 0;
    size += sizeof(DataTypeE);                        // datatype
    size += sizeof(size_t);                        // objectCount
    
    for (auto &it: objects) {
        size += sizeof(size_t); // recID
        size += sizeof(int);    // partition count
        size += it.getPartitionCount() * 2 * sizeof(int); // partitions id + class
        size += sizeof(it.getVertexCount()); // vertex count
        size += it.getVertexCount() * 2 * sizeof(double);    // vertices
    }
    
    return size;
}

void Batch::clear() {
    objectCount = 0;
    objects.clear();
}

DB_STATUS Batch::serialize(char **buffer, int &bufferSize) {
    // calculate size
    int bufferSizeRet = calculateBufferSize();
    // allocate space
    (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
    if (*buffer == NULL) {
        // malloc failed
        return DBERR_MALLOC_FAILED;
    }
    char* localBuffer = *buffer;

    // add data type
    *reinterpret_cast<DataTypeE*>(localBuffer) = dataType;
    localBuffer += sizeof(DataTypeE);
    // add object count
    *reinterpret_cast<size_t*>(localBuffer) = objectCount;
    localBuffer += sizeof(size_t);

    // add batch geometry metadata
    for (auto &it : objects) {
        *reinterpret_cast<size_t*>(localBuffer) = it.recID;
        localBuffer += sizeof(size_t);
        *reinterpret_cast<int*>(localBuffer) = it.getPartitionCount();
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, it.getPartitionsRef()->data(), it.getPartitionCount() * 2 * sizeof(int));
        localBuffer += it.getPartitionCount() * 2 * sizeof(int);
       
        *reinterpret_cast<int*>(localBuffer) = it.getVertexCount();
        localBuffer += sizeof(int);

        // get reference to points in boost geometry format
        const std::vector<bg_point_xy>* pointsRef = it.getReferenceToPoints();
        // transform to continuous double vector
        std::vector<double> pointsList(it.getVertexCount()*2);
        for (int i=0; i<it.getVertexCount(); i++) {
            pointsList[i*2] = pointsRef->at(i).x(); 
            pointsList[i*2+1] = pointsRef->at(i).y(); 
        }
        // copy points to buffer
        std::memcpy(localBuffer, pointsList.data(), it.getVertexCount() * 2 * sizeof(double));
        localBuffer += it.getVertexCount() * 2 * sizeof(double);


        // printf("Serialized Object %ld with %d partitions:\n ", it.recID, it.getPartitionCount());
        // std::vector<int>* partitionRef = it.getPartitionsRef();
        // for (int i=0; i<it.getPartitionCount(); i++) {
        //     printf("(%d,%s),", it.getPartitionID(i), mapping::twoLayerClassIntToStr(it.getPartitionClass(i)).c_str());
        // }
        // printf("\n");
        // it.printGeometry();
    }
    bufferSize = bufferSizeRet;
    return DBERR_OK;
}

/**
@brief fills the struct with data from the input serialized buffer
 * The caller must free the buffer memory
 * @param buffer 
 */
DB_STATUS Batch::deserialize(const char *buffer, int bufferSize) {
    DB_STATUS ret = DBERR_OK;
    int vertexCount, partitionCount;
    const char *localBuffer = buffer;

    // get data type
    DataTypeE dataType = *reinterpret_cast<const DataTypeE*>(localBuffer);
    localBuffer += sizeof(DataTypeE);

    // get object count
    size_t objectCount = *reinterpret_cast<const size_t*>(localBuffer);
    localBuffer += sizeof(size_t);

    // extend reserve space
    objects.reserve(objects.size() + objectCount);

    // empty shape object
    Shape object;
    ret = shape_factory::createEmpty(dataType, object);
    if (ret != DBERR_OK) {
        return ret;
    }

    // deserialize fields for each object in the batch
    for (size_t i=0; i<objectCount; i++) {
        // reset object
        object.reset();
        // rec id
        object.recID = *reinterpret_cast<const size_t*>(localBuffer);
        localBuffer += sizeof(size_t);
        // partitions + partition count
        partitionCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        std::vector<int> partitions(partitionCount * 2);
        const int* partitionPtr = reinterpret_cast<const int*>(localBuffer);
        for (size_t j = 0; j < partitionCount*2; j++) {
            partitions[j] = partitionPtr[j];
        }
        localBuffer += partitionCount * 2 * sizeof(int);
        object.setPartitions(partitions, partitionCount);
        // vertex count
        vertexCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // vertices
        std::vector<double> coords(vertexCount * 2);
        const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
        for (size_t j = 0; j < vertexCount * 2; j++) {
            coords[j] = vertexDataPtr[j];
        }
        localBuffer += vertexCount * 2 * sizeof(double);
        object.setPoints(coords);

        // printf("Deserialized Object %ld with %d partitions:\n ", object.recID, object.getPartitionCount());
        // std::vector<int>* partitionRef = object.getPartitionsRef();
        // for (int i=0; i<object.getPartitionCount(); i++) {
        //     printf("(%d,%s),", object.getPartitionID(i), mapping::twoLayerClassIntToStr(object.getPartitionClass(i)).c_str());
        // }
        // printf("\n");
        // object.printGeometry();

        // add to batch
        this->addObjectToBatch(object);
    }

    return ret;
}

int TwoGridPartitioning::getDistributionGridPartitionID(int i, int j) {
    return (i + (j * distPartitionsPerDim));
}

int TwoGridPartitioning::getPartitioningGridPartitionID(int distI, int distJ, int partI, int partJ) {
    int globalI = (distI * partPartitionsPerDim) + partI;
    int globalJ = (distJ * partPartitionsPerDim) + partJ;
    // printf("Getting ID for coarse indices (%d,%d) and fine indices (%d,%d), with coarse cellsPerDim %d, fine cellsPerDim %d and global cellsPerDim %d. ID: %d\n", distI, distJ, partI, partJ, distPartitionsPerDim, partPartitionsPerDim, globalPartitionsPerDim, (globalI + (globalJ * globalPartitionsPerDim)));
    return (globalI + (globalJ * globalPartitionsPerDim));
}

int TwoGridPartitioning::getDistributionPPD() {
    return distPartitionsPerDim; 
}

int TwoGridPartitioning::getPartitioningPPD() {
    return partPartitionsPerDim; 
}

int TwoGridPartitioning::getGlobalPPD() {
    return globalPartitionsPerDim; 
}

void TwoGridPartitioning::setPartGridDataspace(double xMin, double yMin, double xMax, double yMax) {
    partGridDataspaceMetadata.set(xMin, yMin, xMax, yMax);
    partPartitionExtentX = partGridDataspaceMetadata.xExtent / partPartitionsPerDim;
    partPartitionExtentY = partGridDataspaceMetadata.yExtent / partPartitionsPerDim;
}

void TwoGridPartitioning::setPartGridDataspace(DataspaceMetadata &otherDataspaceMetadata) {
    partGridDataspaceMetadata = otherDataspaceMetadata;
}

double TwoGridPartitioning::getPartPartionExtentX() {
    return partPartitionExtentX;
}

double TwoGridPartitioning::getPartPartionExtentY() {
    return partPartitionExtentY;
}

/** @brief Get the grid's partition ID (from parent). */
int RoundRobinPartitioning::getDistributionGridPartitionID(int i, int j) {
    return (i + (j * distPartitionsPerDim));
}

/** @brief Get the grid's partition ID (from parent). */
int RoundRobinPartitioning::getPartitioningGridPartitionID(int distI, int distJ, int partI, int partJ) {
    if (distI != partI || distJ != partJ) {
        logger::log_error(DBERR_INVALID_PARAMETER, "For Round Robin partitioning, distribution and partitioning partition indices must match. Dist:", distI, distJ, "Part:", partI, partJ);
        return -1;
    }
    return (distI + (distJ * distPartitionsPerDim));
}

/** @brief Returns the distribution (coarse) grid's partitions per dimension number. */
int RoundRobinPartitioning::getDistributionPPD() {
    return distPartitionsPerDim; 
}

/** @brief Returns the partitioning (fine) grid's partitions per dimension number. */
int RoundRobinPartitioning::getPartitioningPPD() {
    return distPartitionsPerDim; 
}

/** @brief Returns the partitioning (fine) grid's partitions per dimension number. */
int RoundRobinPartitioning::getGlobalPPD() {
    return distPartitionsPerDim; 
}

void RoundRobinPartitioning::setPartGridDataspace(double xMin, double yMin, double xMax, double yMax) {
    distGridDataspaceMetadata.set(xMin, yMin, xMax, yMax);
}

void RoundRobinPartitioning::setPartGridDataspace(DataspaceMetadata &otherDataspaceMetadata) {
    distGridDataspaceMetadata = otherDataspaceMetadata;
}

double RoundRobinPartitioning::getPartPartionExtentX() {
    return distPartitionExtentX;
}

double RoundRobinPartitioning::getPartPartionExtentY() {
    return distPartitionExtentY;
}

namespace shape_factory
{
    // Create empty shapes
    Shape createEmptyPointShape() {
        return Shape(PointWrapper());
    }

    Shape createEmptyPolygonShape() {
        return Shape(PolygonWrapper());
    }

    Shape createEmptyLineStringShape() {
        return Shape(LineStringWrapper());
    }

    Shape createEmptyRectangleShape() {
        return Shape(RectangleWrapper());
    }

    DB_STATUS createEmpty(DataTypeE dataType, Shape &object) {
        switch (dataType) {
            case DT_POINT:
                object = createEmptyPointShape();
                break;
            case DT_LINESTRING:
                object = createEmptyLineStringShape();
                break;
            case DT_RECTANGLE:
                object = createEmptyRectangleShape();
                break;
            case DT_POLYGON:
                object = createEmptyPolygonShape();
                break;
            default:
                logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype in factory method:", dataType);
                return DBERR_INVALID_DATATYPE;
        }
        return DBERR_OK;
    }
}

