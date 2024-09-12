#include "containers.h"

Config g_config;

QueryOutput g_queryOutput;

void queryResultReductionFunc(QueryOutput &in, QueryOutput &out) {
    out.queryResults += in.queryResults;
    out.postMBRFilterCandidates += in.postMBRFilterCandidates;
    out.refinementCandidates += in.refinementCandidates;
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
            out.trueHits += in.trueHits;
            out.trueNegatives += in.trueNegatives;
            break;
        case Q_FIND_RELATION:
            for (auto &it : in.topologyRelationsResultMap) {
                out.topologyRelationsResultMap[it.first] += it.second;
            }
            break;
        default:
            logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query type:", g_config.queryInfo.type);
            break;
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
    for (auto &partitionIT: object.partitions) {
        int partitionID = partitionIT.first;
        TwoLayerClassE classType = (TwoLayerClassE) partitionIT.second;
        // add to twolayer index
        this->twoLayerIndex.addObject(partitionID, classType, objectRef);
        // if (partitionID == 289605) {
        //     printf("added object %ld to partition %d\n", object.recID, partitionID);
        //     printf("Partitions from disk: %ld\n", object.partitions.size());
        // }
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
    // dataset's dataspace info (MBR)
    size += 4 * sizeof(double);
    
    return size;
}

/**
 * serialize dataset info (only specific stuff)
 */
int Dataset::serialize(char **buffer) {
    int position = 0;
    // calculate size
    int bufferSize = calculateBufferSize();
    // allocate space
    char* localBuffer = (char*) malloc(bufferSize * sizeof(char));

    if (localBuffer == NULL) {
        // malloc failed
        return -1;
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
    memcpy(localBuffer + position, &dataspaceInfo.xMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceInfo.yMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceInfo.xMaxGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &dataspaceInfo.yMaxGlobal, sizeof(double));
    position += sizeof(double);
    // set and return
    (*buffer) = localBuffer;
    return bufferSize;
}

void Dataset::deserialize(const char *buffer, int bufferSize) {
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
    dataspaceInfo.set(xMin, yMin, xMax, yMax);

    if (position == bufferSize) {
        // all is well
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

void Dataset::addIntervalsToAprilData(const uint sectionID, const size_t recID, const int numIntervals, const std::vector<uint32_t> &intervals, const bool ALL) {
    // retrieve section
    auto secIT = this->sectionMap.find(sectionID);
    if (secIT == this->sectionMap.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find section ID", sectionID, "in section map of dataset", this->nickname);
        return;
    }
    // retrieve object
    auto it = secIT->second.aprilData.find(recID);
    if (it == secIT->second.aprilData.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find recID", recID, "in section map of dataset", this->nickname);
        return;
    }
    // replace intervals in april data
    if (ALL) {
        it->second.numIntervalsALL = numIntervals;
        it->second.intervalsALL = intervals;
    } else {
        it->second.numIntervalsFULL = numIntervals;
        it->second.intervalsFULL = intervals;
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
        for (auto &partIT : this->objects[it].partitions) {
            printf("(%ld,%s),", partIT.first, mapping::twoLayerClassIntToStr((TwoLayerClassE) partIT.second).c_str());
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

Dataset* DatasetInfo::getDatasetByNickname(std::string &nickname) {
    auto it = datasets.find(nickname);
    if (it == datasets.end()) {
        return nullptr;
    }        
    return &it->second;
}

DataspaceInfo::DataspaceInfo() {
    xMinGlobal = std::numeric_limits<int>::max();
    yMinGlobal = std::numeric_limits<int>::max();
    xMaxGlobal = -std::numeric_limits<int>::max();
    yMaxGlobal = -std::numeric_limits<int>::max();
}

void DataspaceInfo::set(double xMinGlobal, double yMinGlobal, double xMaxGlobal, double yMaxGlobal) {
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

void DataspaceInfo::clear() {
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

int DatasetInfo::getNumberOfDatasets() {
    return numberOfDatasets;
}

void DatasetInfo::clear() {
    numberOfDatasets = 0;
    R = nullptr;
    S = nullptr;
    datasets.clear();
    dataspaceInfo.clear();
}

Dataset* DatasetInfo::getDatasetR() {
    return R;
}

Dataset* DatasetInfo::getDatasetS() {
    return S;
}

Dataset* DatasetInfo::getDatasetByIdx(DatasetIndexE datasetIndex) {
    switch (datasetIndex) {
        case DATASET_R:
            return R;
        case DATASET_S:
            return S;
    }
    logger::log_error(DBERR_INVALID_PARAMETER, "Invalid dataset index:", datasetIndex);
    return nullptr;
}
/**
@brief adds a Dataset to the configuration's dataset info
 * @warning it has to be an empty dataset BUT its nickname needs to be set
 */
DB_STATUS DatasetInfo::addDataset(DatasetIndexE datasetIdx, Dataset &dataset) {
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
            break;
    }
    numberOfDatasets++;
    return DBERR_OK;
}

void DatasetInfo::updateDataspace() {
    // find the bounds that enclose both datasets
    for (auto &it: datasets) {
        dataspaceInfo.xMinGlobal = std::min(dataspaceInfo.xMinGlobal, it.second.dataspaceInfo.xMinGlobal);
        dataspaceInfo.yMinGlobal = std::min(dataspaceInfo.yMinGlobal, it.second.dataspaceInfo.yMinGlobal);
        dataspaceInfo.xMaxGlobal = std::max(dataspaceInfo.xMaxGlobal, it.second.dataspaceInfo.xMaxGlobal);
        dataspaceInfo.yMaxGlobal = std::max(dataspaceInfo.yMaxGlobal, it.second.dataspaceInfo.yMaxGlobal);
    }
    dataspaceInfo.xExtent = dataspaceInfo.xMaxGlobal - dataspaceInfo.xMinGlobal;
    dataspaceInfo.yExtent = dataspaceInfo.yMaxGlobal - dataspaceInfo.yMinGlobal;
    // set as both datasets' bounds
    for (auto &it: datasets) {
        it.second.dataspaceInfo = dataspaceInfo;
    }
}

Geometry::Geometry(size_t recID, int vertexCount, std::vector<double> &coords) {
    this->recID = recID;
    this->vertexCount = vertexCount;
    this->coords = coords;
}

Geometry::Geometry(size_t recID, std::vector<int> &partitions, int vertexCount, std::vector<double> &coords) {
    this->recID = recID;
    this->partitions = partitions;
    this->partitionCount = partitions.size() / 2; 
    this->vertexCount = vertexCount;
    this->coords = coords;
}

void Geometry::setPartitions(std::vector<int> &ids, std::vector<TwoLayerClassE> &classes) {
    for (int i=0; i<ids.size(); i++) {
        partitions.emplace_back(ids.at(i));
        partitions.emplace_back(classes.at(i));
    }
    this->partitionCount = ids.size();
}

void Geometry::setPartitions(std::vector<int> &ids) {
    for (int i=0; i<ids.size(); i++) {
        partitions.emplace_back(ids.at(i));
        partitions.emplace_back(CLASS_NONE);
    }
    this->partitionCount = ids.size();
}

bool GeometryBatch::isValid() {
    return !(destRank == -1 || tag == -1 || comm == nullptr);
}

void GeometryBatch::addGeometryToBatch(Geometry &geometry) {
    geometries.emplace_back(geometry);
    objectCount += 1;
}

void GeometryBatch::setDestNodeRank(int destRank) {
    this->destRank = destRank;
}

// calculate the size needed for the serialization buffer
int GeometryBatch::calculateBufferSize() {
    int size = 0;
    size += sizeof(size_t);                        // objectCount
    
    for (auto &it: geometries) {
        size += sizeof(size_t); // recID
        size += sizeof(int);    // partition count
        size += it.partitions.size() * 2 * sizeof(int); // partitions id + class
        size += sizeof(it.vertexCount); // vertex count
        size += it.vertexCount * 2 * sizeof(double);    // vertices
    }
    
    return size;
}

void GeometryBatch::clear() {
    objectCount = 0;
    geometries.clear();
}

int GeometryBatch::serialize(char **buffer) {
    // calculate size
    int bufferSize = calculateBufferSize();
    // allocate space
    (*buffer) = (char*) malloc(bufferSize * sizeof(char));
    if (*buffer == NULL) {
        // malloc failed
        return -1;
    }
    char* localBuffer = *buffer;

    // add object count
    *reinterpret_cast<size_t*>(localBuffer) = objectCount;
    localBuffer += sizeof(size_t);

    // add batch geometry info
    for (auto &it : geometries) {
        *reinterpret_cast<size_t*>(localBuffer) = it.recID;
        localBuffer += sizeof(size_t);
        *reinterpret_cast<int*>(localBuffer) = it.partitionCount;
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, it.partitions.data(), it.partitionCount * 2 * sizeof(int));
        localBuffer += it.partitionCount * 2 * sizeof(int);
        *reinterpret_cast<int*>(localBuffer) = it.vertexCount;
        localBuffer += sizeof(int);
        std::memcpy(localBuffer, it.coords.data(), it.vertexCount * 2 * sizeof(double));
        localBuffer += it.vertexCount * 2 * sizeof(double);
    }

    return bufferSize;
}

/**
@brief fills the struct with data from the input serialized buffer
 * The caller must free the buffer memory
 * @param buffer 
 */
void GeometryBatch::deserialize(const char *buffer, int bufferSize) {
    size_t recID;
    int vertexCount, partitionCount;
    const char *localBuffer = buffer;

    // get object count
    size_t objectCount = *reinterpret_cast<const size_t*>(localBuffer);
    localBuffer += sizeof(size_t);

    // extend reserve space
    geometries.reserve(geometries.size() + objectCount);

    // deserialize fields for each object in the batch
    for (size_t i=0; i<objectCount; i++) {
        // rec id
        recID = *reinterpret_cast<const size_t*>(localBuffer);
        localBuffer += sizeof(size_t);
        // partition count
        partitionCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // partitions
        std::vector<int> partitions(partitionCount * 2);
        const int* partitionPtr = reinterpret_cast<const int*>(localBuffer);
        for (size_t j = 0; j < partitionCount*2; j++) {
            partitions[j] = partitionPtr[j];
        }
        localBuffer += partitionCount * 2 * sizeof(int);
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
        
        // add to batch
        Geometry geometry(recID, partitions, vertexCount, coords);
        this->addGeometryToBatch(geometry);
    }
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
    partGridDataspaceInfo.set(xMin, yMin, xMax, yMax);
    partPartitionExtentX = partGridDataspaceInfo.xExtent / partPartitionsPerDim;
    partPartitionExtentY = partGridDataspaceInfo.yExtent / partPartitionsPerDim;
}

void TwoGridPartitioning::setPartGridDataspace(DataspaceInfo &otherDataspaceInfo) {
    partGridDataspaceInfo = otherDataspaceInfo;
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
    distGridDataspaceInfo.set(xMin, yMin, xMax, yMax);
}

void RoundRobinPartitioning::setPartGridDataspace(DataspaceInfo &otherDataspaceInfo) {
    distGridDataspaceInfo = otherDataspaceInfo;
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
}

