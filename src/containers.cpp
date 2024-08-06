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
    this->queryResults = 0;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    this->topologyRelationsResultMap.insert(std::make_pair(TR_DISJOINT, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_EQUAL, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_MEET, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_CONTAINS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERED_BY, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INSIDE, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INTERSECT, 0));
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

void QueryOutput::reset() {
    // result
    this->queryResults = 0;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    this->topologyRelationsResultMap.insert(std::make_pair(TR_DISJOINT, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_EQUAL, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_MEET, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_CONTAINS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERS, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_COVERED_BY, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INSIDE, 0));
    this->topologyRelationsResultMap.insert(std::make_pair(TR_INTERSECT, 0));
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

int QueryOutput::getResultForTopologyRelation(TopologyRelation relation) {
    auto it = topologyRelationsResultMap.find(relation);
    if (it != topologyRelationsResultMap.end()) {
        return it->second;
    }
    return -1;
}

void QueryOutput::setTopologyRelationResult(int relation, int result) {
    auto it = topologyRelationsResultMap.find(relation);
    if (it != topologyRelationsResultMap.end()) {
        // exists already, replace
        it->second = result;
    } else {
        // new entry
        topologyRelationsResultMap.insert(std::make_pair(relation, result));
    }
}

void QueryOutput::shallowCopy(QueryOutput &other) {
    // result
    this->queryResults = other.queryResults;
    // topology relations results
    this->topologyRelationsResultMap.clear();
    for (auto &it: other.topologyRelationsResultMap) {
        this->topologyRelationsResultMap.insert(std::make_pair(it.first, it.second));
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

std::vector<Shape*>* Partition::getContainerClassContents(TwoLayerClass classType) {
    if (classType < CLASS_A || classType > CLASS_D) {
        logger::log_error(DBERR_OUT_OF_BOUNDS, "class type index out of bounds");
        return nullptr;
    }
    // exists
    return &classIndex[classType];
}

Shape* Dataset::getObject(size_t recID) {
    auto it = objects.find(recID);
    if (it == objects.end()) {
        return nullptr;
    }
    return &it->second;
}

void Dataset::addObject(Shape &object) {
    // add object to the objects map
    objects.insert(std::make_pair(object.recID, object));
    // get object reference
    Shape* objectRef = this->getObject(object.recID);
    // insert reference to partition index
    for (auto &partitionIT: object.partitions) {
        int partitionID = partitionIT.first;
        TwoLayerClass classType = (TwoLayerClass) partitionIT.second;
        // add to twolayer index
        this->twoLayerIndex.addObject(partitionID, classType, objectRef);
    }
}

int Dataset::calculateBufferSize() {
    int size = 0;
    // dataset data type
    size += sizeof(DataType);
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
    memcpy(localBuffer + position, &dataType, sizeof(DataType));
    position += sizeof(DataType);
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
    memcpy(&dataType, buffer + position, sizeof(DataType));
    position += sizeof(DataType);
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
    this->sectionMap[sectionID].aprilData.insert(std::make_pair(recID, aprilData));
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap.insert(std::make_pair(recID, sectionIDs));
    }
}

void Dataset::addObjectToSectionMap(const uint sectionID, const size_t recID) {
    AprilData aprilData;
    this->sectionMap[sectionID].aprilData.insert(std::make_pair(recID, aprilData));
    // store mapping recID -> sectionID
    auto it = this->recToSectionIdMap.find(recID);
    if (it != this->recToSectionIdMap.end()) {
        // exists
        it->second.emplace_back(sectionID);
    } else {
        // doesnt exist, new entry
        std::vector<uint> sectionIDs = {sectionID};
        this->recToSectionIdMap.insert(std::make_pair(recID, sectionIDs));
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
    // printf("Dataset bounds: (%f,%f),(%f,%f)\n", this->xMinGlobal, this->yMinGlobal, this->xMaxGlobal, this->yMaxGlobal);
    // printf("xExtent: %f, yExtent: %f\n", this->xExtent, this->yExtent);
    // printf("Max extent: %f\n", this->maxExtent);
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
        partitions.emplace_back(partitionID);
        size_t newIndex = partitions.size() - 1;
        partitionMap[partitionID] = newIndex;
        return &partitions[newIndex];
    } else {
        // existing
        return &partitions[it->second];
    }
}

void TwoLayerIndex::addObject(int partitionID, TwoLayerClass classType, Shape* objectRef) {
    // get or create new partition entry
    Partition* partition = getOrCreatePartition(partitionID);
    // get or create new class entry of this class type, for this partition
    std::vector<Shape*>* classObjects = partition->getContainerClassContents(classType);
    // add object
    classObjects->push_back(objectRef);
    // logger::log_success("Added object ref:", objectRef->recID);
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
/**
@brief adds a Dataset to the configuration's dataset info
 * @warning it has to be an empty dataset BUT its nickname needs to be set
 */
void DatasetInfo::addDataset(Dataset &dataset) {
    // add to datasets struct
    datasets.insert(std::make_pair(dataset.nickname, dataset));
    if (numberOfDatasets < 1) {
        // R is being added
        R = &datasets.find(dataset.nickname)->second;
    } else {
        // S is being added
        S = &datasets.find(dataset.nickname)->second;
    }
    numberOfDatasets++;
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

void Geometry::setPartitions(std::vector<int> &ids, std::vector<TwoLayerClass> &classes) {
    for (int i=0; i<ids.size(); i++) {
        partitions.emplace_back(ids.at(i));
        partitions.emplace_back(classes.at(i));
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

