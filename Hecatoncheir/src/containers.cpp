#include "containers.h"

Config g_config;

DB_STATUS queryResultReductionFunc(hec::QueryResult &in, hec::QueryResult &out) {
    DB_STATUS ret = DBERR_OK;
    
    out.mergeResults(in);

    return ret;
}

std::vector<std::pair<size_t,size_t>>* hec::QueryResult::getResultPairs() {
    return &this->pairs;
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
    return &classIndex[classType];
}

void Partition::addObjectOfClass(Shape *objectRef, TwoLayerClass classType) {
    classIndex[classType].push_back(objectRef);
    // if (this->partitionID == 289605) {
    //     printf("partition %d class %s new size after insertion of %ld: %ld\n", this->partitionID, mapping::twoLayerClassIntToStr(classType).c_str(), objectRef->recID, classIndex[classType].size());
    // }
    // printf("Adding object of type %s and class %s in partition %ld\n", objectRef->getShapeType().c_str(), mapping::twoLayerClassIntToStr(classType).c_str(), this->partitionID);
}

int DatasetMetadata::calculateBufferSize() {
    int size = 0;
    // persistence
    size += sizeof(bool);
    // dataset index
    size += sizeof(DatasetIndex);
    // dataset data type
    size += sizeof(DataType);
    // dataset file type
    size += sizeof(hec::FileType);
    // dataset path: length + string
    size += sizeof(int) + (path.length() * sizeof(char));  
    // bounds set flag
    size += sizeof(bool);
    // MBR is set (4 values)
    size += 4 * sizeof(double);
    return size;
}

DB_STATUS DatasetMetadata::serialize(char **buffer, int &bufferSize) {
    int position = 0;
    // calculate size
    int bufferSizeRet = calculateBufferSize();
    // allocate space
    (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
    if ((*buffer) == NULL) {
        // malloc failed
        return DBERR_MALLOC_FAILED;
    }
    char* localBuffer = *buffer;

    // add persistence
    memcpy(localBuffer + position, &persist, sizeof(bool));
    position += sizeof(bool);
    // add internal ID
    memcpy(localBuffer + position, &internalID, sizeof(DatasetIndex));
    position += sizeof(DatasetIndex);
    // add datatype
    memcpy(localBuffer + position, &dataType, sizeof(DataType));
    position += sizeof(DataType);
    // add file type
    memcpy(localBuffer + position, &fileType, sizeof(hec::FileType));
    position += sizeof(hec::FileType);
    // add dataset path length + string
    int pathLength = path.length();
    memcpy(localBuffer + position, &pathLength, sizeof(int));
    position += sizeof(int);
    memcpy(localBuffer + position, path.data(), pathLength);
    position += pathLength * sizeof(char);
    // add bounds set flag
    memcpy(localBuffer + position, &dataspaceMetadata.boundsSet, sizeof(bool));
    position += sizeof(bool);
    if (dataspaceMetadata.boundsSet) {
        // add dataset's dataspace MBR
        memcpy(localBuffer + position, &dataspaceMetadata.xMinGlobal, sizeof(double));
        position += sizeof(double);
        memcpy(localBuffer + position, &dataspaceMetadata.yMinGlobal, sizeof(double));
        position += sizeof(double);
        memcpy(localBuffer + position, &dataspaceMetadata.xMaxGlobal, sizeof(double));
        position += sizeof(double);
        memcpy(localBuffer + position, &dataspaceMetadata.yMaxGlobal, sizeof(double));
        position += sizeof(double);
    }
    // set and return
    bufferSize = bufferSizeRet;
    return DBERR_OK;
}

DB_STATUS DatasetMetadata::deserialize(const char *buffer, int bufferSize) {
    int pathLength;
    int position = 0;
    double xMin, yMin, xMax, yMax;

    // bool
    memcpy(&persist, buffer + position, sizeof(bool));
    position += sizeof(bool);
    // dataset index
    memcpy(&internalID, buffer + position, sizeof(DatasetIndex));
    position += sizeof(DatasetIndex);
    // dataset data type
    memcpy(&dataType, buffer + position, sizeof(DataType));
    position += sizeof(DataType);
    // dataset file type
    memcpy(&fileType, buffer + position, sizeof(hec::FileType));
    position += sizeof(hec::FileType);
    // dataset nickname length + string
    memcpy(&pathLength, buffer + position, sizeof(int));
    position += sizeof(int);
    path.assign(buffer + position, pathLength);
    position += pathLength * sizeof(char);
    // bounds set flag
    memcpy(&dataspaceMetadata.boundsSet, buffer + position, sizeof(bool));
    position += sizeof(bool);
    if (dataspaceMetadata.boundsSet) {
        // dataset dataspace MBR
        memcpy(&xMin, buffer + position, sizeof(double));
        position += sizeof(double);
        memcpy(&yMin, buffer + position, sizeof(double));
        position += sizeof(double);
        memcpy(&xMax, buffer + position, sizeof(double));
        position += sizeof(double);
        memcpy(&yMax, buffer + position, sizeof(double));
        position += sizeof(double);
    } else {
        // for validity check issues
        position += 4*sizeof(double);
    }
    // set
    dataspaceMetadata.set(xMin, yMin, xMax, yMax);
    if (position != bufferSize) {
        logger::log_error(DBERR_DESERIALIZE_FAILED, "Dataset Metadata desereialization failed.");
        return DBERR_DESERIALIZE_FAILED;
    }
    return DBERR_OK;
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

    // if (object.recID == 43895) {
    //     logger::log_success("Added object to dataset", this->metadata.datasetName);
    //     // object.printGeometry();
    //     // logger::log_task("MBR:", object.mbr.pMin.x, object.mbr.pMin.y, object.mbr.pMax.x, object.mbr.pMax.y);
    //     AprilData* aprilData = this->getAprilDataBySectionAndObjectID(0, object.recID);
    //     logger::log_task("222. Retrievd april data:", aprilData->numIntervalsALL, aprilData->numIntervalsFULL);
    // }

    return DBERR_OK;
}

int Dataset::calculateBufferSize() {
    // int size = 0;
    // // dataset data type
    // size += sizeof(DataType);
    // // dataset nickname: length + string
    // size += sizeof(int) + (metadata.nickname.length() * sizeof(char));     
    // // dataset's dataspace metadata (MBR)
    // size += 4 * sizeof(double);
    
    // return size;
    return -1;
}

/**
 * serialize dataset metadata (only specific stuff)
 */
DB_STATUS Dataset::serialize(char **buffer, int &bufferSize) {
    // int position = 0;
    // // calculate size
    // int bufferSizeRet = calculateBufferSize();
    // // allocate space
    // char* localBuffer = (char*) malloc(bufferSizeRet * sizeof(char));

    // if (localBuffer == NULL) {
    //     // malloc failed
    //     return DBERR_MALLOC_FAILED;
    // }

    // // add datatype
    // memcpy(localBuffer + position, &metadata.dataType, sizeof(DataType));
    // position += sizeof(DataType);
    // // add dataset nickname length + string
    // int nicknameLength = metadata.nickname.length();
    // memcpy(localBuffer + position, &nicknameLength, sizeof(int));
    // position += sizeof(int);
    // memcpy(localBuffer + position, metadata.nickname.data(), nicknameLength);
    // position += nicknameLength * sizeof(char);
    // // add dataset's dataspace MBR
    // memcpy(localBuffer + position, &metadata.dataspaceMetadata.xMinGlobal, sizeof(double));
    // position += sizeof(double);
    // memcpy(localBuffer + position, &metadata.dataspaceMetadata.yMinGlobal, sizeof(double));
    // position += sizeof(double);
    // memcpy(localBuffer + position, &metadata.dataspaceMetadata.xMaxGlobal, sizeof(double));
    // position += sizeof(double);
    // memcpy(localBuffer + position, &metadata.dataspaceMetadata.yMaxGlobal, sizeof(double));
    // position += sizeof(double);
    // // set and return
    // (*buffer) = localBuffer;
    // bufferSize = bufferSizeRet;
    // return DBERR_OK;
    return DBERR_FEATURE_UNSUPPORTED;
}

DB_STATUS Dataset::deserialize(const char *buffer, int bufferSize) {
    // int nicknameLength;
    // int position = 0;
    // double xMin, yMin, xMax, yMax;
    
    // // dataset data type
    // memcpy(&metadata.dataType, buffer + position, sizeof(DataType));
    // position += sizeof(DataType);
    // // dataset nickname length + string
    // memcpy(&nicknameLength, buffer + position, sizeof(int));
    // position += sizeof(int);
    // metadata.nickname.assign(buffer + position, nicknameLength);
    // position += nicknameLength * sizeof(char);
    // // dataset dataspace MBR
    // memcpy(&xMin, buffer + position, sizeof(double));
    // position += sizeof(double);
    // memcpy(&yMin, buffer + position, sizeof(double));
    // position += sizeof(double);
    // memcpy(&xMax, buffer + position, sizeof(double));
    // position += sizeof(double);
    // memcpy(&yMax, buffer + position, sizeof(double));
    // position += sizeof(double);
    // // set
    // metadata.dataspaceMetadata.set(xMin, yMin, xMax, yMax);

    // if (position == bufferSize) {
    //     // all is well
    //     return DBERR_OK;
    // }
    // else{
    //     return DBERR_DESERIALIZE_FAILED;
    // }
    return DBERR_FEATURE_UNSUPPORTED;
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
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find section ID", sectionID, "in section map of dataset", this->metadata.internalID);
        return DBERR_INVALID_PARAMETER;
    }
    // retrieve object
    auto it = secIT->second.aprilData.find(recID);
    if (it == secIT->second.aprilData.end()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "could not find recID", recID, "in section map of dataset", this->metadata.internalID);
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
    if (recID == 43895) {
        logger::log_task("adding april data to section ID", sectionID);
    }
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
    }
    // section exists
    auto obj = sec->second.aprilData.find(recID);
    if (obj != sec->second.aprilData.end()) {
        // object exists already
        obj->second = aprilData;
    } else {
        // add april data for the object, in section map
        sec->second.aprilData[recID] = aprilData;
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
            printf("(%d,%s),", objects[it].getPartitionID(i), mapping::twoLayerClassIntToStr((TwoLayerClass) objects[it].getPartitionClass(i)).c_str());
        }
        printf("\n");
    }
}

void Dataset::printPartitions() {
    for (auto it : this->twoLayerIndex.partitions) {
        printf("Partition %d\n", it.partitionID);
        for (int c=CLASS_A; c<=CLASS_D; c++) {
            printf("Class %s:\n", mapping::twoLayerClassIntToStr((TwoLayerClass) c).c_str());
            std::vector<Shape*>* container = it.getContainerClassContents((TwoLayerClass) c);
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

Dataset* DatasetOptions::getDatasetByIdx(int index) {
    auto it = datasets.find((DatasetIndex) index);
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
    this->boundsSet = true;
    // printf("-------------------------\n");
    // printf("Dataspace bounds: (%f,%f),(%f,%f)\n", this->xMinGlobal, this->yMinGlobal, this->xMaxGlobal, this->yMaxGlobal);
    // printf("xExtent: %f, yExtent: %f\n", this->xExtent, this->yExtent);
    // printf("Max extent: %f\n", this->maxExtent);
    // printf("-------------------------\n");
}

void DataspaceMetadata::clear() {
    xMinGlobal = std::numeric_limits<int>::max();
    yMinGlobal = std::numeric_limits<int>::max();
    xMaxGlobal = -std::numeric_limits<int>::max();
    yMaxGlobal = -std::numeric_limits<int>::max();  
    xExtent = 0;
    yExtent = 0;
    maxExtent = 0;
    boundsSet = false;
}

void DataspaceMetadata::print() {
    printf("MBR: (%f,%f),(%f,%f)\n", xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal);
    printf("xExtent: %f, yExtent: %f, maxExtent: %f\n", xExtent, yExtent, maxExtent);
}

int DataspaceMetadata::calculateBufferSize() {
    int size = 0;
    size += 4; // dataspace
    size += 3; // extents
    return size;
}

DB_STATUS DataspaceMetadata::serialize(double **buffer, int &bufferSize) {
    // calculate size
    int bufferSizeRet = calculateBufferSize();
    // allocate space
    (*buffer) = (double*) malloc(bufferSizeRet * sizeof(double));
    if ((*buffer) == NULL) {
        // malloc failed
        return DBERR_MALLOC_FAILED;
    }
    double* localBuffer = *buffer;

    // add dataspace
    *reinterpret_cast<double*>(localBuffer) = (double) this->xMinGlobal;
    localBuffer += 1;
    *reinterpret_cast<double*>(localBuffer) = (double) this->yMinGlobal;
    localBuffer += 1;
    *reinterpret_cast<double*>(localBuffer) = (double) this->xMaxGlobal;
    localBuffer += 1;
    *reinterpret_cast<double*>(localBuffer) = (double) this->yMaxGlobal;
    localBuffer += 1;
    // extents
    *reinterpret_cast<double*>(localBuffer) = (double) this->xExtent;
    localBuffer += 1;
    *reinterpret_cast<double*>(localBuffer) = (double) this->yExtent;
    localBuffer += 1;
    *reinterpret_cast<double*>(localBuffer) = (double) this->maxExtent;
    localBuffer += 1;

    // set and return
    bufferSize = bufferSizeRet;
    return DBERR_OK;
}

DB_STATUS DataspaceMetadata::deserialize(const double *buffer, int bufferSize) {
    const double *localBuffer = buffer;
    if (bufferSize != this->calculateBufferSize()) {
        return DBERR_BUFFER_SIZE_MISMATCH;
    }
    // dataspace
    this->xMinGlobal = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;
    this->yMinGlobal = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;
    this->xMaxGlobal = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;
    this->yMaxGlobal = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;
    // extents
    this->xExtent = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;
    this->yExtent = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;
    this->maxExtent = (double) *reinterpret_cast<const double*>(localBuffer);
    localBuffer += 1;

    return DBERR_OK;
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

void TwoLayerIndex::addObject(int partitionID, TwoLayerClass classType, Shape* objectRef) {
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

int DatasetOptions::getNumberOfDatasets() {
    return numberOfDatasets;
}

void DatasetOptions::clear() {
    numberOfDatasets = 0;
    R = nullptr;
    S = nullptr;
    datasets.clear();
    dataspaceMetadata.clear();
}

Dataset* DatasetOptions::getDatasetR() {
    return R;
}

Dataset* DatasetOptions::getDatasetS() {
    return S;
}

DB_STATUS DatasetOptions::getDatasetByIdx(int datasetIndex, Dataset **datasetRef) {
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
DB_STATUS DatasetOptions::addDataset(DatasetIndex datasetIdx, Dataset &dataset) {
    // add to datasets struct (todo, change this to use internal id)
    datasets[datasetIdx] = dataset;
    switch (datasetIdx) {
        case DATASET_R:
            // R is being added
            R = &datasets[datasetIdx];
            break;
        case DATASET_S:
            // S is being added
            S = &datasets[datasetIdx];
            break;
        default:
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid dataset index. Use only DATASET_R or DATASET_S.");
            return DBERR_INVALID_PARAMETER;
    }
    numberOfDatasets++;
    return DBERR_OK;
}

DB_STATUS DatasetOptions::addDataset(Dataset &dataset) {
    if (this->R == nullptr) {
        // set as R
        dataset.metadata.internalID = DATASET_R;
        datasets[DATASET_R] = dataset;
        // R is being added
        R = &datasets[DATASET_R];
        numberOfDatasets++;
        this->updateDataspace();
    } else {
        if (this->S == nullptr) {
            // set as S
            dataset.metadata.internalID = DATASET_S;
            datasets[DATASET_S] = dataset;
            // S is being added
            S = &datasets[DATASET_S];
            numberOfDatasets++;
            this->updateDataspace();
        } else {
            // error, R is set but S is not? 
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid state while adding dataset: R is not set but S is set.");
            return DBERR_INVALID_PARAMETER;
        }
    }
    return DBERR_OK;
}

void DatasetOptions::updateDataspace() {
    // find the bounds that enclose both datasets
    for (auto &it: datasets) {
        dataspaceMetadata.xMinGlobal = std::min(dataspaceMetadata.xMinGlobal, it.second.metadata.dataspaceMetadata.xMinGlobal);
        dataspaceMetadata.yMinGlobal = std::min(dataspaceMetadata.yMinGlobal, it.second.metadata.dataspaceMetadata.yMinGlobal);
        dataspaceMetadata.xMaxGlobal = std::max(dataspaceMetadata.xMaxGlobal, it.second.metadata.dataspaceMetadata.xMaxGlobal);
        dataspaceMetadata.yMaxGlobal = std::max(dataspaceMetadata.yMaxGlobal, it.second.metadata.dataspaceMetadata.yMaxGlobal);
    }
    dataspaceMetadata.xExtent = dataspaceMetadata.xMaxGlobal - dataspaceMetadata.xMinGlobal;
    dataspaceMetadata.yExtent = dataspaceMetadata.yMaxGlobal - dataspaceMetadata.yMinGlobal;
    dataspaceMetadata.maxExtent = std::max(dataspaceMetadata.xExtent, dataspaceMetadata.yExtent);
    // set as both datasets' bounds
    for (auto &it: datasets) {
        it.second.metadata.dataspaceMetadata = dataspaceMetadata;
    }

    // logger::log_success("Updated global dataspace:", g_config.datasetOptions.dataspaceMetadata.xMinGlobal, g_config.datasetOptions.dataspaceMetadata.yMinGlobal, g_config.datasetOptions.dataspaceMetadata.xMaxGlobal, g_config.datasetOptions.dataspaceMetadata.yMaxGlobal);
}

void DatasetOptions::updateDatasetDataspaceToGlobal() {
    for (auto &it: datasets) {
        it.second.metadata.dataspaceMetadata.xMinGlobal = dataspaceMetadata.xMinGlobal;
        it.second.metadata.dataspaceMetadata.yMinGlobal = dataspaceMetadata.yMinGlobal;
        it.second.metadata.dataspaceMetadata.xMaxGlobal = dataspaceMetadata.xMaxGlobal;
        it.second.metadata.dataspaceMetadata.yMaxGlobal = dataspaceMetadata.yMaxGlobal;
        it.second.metadata.dataspaceMetadata.xExtent = dataspaceMetadata.xExtent;
        it.second.metadata.dataspaceMetadata.yExtent = dataspaceMetadata.yExtent;
        it.second.metadata.dataspaceMetadata.maxExtent = dataspaceMetadata.maxExtent;
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
    size += sizeof(DataType);                        // datatype
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
    *reinterpret_cast<DataType*>(localBuffer) = dataType;
    localBuffer += sizeof(DataType);
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
    DataType dataType = *reinterpret_cast<const DataType*>(localBuffer);
    localBuffer += sizeof(DataType);

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

    DB_STATUS createEmpty(DataType dataType, Shape &object) {
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

