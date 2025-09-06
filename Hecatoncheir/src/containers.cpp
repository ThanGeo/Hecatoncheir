#include "containers.h"

#include "APRIL/generate.h"

#include "TwoLayer/filter.h"
#include "UniformGrid/filter.h"

/* CONFIGURATION */

Config g_config;

void Config::reset() {
    datasetOptions.clear();
    if (partitioningMethod != nullptr) {
        // @todo: find how to delete properly
        delete partitioningMethod;
        partitioningMethod = nullptr; // Avoid dangling pointer
    } 
}

/* MERGE METHODS */

/** @brief  Merges two QResultBase objects. 
 * Both dest and src must contain QResultBase derived objects of THE SAME class.
*/
DB_STATUS mergeResultObjects(hec::QResultBase *out, hec::QResultBase *in) {
    if (out == nullptr || in == nullptr) {
        logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Either in or out pointer is null during query result reduction.");
        return DBERR_NULL_PTR_EXCEPTION;
    }

    try {
        out->mergeResults(in);
        return DBERR_OK;
    } catch (const std::bad_cast& e) {
        logger::log_error(DBERR_TYPE_CAST_MISMATCH, "Type mismatch during result merging: " + std::string(e.what()));
        return DBERR_TYPE_CAST_MISMATCH;
    } catch (...) {
        logger::log_error(DBERR_REDUCTION_MERGE_FAILURE, "Unknown error during result merging");
        return DBERR_REDUCTION_MERGE_FAILURE;
    }

}

/** @brief Merges two batch query result maps into a single one.
 * Both dest and src must contain QResultBase derived objects of THE SAME class.
 */
DB_STATUS mergeBatchResultMaps(std::unordered_map<int, hec::QResultBase*> &dest, std::unordered_map<int, hec::QResultBase*>& src) {
    DB_STATUS ret = DBERR_OK;
    for (auto& [key, value] : src) {
        auto it = dest.find(key);
        if (it == dest.end()) {
            dest[key] = value;
            if (key == 0) {
            }
        } else {
            dest[key]->mergeResults(value);
            delete value;
        }
    }
    return ret;
}

/* APRIL DATA */

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

std::vector<Shape*>* PartitionTwoLayer::getContents(TwoLayerClass classType) {
    return &classIndex[classType];
}

void PartitionTwoLayer::addObject(Shape *objectRef, TwoLayerClass classType) {
    classIndex[classType].push_back(objectRef);
}

std::vector<Shape*>* PartitionUniformGrid::getContents(TwoLayerClass classType) {
    return &classIndex;
}

void PartitionUniformGrid::addObject(Shape *objectRef, TwoLayerClass classType) {
    classIndex.push_back(objectRef);
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
        // set
        dataspaceMetadata.set(xMin, yMin, xMax, yMax);
        if (position != bufferSize) {
            logger::log_error(DBERR_DESERIALIZE_FAILED, "Dataset Metadata desereialization failed.");
            return DBERR_DESERIALIZE_FAILED;
        }
    } else {
        // for validity check issues
        position += 4*sizeof(double);
    }
    return DBERR_OK;
}


Shape* Dataset::getObject(size_t recID) {
    return &objects[objectPosMap[recID]];
}

DB_STATUS Dataset::storeObject(Shape &object) {
    // add object to the objects and map its pos
    objectPosMap[object.recID] = objects.size();
    objects.push_back(object);

    return DBERR_OK;
}

DB_STATUS Dataset::buildIndex(hec::IndexType indexType) {
    DB_STATUS ret = DBERR_OK;
    switch (indexType) {
        case hec::IT_TWO_LAYER:
            /** non-point geometries */
            this->index = std::make_unique<TwoLayerIndex>();
            // add objects to index
            for(auto &object : this->objects) {
                ret = index->addObject(&object);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to add object", object.recID, "to index.");
                    return ret;
                }
            }
            // sort
            this->index->sortPartitionsOnY();
            break;
        case hec::IT_UNIFORM_GRID:
            /** point geometries - no april */
            this->index = std::make_unique<UniformGridIndex>();
            // add objects to index
            for(auto &object : this->objects) {
                ret = index->addObject(&object);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to add object", object.recID, "to index.");
                    return ret;
                }
            }
            break;
        default:
            logger::log_error(DBERR_INVALID_INDEX_TYPE, "Invalid index type for build index.");
            return DBERR_INVALID_INDEX_TYPE;
    }
    return ret;
}

DB_STATUS Dataset::buildAPRIL() {
    DB_STATUS ret = DBERR_OK;
    if (this->metadata.dataType == DT_POINT) {
        // logger::log_warning("APRIL does not support points.");
        return DBERR_OK;
    }
    // logger::log_task("Building APRIL:");

    // init rasterization environment
    ret = APRIL::generation::setRasterBounds(this->metadata.dataspaceMetadata);
    if (ret != DBERR_OK) {
        return ret;
    }

    #pragma omp parallel num_threads(MAX_THREADS)
    {
        DB_STATUS local_ret = DBERR_OK;
        #pragma omp for
        for (int i=0; i<this->objects.size(); i++) {
            // generate april
            local_ret = APRIL::generation::memory::createAPRILforObject(&this->objects[i], this->metadata.dataType, this->aprilConfig, this->objects[i].aprilData);
            if (local_ret != DBERR_OK) {
                #pragma omp cancel for
                logger::log_error(ret, "Failed to create APRIL for object", this->objects[i].recID);
            }
        }

        if (local_ret != DBERR_OK) {
            #pragma omp cancel parallel
            ret = local_ret;
        }
    }

    return ret;
}

int Dataset::calculateBufferSize() {
    int size = 0;
    // dataset index
    size += sizeof(DatasetIndex);
    // total objects
    size += sizeof(size_t);
    // dataset's dataspace metadata (MBR)
    size += 4 * sizeof(double);
    
    // return size;
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
    (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
    if ((*buffer) == NULL) {
        // malloc failed
        return DBERR_MALLOC_FAILED;
    }
    char* localBuffer = *buffer;
    
    // add index
    memcpy(localBuffer + position, &metadata.internalID, sizeof(DatasetIndex));
    position += sizeof(DatasetIndex);
    // add total objects
    memcpy(localBuffer + position, &totalObjects, sizeof(size_t));
    position += sizeof(size_t);
    // add dataset's dataspace MBR
    memcpy(localBuffer + position, &metadata.dataspaceMetadata.xMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &metadata.dataspaceMetadata.yMinGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &metadata.dataspaceMetadata.xMaxGlobal, sizeof(double));
    position += sizeof(double);
    memcpy(localBuffer + position, &metadata.dataspaceMetadata.yMaxGlobal, sizeof(double));
    position += sizeof(double);
    // set and return
    bufferSize = bufferSizeRet;
    return DBERR_OK;
}

DB_STATUS Dataset::deserialize(const char *buffer, int bufferSize) {
    // int nicknameLength;
    int position = 0;
    double xMin, yMin, xMax, yMax;
    // dataset index
    memcpy(&metadata.internalID, buffer + position, sizeof(DatasetIndex));
    position += sizeof(DatasetIndex);
    // dataset data type
    memcpy(&totalObjects, buffer + position, sizeof(size_t));
    position += sizeof(size_t);
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
    metadata.dataspaceMetadata.set(xMin, yMin, xMax, yMax);

    if (position == bufferSize) {
        // all is well
        return DBERR_OK;
    }
    else{
        return DBERR_DESERIALIZE_FAILED;
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
        it->second.intervalsALL = intervals;
    } else {
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

void Dataset::clear() {
    // free index memory
    index.reset();
    // delete objects
    this->objects.clear();
    this->objectPosMap.clear();
    this->sectionMap.clear();
    this->recToSectionIdMap.clear();
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

/** DISTANCE JOIN BATCH */

void DJBatch::addObjectR(Shape& obj) {
    this->objectsR[obj.recID] = obj;
}

void DJBatch::addObjectS(Shape& obj) {
    this->objectsS[obj.recID] = obj;
}   

int DJBatch::calculateBufferSize() {
    int size = 0;
    size += sizeof(int);                                        // destination rank

    size += sizeof(DataType);                                     // datatypeR
    size += sizeof(size_t);                                     // objectsR count
    for (auto &it: this->objectsR) {
        size += sizeof(size_t);                                 // recID
        size += sizeof(DataType);                               // data type
        size += 4 * sizeof(double);                             // mbr
        size += sizeof(it.second.getVertexCount());                    // vertex count
        size += it.second.getVertexCount() * 2 * sizeof(double);       // vertices
    }
    
    size += sizeof(DataType);                                     // datatypeS
    size += sizeof(size_t);                                     // objectsS count
    for (auto &it: this->objectsS) {
        size += sizeof(size_t);                                 // recID
        size += sizeof(DataType);                               // data type
        size += 4 * sizeof(double);                             // mbr
        size += sizeof(it.second.getVertexCount());                    // vertex count
        size += it.second.getVertexCount() * 2 * sizeof(double);       // vertices
    }
    
    return size;
}

DB_STATUS DJBatch::serialize(char **buffer, int &bufferSize) {
    DB_STATUS ret = DBERR_OK;
    // calculate size
    int bufferSizeRet = calculateBufferSize();
    // allocate space
    (*buffer) = (char*) malloc(bufferSizeRet * sizeof(char));
    if (*buffer == NULL) {
        // malloc failed
        return DBERR_MALLOC_FAILED;
    }
    char* localBuffer = *buffer;

    // add dest rank
    *reinterpret_cast<int*>(localBuffer) = this->destRank;
    localBuffer += sizeof(int);
    
    // add datatype R
    *reinterpret_cast<DataType*>(localBuffer) = this->dataTypeR;
    localBuffer += sizeof(DataType);

    // add objects R count
    size_t totalObjectsR = this->objectsR.size();
    *reinterpret_cast<size_t*>(localBuffer) = totalObjectsR;
    localBuffer += sizeof(size_t);

    // add batch geometry metadata
    for (auto &it : this->objectsR) {
        // ID
        *reinterpret_cast<size_t*>(localBuffer) = it.second.recID;
        localBuffer += sizeof(size_t);
        // type
        *reinterpret_cast<DataType*>(localBuffer) = it.second.type;
        localBuffer += sizeof(DataType);
        // mbr
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMin.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMin.y;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMax.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMax.y;
        localBuffer += sizeof(double);
        // vertex count
        *reinterpret_cast<int*>(localBuffer) = it.second.getVertexCount();
        localBuffer += sizeof(int);
        // serialize and copy coordinates to buffer
        double* bufferPtr;
        int bufferElementCount;
        ret = it.second.serializeCoordinates(&bufferPtr, bufferElementCount);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Serializing coordinates failed.");
            free(*buffer);
            *buffer = nullptr;
            return ret;
        }
        // logger::log_task("Adding object to buffer with", bufferElementCount, "elements");
        std::memcpy(localBuffer, bufferPtr, bufferElementCount * sizeof(double));
        localBuffer += bufferElementCount * sizeof(double);
        // free 
        free(bufferPtr);
    }

    // add datatype S
    *reinterpret_cast<DataType*>(localBuffer) = this->dataTypeS;
    localBuffer += sizeof(DataType);

    // add objects S count
    size_t totalObjectsS = this->objectsS.size();
    *reinterpret_cast<size_t*>(localBuffer) = totalObjectsS;
    localBuffer += sizeof(size_t);

    // add batch geometry metadata
    for (auto &it : this->objectsS) {
        // id
        *reinterpret_cast<size_t*>(localBuffer) = it.second.recID;
        localBuffer += sizeof(size_t);
        // type
        *reinterpret_cast<DataType*>(localBuffer) = it.second.type;
        localBuffer += sizeof(DataType);
        // mbr
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMin.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMin.y;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMax.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.second.mbr.pMax.y;
        localBuffer += sizeof(double);
        // vertex count
        *reinterpret_cast<int*>(localBuffer) = it.second.getVertexCount();
        localBuffer += sizeof(int);
        // serialize and copy coordinates to buffer
        double* bufferPtr;
        int bufferElementCount;
        ret = it.second.serializeCoordinates(&bufferPtr, bufferElementCount);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Serializing coordinates failed.");
            free(*buffer);
            *buffer = nullptr;
            return ret;
        }
        // logger::log_task("Adding object to buffer with", bufferElementCount, "elements");
        std::memcpy(localBuffer, bufferPtr, bufferElementCount * sizeof(double));
        localBuffer += bufferElementCount * sizeof(double);
        // free 
        free(bufferPtr);
    }
    bufferSize = bufferSizeRet;
    return ret;
}

DB_STATUS DJBatch::deserialize(const char *buffer, int bufferSize) {
    DB_STATUS ret = DBERR_OK;
    int vertexCount;
    const char *localBuffer = buffer;

    // get destination rank
    this->destRank = *reinterpret_cast<const int*>(localBuffer);
    localBuffer += sizeof(int);
    
    // get datatype R
    this->dataTypeR = *reinterpret_cast<const DataType*>(localBuffer);
    localBuffer += sizeof(DataType);

    // get object count
    size_t objectCountR = *reinterpret_cast<const size_t*>(localBuffer);
    localBuffer += sizeof(size_t);
    // reserve space
    this->objectsR.reserve(objectCountR);

    // deserialize fields for each object in the batch
    for (size_t i=0; i<objectCountR; i++) {
        // rec id
        size_t recID = *reinterpret_cast<const size_t*>(localBuffer);
        localBuffer += sizeof(size_t);
        // data type
        DataType type = *reinterpret_cast<const DataType*>(localBuffer);
        localBuffer += sizeof(DataType);
        // empty shape object
        Shape object;
        ret = shape_factory::createEmpty(type, object);
        if (ret != DBERR_OK) {
            return ret;
        }
        object.recID = recID;
        // mbr
        double xMin, yMin, xMax, yMax;
        xMin = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMin = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        xMax = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMax = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        object.setMBR(xMin, yMin, xMax, yMax);
        // vertex count
        vertexCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // vertices
        // std::vector<double> coords(vertexCount * 2);
        const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
        for (size_t j = 0; j < vertexCount * 2; j+=2) {
            object.addPoint(vertexDataPtr[j], vertexDataPtr[j+1]);
        }
        localBuffer += vertexCount * 2 * sizeof(double);
        // add to batch
        this->addObjectR(object);
        // for safety
        object.reset();
    }

    // get datatype S
    this->dataTypeS = *reinterpret_cast<const DataType*>(localBuffer);
    localBuffer += sizeof(DataType);

    // get object count
    size_t objectCountS = *reinterpret_cast<const size_t*>(localBuffer);
    localBuffer += sizeof(size_t);
    // reserve space
    this->objectsS.reserve(objectCountS);

    // deserialize fields for each object in the batch
    for (size_t i=0; i<objectCountS; i++) {
        // rec id
        size_t recID = *reinterpret_cast<const size_t*>(localBuffer);
        localBuffer += sizeof(size_t);
        // data type
        DataType type = *reinterpret_cast<const DataType*>(localBuffer);
        localBuffer += sizeof(DataType);
        // empty shape object
        Shape object;
        ret = shape_factory::createEmpty(type, object);
        if (ret != DBERR_OK) {
            return ret;
        }
        object.recID = recID;
        // mbr
        double xMin, yMin, xMax, yMax;
        xMin = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMin = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        xMax = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMax = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        object.setMBR(xMin, yMin, xMax, yMax);
        // vertex count
        vertexCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // vertices
        // std::vector<double> coords(vertexCount * 2);
        const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
        for (size_t j = 0; j < vertexCount * 2; j+=2) {
            object.addPoint(vertexDataPtr[j], vertexDataPtr[j+1]);
        }
        localBuffer += vertexCount * 2 * sizeof(double);

        // add to batch
        this->addObjectS(object);
        // for safety
        object.reset();
    }

    return ret;
}

DB_STATUS DJBatch::probeSerializedForDestRank(const char *buffer, int bufferSize, int &destRank) {
    if (buffer == nullptr) {
        logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Null buffer during destRank probing.");
        return DBERR_NULL_PTR_EXCEPTION;
    }

    if (bufferSize < static_cast<int>(sizeof(int))) {
        logger::log_error(DBERR_BUFFER_SIZE_MISMATCH, "Buffer does not contain enough bytes for the destRank probing.");
        return DBERR_BUFFER_SIZE_MISMATCH;
    }

    std::memcpy(&destRank, buffer, sizeof(int));

    return DBERR_OK;
}


/** BASE INDEX */

std::vector<PartitionBase*>* BaseIndex::getPartitions() {
    return &partitions;
}

PartitionBase* BaseIndex::getPartition(int partitionID) {
    auto it = partitionMap.find(partitionID);
    if (it != partitionMap.end()) {
        return partitions[it->second];
    }
    return nullptr;
}

PartitionBase* BaseIndex::getPartition(double x, double y, DataspaceMetadata* dataspaceMetadata, PartitioningMethod* partitioningMethod) {
    int fineMinX = std::floor((x - dataspaceMetadata->xMinGlobal) / partitioningMethod->getPartPartionExtentX());
    int fineMinY = std::floor((y - dataspaceMetadata->yMinGlobal) / partitioningMethod->getPartPartionExtentY());
    int partitionID = partitioningMethod->getPartitionID(fineMinX, fineMinY, partitioningMethod->getGlobalPPD());
    auto it = partitionMap.find(partitionID);
    if (it != partitionMap.end()) {
        return partitions[it->second];
    }
    return nullptr;
}

DB_STATUS BaseIndex::clear() {
    if (partitions.size() > 0) {
        for (auto& it : partitions) {
            delete it;
            it = nullptr; // optional, for safety
        }
        partitions.clear();
    }
    partitionMap.clear();
    return DBERR_OK;
}

BaseIndex::~BaseIndex() {
    this->clear();
}


/** TWO LAYER INDEX */

DB_STATUS TwoLayerIndex::getPartitionsForMBR(Shape* objectRef, std::vector<int> &partitionIDs, std::vector<TwoLayerClass> &partionClasses){
    DataspaceMetadata* dataspace = &g_config.datasetOptions.dataspaceMetadata;
    PartitioningMethod* partitioning = g_config.partitioningMethod;
    const int fineCellsPerCoarseCell = partitioning->getGlobalPPD() / partitioning->getDistributionPPD();
    
    // Calculate bounds of the MBR in fine grid coordinates
    int fineMinX = std::floor((objectRef->mbr.pMin.x - dataspace->xMinGlobal) / partitioning->getPartPartionExtentX());
    int fineMinY = std::floor((objectRef->mbr.pMin.y - dataspace->yMinGlobal) / partitioning->getPartPartionExtentY());
    int fineMaxX = std::floor((objectRef->mbr.pMax.x - dataspace->xMinGlobal) / partitioning->getPartPartionExtentX());
    int fineMaxY = std::floor((objectRef->mbr.pMax.y - dataspace->yMinGlobal) / partitioning->getPartPartionExtentY());
    // Ensure the MBR is within bounds of the fine grid
    if (fineMinX < 0 || fineMinY < 0 || fineMaxX >= partitioning->getGlobalPPD() || fineMaxY >= partitioning->getGlobalPPD()) {
        logger::log_error(DBERR_OUT_OF_BOUNDS, "Fine grid indices out of bounds:", fineMinX, fineMinY, fineMaxX, fineMaxY);
        if (objectRef == nullptr) {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Null Object reference:", objectRef);
        } else {
            logger::log_error(DBERR_OUT_OF_BOUNDS, "Object", objectRef->recID, "MBR:", objectRef->mbr.pMin.x, objectRef->mbr.pMin.y, objectRef->mbr.pMax.x, objectRef->mbr.pMax.y);
        }
        return DBERR_OUT_OF_BOUNDS;
    }
    // Calculate the coarse grid cell bounds
    int coarseMinX = std::floor((objectRef->mbr.pMin.x - dataspace->xMinGlobal) / partitioning->getDistPartionExtentX());
    int coarseMinY = std::floor((objectRef->mbr.pMin.y - dataspace->yMinGlobal) / partitioning->getDistPartionExtentY());
    int coarseMaxX = std::floor((objectRef->mbr.pMax.x - dataspace->xMinGlobal) / partitioning->getDistPartionExtentX());
    int coarseMaxY = std::floor((objectRef->mbr.pMax.y - dataspace->yMinGlobal) / partitioning->getDistPartionExtentY());

    // Consolidated bounds check for the coarse grid
    if (coarseMinX < 0 || coarseMinY < 0 || coarseMaxX >= partitioning->getDistributionPPD() || coarseMaxY >= partitioning->getDistributionPPD()) {
        logger::log_error(DBERR_OUT_OF_BOUNDS, "Coarse grid indices out of bounds:", coarseMinX, coarseMinY, coarseMaxX, coarseMaxY);
        return DBERR_OUT_OF_BOUNDS;
    }

    int originalMinX = std::max(fineMinX, coarseMinX * fineCellsPerCoarseCell);
    int originalMinY = std::max(fineMinY, coarseMinY * fineCellsPerCoarseCell);
    // if (objectRef->recID == 101911 || objectRef->recID == 1691538) {
    //     logger::log_task("original x,y:", originalMinX, originalMinY);
    // }

    // if (objectRef->recID == 99943 || objectRef->recID == 1661475) {
    //     logger::log_task("Object", objectRef->recID, "with MBR: ", objectRef->mbr.pMin.x, objectRef->mbr.pMin.y, objectRef->mbr.pMax.x, objectRef->mbr.pMax.y);
    //     logger::log_task(" range in fine grid", fineMinX, fineMinY, fineMaxX, fineMaxY);
    // }


    // Iterate over the coarse grid cells overlapping the MBR
    for (int coarseX = coarseMinX; coarseX <= coarseMaxX; ++coarseX) {
        for (int coarseY = coarseMinY; coarseY <= coarseMaxY; ++coarseY) {
            // Get the partition ID and node rank
            int distPartitionID = partitioning->getPartitionID(coarseX, coarseY, partitioning->getDistributionPPD());
            int assignedNodeRank = partitioning->getNodeRankForPartitionID(distPartitionID);
            // if (objectRef->recID == 101911 || objectRef->recID == 1691538) {
            //     logger::log_task("coarse x,y:", coarseX, coarseY, "id:", distPartitionID, "belongs to node", assignedNodeRank, "/", g_world_size);
            // }
            // If this rank is responsible for the coarse grid cell
            if (assignedNodeRank == g_parent_original_rank) {
                // Calculate the bounds of this coarse grid cell in fine grid coordinates
                int coarseFineMinX = coarseX * fineCellsPerCoarseCell;
                int coarseFineMinY = coarseY * fineCellsPerCoarseCell;
                int coarseFineMaxX = (coarseX + 1) * fineCellsPerCoarseCell - 1;
                int coarseFineMaxY = (coarseY + 1) * fineCellsPerCoarseCell - 1;

                int startMinX = std::max(fineMinX, coarseFineMinX);
                int startMinY = std::max(fineMinY, coarseFineMinY);
                int endMinX = std::min(fineMaxX, coarseFineMaxX);
                int endMinY = std::min(fineMaxY, coarseFineMaxY);
                // if (objectRef->recID == 101911 || objectRef->recID == 1691538) {
                //     logger::log_task("Coarse grid mins:", coarseMinX, coarseMinY);
                //     logger::log_task("Fine grid start min/max:", startMinX, startMinY, endMinX, endMinY);
                // }

                // Iterate over overlapping fine grid cells within the coarse grid bounds
                for (int fineX = startMinX; fineX <= endMinX; fineX++) {
                    for (int fineY = startMinY; fineY <= endMinY; fineY++) {
                        // if (objectRef->recID == 99943 || objectRef->recID == 1661475) {
                        //     logger::log_task("indexes:", fineX, fineY, "id:", partitioning->getPartitionID(fineX, fineY, partitioning->getGlobalPPD()));
                        // }

                        // Store the fine grid cell
                        int partitionID = partitioning->getPartitionID(fineX, fineY, partitioning->getGlobalPPD());
                        partitionIDs.emplace_back(partitionID);
                        if (fineX == originalMinX && fineY == originalMinY) {
                            // Class A
                            partionClasses.push_back(CLASS_A);
                            // if (objectRef->recID == 99943 || objectRef->recID == 1661475) {
                            //     logger::log_task("assigned node rank:", assignedNodeRank, "Added object ", objectRef->recID, "to partition", partitionID, "with class", CLASS_A);
                            // }
                        } else if (fineX == originalMinX && fineY != originalMinY) {
                            // Class B
                            partionClasses.push_back(CLASS_B);
                            // if (objectRef->recID == 99943 || objectRef->recID == 1661475) {
                            //     logger::log_task("assigned node rank:", assignedNodeRank, "Added object ", objectRef->recID, "to partition", partitionID, "with class", CLASS_B);
                            // }
                        } else if (fineX != originalMinX && fineY == originalMinY) {
                            // Class C
                            partionClasses.push_back(CLASS_C);
                            // if (objectRef->recID == 99943 || objectRef->recID == 1661475) {
                            //     logger::log_task("assigned node rank:", assignedNodeRank, "Added object ", objectRef->recID, "to partition", partitionID, "with class", CLASS_C);
                            // }
                        }  else {
                            // Class D
                            partionClasses.push_back(CLASS_D);
                            // if (objectRef->recID == 99943 || objectRef->recID == 1661475) {
                            //     logger::log_task("assigned node rank:", assignedNodeRank, "Added object ", objectRef->recID, "to partition", partitionID, "with class", CLASS_D);
                            // }
                        }
                    }
                }
            }
        }
    }
    return DBERR_OK;
}

PartitionBase* TwoLayerIndex::getOrCreatePartition(int partitionID) {
    auto it = partitionMap.find(partitionID);
    if (it != partitionMap.end()) {
        // exists
        return partitions[it->second];
    }
    // create new partition
    partitionMap[partitionID] = partitions.size();
    partitions.push_back(new PartitionTwoLayer(partitionID));
    return partitions[partitionMap[partitionID]];
}


DB_STATUS TwoLayerIndex::addObject(Shape *objectRef) {
    // find partitions and clases
    std::vector<int> partitionIDs;
    std::vector<TwoLayerClass> partitionClasses;
    DB_STATUS ret = getPartitionsForMBR(objectRef, partitionIDs, partitionClasses);
    if (ret != DBERR_OK) {
        return ret;
    }

    if (partitionIDs.size() != partitionClasses.size()) {
        logger::log_error(DBERR_INVALID_PARAMETER, "Partition IDs and classes should match in number.");
        return DBERR_INVALID_PARAMETER;
    }

    for (int i=0; i<partitionIDs.size(); i++) {
        PartitionBase* partition = this->getOrCreatePartition(partitionIDs[i]);
        partition->addObject(objectRef, partitionClasses[i]);
    }
    
    return ret;
}

void TwoLayerIndex::sortPartitionsOnY() {
    std::vector<PartitionBase*>* partitions = this->getPartitions();
    for(int i=0; i<partitions->size(); i++) {
        // sort A
        std::vector<Shape*>* contentsA = partitions->at(i)->getContents(CLASS_A);
        std::sort(contentsA->begin(), contentsA->end(), compareByY);

        // sort C
        std::vector<Shape*>* contentsC = partitions->at(i)->getContents(CLASS_C);
        std::sort(contentsC->begin(), contentsC->end(), compareByY);
    }
}

DB_STATUS TwoLayerIndex::evaluateQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult) {
    DB_STATUS ret = twolayer::processQuery(query, queryResult);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to process query with id:", query->getQueryID());
        return ret;
    }
    return ret;
}

/**
 * UNIFORM GRID INDEX
 */

PartitionBase* UniformGridIndex::getOrCreatePartition(int partitionID) {
    auto it = partitionMap.find(partitionID);
    if (it != partitionMap.end()) {
        // exists
        return partitions[it->second];
    }
    // create new partition
    partitionMap[partitionID] = partitions.size();
    partitions.push_back(new PartitionUniformGrid(partitionID));
    return partitions[partitionMap[partitionID]];
}

DB_STATUS UniformGridIndex::getPartitionsForMBR(Shape* objectRef, std::vector<int> &partitionIDs){
    DataspaceMetadata* dataspace = &g_config.datasetOptions.dataspaceMetadata;
    PartitioningMethod* partitioning = g_config.partitioningMethod;
    const int fineCellsPerCoarseCell = partitioning->getGlobalPPD() / partitioning->getDistributionPPD();
    
    // Calculate bounds of the MBR in fine grid coordinates
    int fineMinX = std::floor((objectRef->mbr.pMin.x - dataspace->xMinGlobal) / partitioning->getPartPartionExtentX());
    int fineMinY = std::floor((objectRef->mbr.pMin.y - dataspace->yMinGlobal) / partitioning->getPartPartionExtentY());
    int fineMaxX = std::floor((objectRef->mbr.pMax.x - dataspace->xMinGlobal) / partitioning->getPartPartionExtentX());
    int fineMaxY = std::floor((objectRef->mbr.pMax.y - dataspace->yMinGlobal) / partitioning->getPartPartionExtentY());
    // Ensure the MBR is within bounds of the fine grid
    if (fineMinX < 0 || fineMinY < 0 || fineMaxX >= partitioning->getGlobalPPD() || fineMaxY >= partitioning->getGlobalPPD()) {
        logger::log_error(DBERR_OUT_OF_BOUNDS, "Fine grid indices out of bounds:", fineMinX, fineMinY, fineMaxX, fineMaxY);
        return DBERR_OUT_OF_BOUNDS;
    }
    // Calculate the coarse grid cell bounds
    int coarseMinX = std::floor((objectRef->mbr.pMin.x - dataspace->xMinGlobal) / partitioning->getDistPartionExtentX());
    int coarseMinY = std::floor((objectRef->mbr.pMin.y - dataspace->yMinGlobal) / partitioning->getDistPartionExtentY());
    int coarseMaxX = std::floor((objectRef->mbr.pMax.x - dataspace->xMinGlobal) / partitioning->getDistPartionExtentX());
    int coarseMaxY = std::floor((objectRef->mbr.pMax.y - dataspace->yMinGlobal) / partitioning->getDistPartionExtentY());

    // Consolidated bounds check for the coarse grid
    if (coarseMinX < 0 || coarseMinY < 0 || coarseMaxX >= partitioning->getDistributionPPD() || coarseMaxY >= partitioning->getDistributionPPD()) {
        logger::log_error(DBERR_OUT_OF_BOUNDS, "Coarse grid indices out of bounds:", coarseMinX, coarseMinY, coarseMaxX, coarseMaxY);
        return DBERR_OUT_OF_BOUNDS;
    }

    // Iterate over the coarse grid cells overlapping the MBR
    for (int coarseX = coarseMinX; coarseX <= coarseMaxX; ++coarseX) {
        for (int coarseY = coarseMinY; coarseY <= coarseMaxY; ++coarseY) {
            // Get the partition ID and node rank
            int distPartitionID = partitioning->getPartitionID(coarseX, coarseY, partitioning->getDistributionPPD());
            int assignedNodeRank = partitioning->getNodeRankForPartitionID(distPartitionID);
            // If this rank is responsible for the coarse grid cell
            if (assignedNodeRank == g_parent_original_rank) {
                // Calculate the bounds of this coarse grid cell in fine grid coordinates
                int coarseFineMinX = coarseX * fineCellsPerCoarseCell;
                int coarseFineMinY = coarseY * fineCellsPerCoarseCell;
                int coarseFineMaxX = (coarseX + 1) * fineCellsPerCoarseCell - 1;
                int coarseFineMaxY = (coarseY + 1) * fineCellsPerCoarseCell - 1;

                int startMinX = std::max(fineMinX, coarseFineMinX);
                int startMinY = std::max(fineMinY, coarseFineMinY);
                int endMinX = std::min(fineMaxX, coarseFineMaxX);
                int endMinY = std::min(fineMaxY, coarseFineMaxY);

                // Iterate over overlapping fine grid cells within the coarse grid bounds
                for (int fineX = startMinX; fineX <= endMinX; fineX++) {
                    for (int fineY = startMinY; fineY <= endMinY; fineY++) {
                        // Store the fine grid cell
                        int partitionID = partitioning->getPartitionID(fineX, fineY, partitioning->getGlobalPPD());
                        partitionIDs.emplace_back(partitionID);
                    }
                }
            }
        }
    }
    return DBERR_OK;
}

DB_STATUS UniformGridIndex::addObject(Shape *objectRef) {
    DB_STATUS ret = DBERR_OK;
    
    // find partitions and clases
    std::vector<int> partitionIDs;
    ret = getPartitionsForMBR(objectRef, partitionIDs);
    if (ret != DBERR_OK) {
        return ret;
    }

    for (int i=0; i<partitionIDs.size(); i++) {
        PartitionBase* partition = this->getOrCreatePartition(partitionIDs[i]);
        partition->addObject(objectRef);
    }

    return ret;

}

void UniformGridIndex::sortPartitionsOnY() {
    std::vector<PartitionBase*>* partitions = this->getPartitions();
    for(int i=0; i<partitions->size(); i++) {
        std::vector<Shape*>* contents = partitions->at(i)->getContents();
        std::sort(contents->begin(), contents->end(), compareByY);
    }
}

DB_STATUS UniformGridIndex::evaluateQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult) {
    DB_STATUS ret = uniform_grid::processQuery(query, queryResult);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to process query with id:", query->getQueryID());
        return ret;
    }
    return ret;
}

DB_STATUS UniformGridIndex::evaluateQuery(hec::Query* query, std::unordered_map<int, DJBatch> &borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult) {
    DB_STATUS ret = uniform_grid::processQuery(query, borderObjectsMap, queryResult);
    if (ret != DBERR_OK) {
        logger::log_error(ret, "Failed to process query with id:", query->getQueryID());
        return ret;
    }
    return ret;
}

DB_STATUS UniformGridIndex::evaluateDJBatch(hec::Query* query, DJBatch& batch, std::unique_ptr<hec::QResultBase>& queryResult) {
    DB_STATUS ret = DBERR_OK;
    // switch based on query type
    switch (query->getQueryType()) {
        case hec::Q_DISTANCE_JOIN:
            {
                // cast
                hec::DistanceJoinQuery* distanceQuery = dynamic_cast<hec::DistanceJoinQuery*>(query);
                // evaluate
                ret = uniform_grid::distance_filter::evaluateDJBatch(distanceQuery, batch, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to process query with id:", distanceQuery->getQueryID());
                    return ret;
                }
            }
            break;
        default:
            logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type must be distance join for this version of uniform index query processing. Type:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
            return DBERR_FEATURE_UNSUPPORTED;
    }
    return ret;
}

/**
 * DATASET OPTIONS
 */

void DatasetOptions::clear() {
    R.reset();
    S.reset();
    dataspaceMetadata.clear();
}

Dataset* DatasetOptions::getDatasetR() {
    return R.has_value() ? &R.value() : nullptr;
}

Dataset* DatasetOptions::getDatasetS() {
    return S.has_value() ? &S.value() : nullptr;
}

DB_STATUS DatasetOptions::unloadDataset(int datasetIndex) {
    switch (datasetIndex) {
        case DATASET_R:
            if (R.has_value()) {
                R->clear();
                R.reset();
                // this->print();
                return DBERR_OK;
            }
            break;
        case DATASET_S:
            if (S.has_value()) {
                S->clear();
                S.reset();
                // this->print();
                return DBERR_OK;
            }
            break;
        default:
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid dataset index:", datasetIndex);
            return DBERR_INVALID_PARAMETER;
    }
    
    logger::log_warning("No dataset is loaded with ID", datasetIndex);
    return DBERR_OK;
}

void DatasetOptions::print() {
    logger::log_task("Current DatasetOptions status");
    if (R) {
        printf("  R is loaded. Id: %d, name: %s\n", R->metadata.internalID, R->metadata.datasetName.c_str());
    } else {
        printf("  R is empty.");
    }

    if (S) {
        printf("  S is loaded. Id: %d, name: %s\n", S->metadata.internalID, S->metadata.datasetName.c_str());
    } else {
        printf("  S is empty.");
    }
}

Dataset* DatasetOptions::getDatasetByIdx(int datasetIndex) {
    switch (datasetIndex) {
        case DATASET_R: return R.has_value() ? &R.value() : nullptr;
        case DATASET_S: return S.has_value() ? &S.value() : nullptr;
        default: return nullptr;
    }
}

/**
@brief adds a Dataset to the configuration's dataset metadata
 */
DB_STATUS DatasetOptions::addDataset(DatasetIndex datasetIdx, Dataset&& dataset) {
    switch (datasetIdx) {
        case DATASET_R:
            if (R.has_value()) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Dataset R already exists");
                return DBERR_INVALID_PARAMETER;
            }
            R = std::move(dataset);
            R->metadata.internalID = DATASET_R;
            break;
        case DATASET_S:
            if (S.has_value()) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Dataset S already exists");
                return DBERR_INVALID_PARAMETER;
            }
            S = std::move(dataset);
            S->metadata.internalID = DATASET_S;
            break;
        default:
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid dataset index");
            return DBERR_INVALID_PARAMETER;
    }
    return DBERR_OK;
}


DB_STATUS DatasetOptions::addDataset(Dataset&& dataset, int &id) {
    if (!R.has_value()) {
        R = std::move(dataset);
        R->metadata.internalID = DATASET_R;
        id = DATASET_R;
        // logger::log_success("Set R dataset id to", R->metadata.internalID);
    } else if (!S.has_value()) {
        S = std::move(dataset);
        S->metadata.internalID = DATASET_S;
        id = DATASET_S;
        // logger::log_success("Set S dataset id to", S->metadata.internalID);
    } else {
        logger::log_error(DBERR_INVALID_PARAMETER, "Both R and S datasets exist:", R->metadata.datasetName, S->metadata.datasetName);
        return DBERR_INVALID_PARAMETER;
    }
    return DBERR_OK;
}


void DatasetOptions::updateDataspace() {
    // Reset bounds to extreme values
    dataspaceMetadata.xMinGlobal = std::numeric_limits<double>::max();
    dataspaceMetadata.yMinGlobal = std::numeric_limits<double>::max();
    dataspaceMetadata.xMaxGlobal = std::numeric_limits<double>::lowest();
    dataspaceMetadata.yMaxGlobal = std::numeric_limits<double>::lowest();

    // Update bounds based on available datasets
    if (R) {
        const auto& meta = R->metadata.dataspaceMetadata;
        dataspaceMetadata.xMinGlobal = std::min(dataspaceMetadata.xMinGlobal, meta.xMinGlobal);
        dataspaceMetadata.yMinGlobal = std::min(dataspaceMetadata.yMinGlobal, meta.yMinGlobal);
        dataspaceMetadata.xMaxGlobal = std::max(dataspaceMetadata.xMaxGlobal, meta.xMaxGlobal);
        dataspaceMetadata.yMaxGlobal = std::max(dataspaceMetadata.yMaxGlobal, meta.yMaxGlobal);
    }
    
    if (S) {
        const auto& meta = S->metadata.dataspaceMetadata;
        dataspaceMetadata.xMinGlobal = std::min(dataspaceMetadata.xMinGlobal, meta.xMinGlobal);
        dataspaceMetadata.yMinGlobal = std::min(dataspaceMetadata.yMinGlobal, meta.yMinGlobal);
        dataspaceMetadata.xMaxGlobal = std::max(dataspaceMetadata.xMaxGlobal, meta.xMaxGlobal);
        dataspaceMetadata.yMaxGlobal = std::max(dataspaceMetadata.yMaxGlobal, meta.yMaxGlobal);
    }

    // Calculate extents
    dataspaceMetadata.xExtent = dataspaceMetadata.xMaxGlobal - dataspaceMetadata.xMinGlobal;
    dataspaceMetadata.yExtent = dataspaceMetadata.yMaxGlobal - dataspaceMetadata.yMinGlobal;
    dataspaceMetadata.maxExtent = std::max(dataspaceMetadata.xExtent, dataspaceMetadata.yExtent);

    // Update both datasets' bounds
    updateDatasetDataspaceToGlobal();
}

void DatasetOptions::updateDatasetDataspaceToGlobal() {
    if (R) {
        R->metadata.dataspaceMetadata = dataspaceMetadata;
    }
    if (S) {
        S->metadata.dataspaceMetadata = dataspaceMetadata;
    }
}

Batch::Batch(){}

bool Batch::isValid() {
    return !(destRank == -1 || tag == -1);
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
        size += 4 * sizeof(double);             // mbr
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
    DB_STATUS ret = DBERR_OK;
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
       
        // mbr
        *reinterpret_cast<double*>(localBuffer) = it.mbr.pMin.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.mbr.pMin.y;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.mbr.pMax.x;
        localBuffer += sizeof(double);
        *reinterpret_cast<double*>(localBuffer) = it.mbr.pMax.y;
        localBuffer += sizeof(double);

        // vertex count
        *reinterpret_cast<int*>(localBuffer) = it.getVertexCount();
        localBuffer += sizeof(int);
        // serialize and copy coordinates to buffer
        double* bufferPtr;
        int bufferElementCount;
        ret = it.serializeCoordinates(&bufferPtr, bufferElementCount);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Serializing coordinates failed.");
            free(*buffer);
            *buffer = nullptr;
            return ret;
        }
        // logger::log_task("Adding object to buffer with", bufferElementCount, "elements");
        std::memcpy(localBuffer, bufferPtr, bufferElementCount * sizeof(double));
        localBuffer += bufferElementCount * sizeof(double);
        // free 
        free(bufferPtr);
    }
    bufferSize = bufferSizeRet;
    return ret;
}

/**
@brief fills the struct with data from the input serialized buffer
 * The caller must free the buffer memory
 * @param buffer 
 */
DB_STATUS Batch::deserialize(const char *buffer, int bufferSize) {
    DB_STATUS ret = DBERR_OK;
    int vertexCount;
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
        // mbr
        double xMin, yMin, xMax, yMax;
        xMin = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMin = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        xMax = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        yMax = *reinterpret_cast<const double*>(localBuffer);
        localBuffer += sizeof(double);
        object.setMBR(xMin, yMin, xMax, yMax);
        // vertex count
        vertexCount = *reinterpret_cast<const int*>(localBuffer);
        localBuffer += sizeof(int);
        // vertices
        // std::vector<double> coords(vertexCount * 2);
        const double* vertexDataPtr = reinterpret_cast<const double*>(localBuffer);
        for (size_t j = 0; j < vertexCount * 2; j+=2) {
            object.addPoint(vertexDataPtr[j], vertexDataPtr[j+1]);
        }
        localBuffer += vertexCount * 2 * sizeof(double);
        // object.setPoints(coords);

        // add to batch
        this->addObjectToBatch(object);
    }

    return ret;
}

int TwoGridPartitioning::getPartitionID(int i, int j, int ppd) {
    return (i + (j * ppd));
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
    partPartitionExtentX = partGridDataspaceMetadata.xExtent / globalPartitionsPerDim;
    partPartitionExtentY = partGridDataspaceMetadata.yExtent / globalPartitionsPerDim;
}

void TwoGridPartitioning::setPartGridDataspace(DataspaceMetadata &otherDataspaceMetadata) {
    partGridDataspaceMetadata = otherDataspaceMetadata;
    partPartitionExtentX = partGridDataspaceMetadata.xExtent / globalPartitionsPerDim;
    partPartitionExtentY = partGridDataspaceMetadata.yExtent / globalPartitionsPerDim;
}

double TwoGridPartitioning::getPartPartionExtentX() {
    return partPartitionExtentX;
}

double TwoGridPartitioning::getPartPartionExtentY() {
    return partPartitionExtentY;
}

/** @brief Get the grid's partition ID (from parent). */
int RoundRobinPartitioning::getPartitionID(int i, int j, int ppd) {
    return (i + (j * ppd));
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
            case DT_BOX:
                object = createEmptyRectangleShape();
                break;
            case DT_POLYGON:
                object = createEmptyPolygonShape();
                break;
            default:
                logger::log_error(DBERR_INVALID_DATATYPE, "Invalid datatype in factory method:", dataType);
                return DBERR_INVALID_DATATYPE;
        }
        // set data type
        object.type = dataType;
        return DBERR_OK;
    }
}

namespace qresult_factory
{
    int createNew(int queryID, hec::QueryType queryType, hec::QueryResultType resultType, hec::QResultBase **object) {
        switch (queryType) {
            case hec::Q_RANGE:
                switch (resultType) {
                    case hec::QR_COLLECT:
                        (*object) = new hec::QResultCollect(queryID, queryType, resultType);
                        break;
                    case hec::QR_COUNT:
                        (*object) = new hec::QResultCount(queryID, queryType, resultType);
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for range query. QR type:", resultType);
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
            case hec::Q_DISTANCE_JOIN:
                switch (resultType) {
                    case hec::QR_COLLECT:
                        (*object) = new hec::QPairResultCollect(queryID, queryType, resultType);
                        break;
                    case hec::QR_COUNT:
                        (*object) = new hec::QResultCount(queryID, queryType, resultType);
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for predicate join. QR type:", resultType);
                        return DBERR_QUERY_RESULT_INVALID_TYPE;
                }
                break;
            case hec::Q_FIND_RELATION_JOIN:
                switch (resultType) {
                    case hec::QR_COLLECT:
                        (*object) = new hec::QTopologyResultCollect(queryID, queryType, resultType);
                        break;
                    case hec::QR_COUNT:
                        (*object) = new hec::QTopologyResultCount(queryID, queryType, resultType);
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for Find Relation join. QR type:", resultType);
                        return DBERR_QUERY_RESULT_INVALID_TYPE;
                }
                break;
            case hec::Q_KNN:
                switch (resultType) {
                    case hec::QR_KNN:
                        (*object) = new hec::QResultkNN(queryID, 1);    // dummy value for K, will replace later
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for KNN query. QR type:", resultType);
                        return DBERR_QUERY_RESULT_INVALID_TYPE;
                }
                break;
            default:
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type for qresult_factory::createNew (arguments):", queryType);
                return -1;
        }
        
        return 0;
    }
    
    int createNew(hec::Query* query, hec::QResultBase **object) {
        if (query == nullptr) {
            logger::log_error(DBERR_NULL_PTR_EXCEPTION, "Query null pointer at qresult_factory::createNew.");
            return DBERR_NULL_PTR_EXCEPTION;
        }
        switch (query->getQueryType()) {
            case hec::Q_RANGE:
                switch (query->getResultType()) {
                    case hec::QR_COLLECT:
                        (*object) = new hec::QResultCollect(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    case hec::QR_COUNT:
                        (*object) = new hec::QResultCount(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for range query. QR type:", query->getResultType());
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
            case hec::Q_DISTANCE_JOIN:
                switch (query->getResultType()) {
                    case hec::QR_COLLECT:
                        (*object) = new hec::QPairResultCollect(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    case hec::QR_COUNT:
                        (*object) = new hec::QResultCount(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for predicate join. QR type:", query->getResultType());
                        return DBERR_QUERY_RESULT_INVALID_TYPE;
                }
                break;
            case hec::Q_FIND_RELATION_JOIN:
                switch (query->getResultType()) {
                    case hec::QR_COLLECT:
                        (*object) = new hec::QTopologyResultCollect(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    case hec::QR_COUNT:
                        (*object) = new hec::QTopologyResultCount(query->getQueryID(), query->getQueryType(), query->getResultType());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for Find Relation join. QR type:", query->getResultType());
                        return DBERR_QUERY_RESULT_INVALID_TYPE;
                }
                break;
            case hec::Q_KNN:
                switch (query->getResultType()) {
                    case hec::QR_KNN:
                        (*object) = new hec::QResultkNN(query->getQueryID(), query->getK());
                        break;
                    default:
                        logger::log_error(DBERR_QUERY_RESULT_INVALID_TYPE, "Invalid query result type for KNN query. QR type:", query->getResultType());
                        return DBERR_QUERY_RESULT_INVALID_TYPE;
                }
                break;
            default:
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Invalid query type for qresult_factory::createNew (query pointer):", query->getQueryType());
                return -1;
        }
        
        return 0;
    }
}
