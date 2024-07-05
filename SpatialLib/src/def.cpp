#include "def.h"

namespace spatial_lib 
{
    QueryOutputT g_queryOutput;

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

        // on the fly april
        this->rasterizationsDone = 0;
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

        // on the fly april
        this->rasterizationsDone = 0;
    }

    void QueryOutput::countAPRILresult(int result) {
        switch (result) {
            case TRUE_NEGATIVE:
                g_queryOutput.trueNegatives += 1;
                break;
            case TRUE_HIT:
                g_queryOutput.trueHits += 1;
                break;
            case INCONCLUSIVE:
                g_queryOutput.refinementCandidates += 1;
                break;
        }
    }

    void QueryOutput::countResult(){
        g_queryOutput.queryResults += 1;
    }

    void QueryOutput::countMBRresult(){
        g_queryOutput.postMBRFilterCandidates += 1;
    }

    void QueryOutput::countTopologyRelationResult(int relation) {
        g_queryOutput.topologyRelationsResultMap[relation] += 1;
    }

    int QueryOutput::getResultForTopologyRelation(TopologyRelationE relation) {
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

        // on the fly april
        this->rasterizationsDone = other.rasterizationsDone;
    }
    
    static void deepCopyAprilData(AprilDataT* from, AprilDataT* to) {
        to->numIntervalsALL = from->numIntervalsALL;
        to->intervalsALL = from->intervalsALL;
        to->numIntervalsFULL = from->numIntervalsFULL;
        to->intervalsFULL = from->intervalsFULL;
    }

    

    void addObjectToSectionMap(DatasetT &dataset, uint sectionID, uint recID) {
        // store mapping recID -> sectionID
        auto it = dataset.recToSectionIdMap.find(recID);
        if (it != dataset.recToSectionIdMap.end()) {
            // exists
            it->second.emplace_back(sectionID);
        } else {
            // doesnt exist, new entry
            std::vector<uint> sectionIDs = {sectionID};
            dataset.recToSectionIdMap.insert(std::make_pair(recID, sectionIDs));
        }
    }

    AprilDataT* getAprilDataBySectionAndObjectIDs(Dataset &dataset, uint sectionID, uint recID) {
        auto sec = dataset.sectionMap.find(sectionID);
        if (sec != dataset.sectionMap.end()){
            auto obj = sec->second.aprilData.find(recID);
            if (obj != sec->second.aprilData.end()) {
                return &(obj->second);
            }
        }
        return nullptr;
    }

    std::vector<uint> getCommonSectionIDsOfObjects(Dataset *datasetR, Dataset *datasetS, uint idR, uint idS) {
        auto itR = datasetR->recToSectionIdMap.find(idR);
	    auto itS = datasetS->recToSectionIdMap.find(idS);
        std::vector<uint> commonSectionIDs;
        set_intersection(itR->second.begin(),itR->second.end(),itS->second.begin(),itS->second.end(),back_inserter(commonSectionIDs));
        return commonSectionIDs;
    }

    std::vector<uint>* getSectionIDsOfObject(DatasetT &dataset, uint id) {
        auto it = dataset.recToSectionIdMap.find(id);
        if (it == dataset.recToSectionIdMap.end()) {
            // return empty vector
            return nullptr;
        }
        return &it->second;
    }

    std::unordered_map<uint,unsigned long> loadOffsetMap(std::string &offsetMapPath){
        unsigned long offset;
        uint lineCounter = 0;
        uint recID;
        std::ifstream fin(offsetMapPath, std::fstream::in | std::ios_base::binary);
        std::unordered_map<uint,unsigned long> offset_map;
        int totalLines;

        //read total lines
        fin.read((char*) &totalLines, sizeof(int));

        while(lineCounter < totalLines){
            //read rec id
            fin.read((char*) &recID, sizeof(int));
            //read byte offset
            fin.read((char*) &offset, sizeof(unsigned long));

            offset_map.insert(std::make_pair(recID, offset));		
            lineCounter++;
        }
        fin.close();

        return offset_map;
    }

    bg_polygon loadPolygonFromDiskBoostGeometry(uint recID, std::ifstream &fin, std::unordered_map<uint,unsigned long> &offsetMap) {
        bg_polygon pol;
        int readID;
        int vertexCount, polygonCount;
        double x,y;
        //search the map for the specific polygon offset
        std::unordered_map<uint,unsigned long>::const_iterator got = offsetMap.find(recID);
        if(got != offsetMap.end()){ 
            //set read offset
            fin.seekg(got->second-fin.tellg(), fin.cur);		
            //read rec ID
            fin.read((char*) &readID, sizeof(int));
            //read vertex count
            fin.read((char*) &vertexCount, sizeof(int));
            for(int i=0; i<vertexCount; i++){
                fin.read((char*) &x, sizeof(double));
                fin.read((char*) &y, sizeof(double));

                pol.outer().push_back(bg_point_xy(x,y));
            }
        } else {
            printf("Object with id %u not found in offset map.\n", recID);
        }
        boost::geometry::correct(pol);
        return pol;
    }

    SectionT* getSectionByID(DatasetT &dataset, uint sectionID) {
        auto it = dataset.sectionMap.find(sectionID);
        if (it == dataset.sectionMap.end()) {
            return nullptr;
        }
        return &(it->second);
    }

    std::vector<SectionT*> getSectionsOfMBR(spatial_lib::DatasetT &dataset, double xMin, double yMin, double xMax, double yMax) {
        uint i_min = (xMin - dataset.dataspaceInfo.xMinGlobal) / (dataset.dataspaceInfo.xExtent / dataset.aprilConfig.partitions);
        uint j_min = (yMin - dataset.dataspaceInfo.yMinGlobal) / (dataset.dataspaceInfo.yExtent / dataset.aprilConfig.partitions);
        uint i_max = (xMax - dataset.dataspaceInfo.xMinGlobal) / (dataset.dataspaceInfo.xExtent / dataset.aprilConfig.partitions);
        uint j_max = (yMax - dataset.dataspaceInfo.yMinGlobal) / (dataset.dataspaceInfo.yExtent / dataset.aprilConfig.partitions);
        
        std::vector<SectionT*> sections;
        sections.reserve((i_max-i_min) * (j_max-j_min));

        // printf("dataspace MBR: (%f,%f),(%f,%f)\n", dataset.dataspaceInfo.xMinGlobal, dataset.dataspaceInfo.yMinGlobal, dataset.dataspaceInfo.xMaxGlobal, dataset.dataspaceInfo.yMaxGlobal);
        // printf("object MBR: (%f,%f),(%f,%f)\n", xMin, yMin, xMax, yMax);


        for(uint i=i_min; i<=i_max; i++) {
            for(uint j=j_min; j<=j_max; j++) {
                uint sectionID = getSectionIDFromIdxs(i,j, dataset.aprilConfig.partitions);
                
                sections.emplace_back(getSectionByID(dataset, sectionID));

            }
        }

        return sections;
    }


    void printBoostPolygon(bg_polygon &polygon) {
        for(auto &it : polygon.outer()) {
            printf("(%f,%f),", it.x(), it.y());
        }
        printf("\n");
    }

    

    void TwoLayerContainer::createClassEntry(TwoLayerClassE classType) {
        std::vector<PolygonT> vec;
        auto it = classIndex.find(classType);
        if (it == classIndex.end()) {
            classIndex.insert(std::make_pair(classType, vec));
        } else {
            classIndex[classType] = vec;
        }
    }

    std::vector<PolygonT>* TwoLayerContainer::getOrCreateContainerClassContents(TwoLayerClassE classType) {
        auto it = classIndex.find(classType);
        if (it == classIndex.end()) {
            // does not exist, create
            createClassEntry(classType);
            // return its reference
            return &classIndex.find(classType)->second;
        }
        // exists
        return &it->second;
    }

    std::vector<PolygonT>* TwoLayerContainer::getContainerClassContents(TwoLayerClassE classType) {
        auto it = classIndex.find(classType);
        if (it == classIndex.end()) {
            // does not exist
            return nullptr;
        }
        // exists
        return &it->second;
    }

    void TwoLayerIndex::createPartitionEntry(int partitionID) {
        TwoLayerContainerT container;
        auto it = partitionIndex.find(partitionID);
        if (it == partitionIndex.end()) {
            partitionIndex.insert(std::make_pair(partitionID, container));
        } else {
            partitionIndex[partitionID] = container;
        }
    }

    TwoLayerContainerT* TwoLayerIndex::getOrCreatePartition(int partitionID) {
        auto it = partitionIndex.find(partitionID);
        if (it == partitionIndex.end()) {
            // does not exist, create it
            createPartitionEntry(partitionID);
            // return its reference
            TwoLayerContainerT* ref = &partitionIndex.find(partitionID)->second;
            return ref;
        } 
        // exists
        return &it->second;
    }

    TwoLayerContainerT* TwoLayerIndex::getPartition(int partitionID) {
        auto it = partitionIndex.find(partitionID);
        if (it == partitionIndex.end()) {
            // does not exist
            return nullptr;
        } 
        // exists
        return &it->second;
    }

    void TwoLayerIndex::addObject(int partitionID, TwoLayerClassE classType, PolygonT &pol) {
        // get or create new partition entry
        TwoLayerContainerT* partition = getOrCreatePartition(partitionID);
        
        // get or create new class entry of this class type, for this partition
        std::vector<PolygonT>* classObjects = partition->getOrCreateContainerClassContents(classType);

        // if (pol.recID == 112249 || pol.recID == 1782639 || pol.recID == 110360 || pol.recID == 1781854) {
        //     printf("vector size for partition %d, class %d: %d\n", partitionID, classType, classObjects->size());
        // }
        
        // printf("Adding polygon %d with MBR: (%f,%f),(%f,%f)\n", polRef->recID, polRef->mbr.minPoint.x, polRef->mbr.minPoint.y, polRef->mbr.maxPoint.x, polRef->mbr.maxPoint.y);

        // add object
        classObjects->emplace_back(pol);
    }


    bool compareByY(const PolygonT &a, const PolygonT &b) {
        return a.mbr.minPoint.y < b.mbr.minPoint.y;
    }

    void TwoLayerIndex::sortPartitionsOnY() {
        for (auto &it: partitionIndex) {
            // sort A
            std::vector<PolygonT>* objectsA = it.second.getContainerClassContents(CLASS_A);
            if (objectsA != nullptr) {
                std::sort(objectsA->begin(), objectsA->end(), compareByY);
            }
            // sort C
            std::vector<PolygonT>* objectsC = it.second.getContainerClassContents(CLASS_C);
            if (objectsC != nullptr) {
                std::sort(objectsC->begin(), objectsC->end(), compareByY);
            }
        }
    }

    // PolygonT* Dataset::getPolygonByID(int recID) {
    //     auto it = polygons.find(recID);
    //     if (it == polygons.end()) {
    //         return nullptr;
    //     }
    //     return &it->second;
    // }

    void Dataset::addPolygon(PolygonT &polygon) {
        // insert reference to partition index
        for (auto &partitionIT: polygon.partitions) {
            int partitionID = partitionIT.first;
            TwoLayerClassE classType = (TwoLayerClassE) partitionIT.second;
            // add to twolayer index
            this->twoLayerIndex.addObject(partitionID, classType, polygon);

            // if (polygon.recID == 112249 || polygon.recID == 1782639) {
            //     // logger::log_success("pol", polygon.recID, "added to partition", partitionID, "as class", classType);
            //     printf("Polygon %d added to partition %d as class %d\n", polygon.recID, partitionID, classType);
            // }
        }
    }

    // calculate the size needed for the serialization buffer
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

    void Dataset::addAprilDataToApproximationDataMap(const uint sectionID, const uint recID, const AprilDataT &aprilData) {
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

    void Dataset::addObjectToSectionMap(const uint sectionID, const uint recID) {
        AprilDataT aprilData;
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

    void Dataset::addIntervalsToAprilData(const uint sectionID, const uint recID, const int numIntervals, const std::vector<uint> &intervals, const bool ALL) {
        // retrieve section
        auto secIT = this->sectionMap.find(sectionID);
        if (secIT == this->sectionMap.end()) {
            printf("Error: could not find section ID %d in dataset section map\n", sectionID);
            return;
        }
        // retrieve object
        auto it = secIT->second.aprilData.find(recID);
        if (it == secIT->second.aprilData.end()) {
            printf("Error: could not find rec ID %d in section map\n", recID);
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

    AprilDataT* Dataset::getAprilDataBySectionAndObjectID(uint sectionID, uint recID) {
        auto sec = this->sectionMap.find(sectionID);
        if (sec != this->sectionMap.end()){
            auto obj = sec->second.aprilData.find(recID);
            if (obj != sec->second.aprilData.end()) {
                return &(obj->second);
            }
        }
        return nullptr;
    }

}