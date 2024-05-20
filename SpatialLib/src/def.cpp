#include "def.h"

namespace spatial_lib 
{
    QueryOutputT g_queryOutput;
    ScalabilityContainerT g_scalContainer;

    void resetQueryOutput() {
        // result
        g_queryOutput.queryResults = 0;
        // topology relations results
        g_queryOutput.topologyRelationsResultMap.clear();
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_DISJOINT, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_EQUAL, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_MEET, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_CONTAINS, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_COVERS, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_COVERED_BY, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_INSIDE, 0));
        g_queryOutput.topologyRelationsResultMap.insert(std::make_pair(TR_INTERSECT, 0));
        // statistics
        g_queryOutput.postMBRFilterCandidates = 0;
        g_queryOutput.refinementCandidates = 0;
        g_queryOutput.trueHits = 0;
        g_queryOutput.trueNegatives = 0;
        
        // time
        g_queryOutput.mbrFilterTime = 0;
        g_queryOutput.iFilterTime = 0;
        g_queryOutput.refinementTime = 0;

        // on the fly april
        g_queryOutput.rasterizationsDone = 0;
        
    }

    void setupScalabilityTesting() {
        g_scalContainer.numberOfBuckets = 10;
        g_scalContainer.bucketDataPath = "../data/scalability_tests/europe_buckets_" + std::to_string(g_scalContainer.numberOfBuckets) + ".csv";
        g_scalContainer.bucketContentsPath = "../data/scalability_tests/europe_bucketed_pairs_" + std::to_string(g_scalContainer.numberOfBuckets) + ".csv";

        // load bucket data (bucket ID and ranges)
        char del;
        uint bucketID, rangeMin, rangeMax, bucketCount;
        std::string line;
        // skip header
        std::ifstream fin(g_scalContainer.bucketDataPath, std::ios::in);
        getline(fin, line);
        while (!fin.eof()) {
            fin >> bucketID >> del >> rangeMin >> del >> rangeMax >> del >> bucketCount;
            g_scalContainer.bucketIDTobucketRangeMap.insert(std::make_pair(bucketID, std::make_pair(rangeMin, rangeMax)));
            g_scalContainer.bucketIfilterTime.insert(std::make_pair(bucketID,0));
            g_scalContainer.bucketRefinementTime.insert(std::make_pair(bucketID,0));
            g_scalContainer.bucketInconclusiveCount.insert(std::make_pair(bucketID, 0));
        }
        fin.close();
        // load bucket contents
        uint idR, idS, complexity;
        fin.open(g_scalContainer.bucketContentsPath, std::ios::in);
        // skip header
        getline(fin, line);
        while (!fin.eof()) {
            fin >> idR >> del >> idS >> del >> complexity >> del >> bucketID;
            g_scalContainer.pairToBucketIDMap.insert(std::make_pair(std::make_pair(idR,idS),bucketID));
            // if (bucketID == 9) {
            //     printf("%u,%u\n",idR,idS);
            // }
        }
        fin.close();
    }

    uint getBucketOfPair(uint idR, uint idS) {
        auto it = g_scalContainer.pairToBucketIDMap.find(std::make_pair(idR,idS));
        if (it != g_scalContainer.pairToBucketIDMap.end()) {
            return it->second;
        } else {
            printf("Error: couldn't find bucket for pair: %u,%u \n", idR, idS);
            exit(-1);
        }
    }

    void addIFilterTimeToBucket(uint bucketID, double time) {
        auto it = g_scalContainer.bucketIfilterTime.find(bucketID);
        it->second += time;
    }

    void addRefinementTimeToBucket(uint bucketID, double time) {
        auto it = g_scalContainer.bucketRefinementTime.find(bucketID);
        it->second += time;
    }

    void countInconclusiveToBucket(uint bucketID) {
        auto it = g_scalContainer.bucketInconclusiveCount.find(bucketID);
        it->second++;
    }

    void countAPRILResult(int result) {
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

    void countResult(){
        g_queryOutput.queryResults += 1;
    }

    void countTopologyRelationResult(int relation) {
        g_queryOutput.topologyRelationsResultMap[relation] += 1;
    }
    
    static void deepCopyAprilData(AprilDataT* from, AprilDataT* to) {
        to->numIntervalsALL = from->numIntervalsALL;
        to->intervalsALL = from->intervalsALL;
        to->numIntervalsFULL = from->numIntervalsFULL;
        to->intervalsFULL = from->intervalsFULL;
    }

    void addAprilDataToApproximationDataMap(DatasetT &dataset, uint sectionID, uint recID, AprilDataT* aprilData) {
        // AprilDataT copyAprilData;

        // deepCopyAprilData(aprilData, &copyAprilData);
        
        // store april data
        dataset.sectionMap[sectionID].aprilData.insert(std::make_pair(recID, *aprilData));
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

    std::vector<uint> getCommonSectionIDsOfObjects(Dataset &datasetR, Dataset &datasetS, uint idR, uint idS) {
        auto itR = datasetR.recToSectionIdMap.find(idR);
	    auto itS = datasetS.recToSectionIdMap.find(idS);
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
    

}