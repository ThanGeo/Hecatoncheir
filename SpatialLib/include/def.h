#ifndef SPATIAL_LIB_DEF_H
#define SPATIAL_LIB_DEF_H

#include <vector>
#include <sys/types.h>
#include <string>
#include <unordered_map>
#include <fstream>

#include "data.h"

namespace spatial_lib
{
    typedef enum FileType {
        FT_INVALID,
        FT_BINARY,
        FT_CSV,
        FT_WKT,
    } FileTypeE;

    typedef enum QueryResult {
        TRUE_NEGATIVE,
        TRUE_HIT,
        INCONCLUSIVE,
    } QueryResultT;

    typedef struct QueryOutput {
        // for regular query rsesults
        int queryResults;
        // for topology relations results
        std::unordered_map<int,uint> topologyRelationsResultMap;
        // statistics
        int postMBRFilterCandidates;
        int refinementCandidates;
        int trueHits;
        int trueNegatives;
        // times
        double totalTime;
        double mbrFilterTime;
        double iFilterTime;
        double refinementTime;
        // on the fly april
        uint rasterizationsDone;
    } QueryOutputT;

    // global query output variable
    extern QueryOutputT g_queryOutput;
    typedef enum QueryType{
        Q_NONE, // no query
        Q_RANGE,
        Q_INTERSECT,
        Q_INSIDE,
        Q_DISJOINT,
        Q_EQUAL,
        Q_MEET,
        Q_CONTAINS,
        Q_COVERS,
        Q_COVERED_BY,
        Q_FIND_RELATION,    // find what type of topological relation is there
    }QueryTypeE;

    typedef struct Section {
        uint sectionID;
        // axis position indexes
        uint i,j;
        //objects that intersect this MBR will be assigned to this area
        double interestxMin, interestyMin, interestxMax, interestyMax;
        // double normInterestxMin, normInterestyMin, normInterestxMax, normInterestyMax;
        //this MBR defines the rasterization area (widened interest area to include intersecting polygons completely)
        double rasterxMin, rasteryMin, rasterxMax, rasteryMax;
        // double normRasterxMin, normRasteryMin, normRasterxMax, normRasteryMax;
        // APRIL data
        uint objectCount = 0;
        std::unordered_map<uint, spatial_lib::AprilDataT> aprilData;
    } SectionT;

    typedef struct DataspaceInfo {
        double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
        double xExtent, yExtent;

        void clear() {
            xMinGlobal = 0;
            yMinGlobal = 0;
            xMaxGlobal = 0;
            yMaxGlobal = 0;
            xExtent = 0;
            yExtent = 0;
        }
    } DataspaceInfoT;

    typedef struct Dataset{
        DataTypeE dataType;
        FileTypeE fileType;
        std::string path;
        std::string offsetMapPath;
        // derived from the path
        std::string datasetName;
        // as given by arguments and specified by datasets.ini config file
        std::string nickname;
        // holds the dataset's dataspace info (MBR, extent)
        DataspaceInfoT dataspaceInfo;
        // vector data
        int totalObjects = 0;
        std::unordered_map<int, PolygonT> polygons;
        // index
        std::unordered_map<int, std::vector<PolygonT*>> index;  // todo: maybe store <partitionID, std::vector<recID>> instead
        /* approximations */ 
        ApproximationTypeE approxType;
        // APRIL
        spatial_lib::AprilConfigT aprilConfig;
        std::unordered_map<uint, SectionT> sectionMap;                        // map: k,v = sectionID,(unordered map of k,v = recID,aprilData)
        std::unordered_map<uint,std::vector<uint>> recToSectionIdMap;         // map: k,v = recID,vector<sectionID>: maps recs to sections 

        /**
         * Methods
        */

        PolygonT* getPolygonByID(int recID) {
            auto it = polygons.find(recID);
            if (it == polygons.end()) {
                return nullptr;
            }
            return &it->second;
        }

        void addPolygon(PolygonT &polygon) {
            // insert to map
            polygons.insert(std::make_pair(polygon.recID, polygon));
            // get reference
            PolygonT* polRef = getPolygonByID(polygon.recID);
            if (polRef == nullptr) {
                // definitely an error if this happens
                return;
            }
            // insert reference to partition index
            for (auto &partitionID: polygon.partitionIDs) {
                auto it = index.find(partitionID);
                if (it == index.end()) {
                    std::vector<PolygonT*> newVec = {polRef};
                    index.insert(std::make_pair(partitionID, newVec));
                } else {
                    it->second.emplace_back(polRef);
                }
            }
        }

        // calculate the size needed for the serialization buffer
        int calculateBufferSize() {
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
        int serialize(char **buffer) {
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

        void deserialize(const char *buffer, int bufferSize) {
            int nicknameLength;
            int position = 0;
            
            // dataset data type
            memcpy(&dataType, buffer + position, sizeof(DataTypeE));
            position += sizeof(DataTypeE);
            // dataset nickname length + string
            memcpy(&nicknameLength, buffer + position, sizeof(int));
            position += sizeof(int);
            nickname.assign(buffer + position, nicknameLength);
            position += nicknameLength * sizeof(char);
            // dataset dataspace MBR
            memcpy(&dataspaceInfo.xMinGlobal, buffer + position, sizeof(double));
            position += sizeof(double);
            memcpy(&dataspaceInfo.yMinGlobal, buffer + position, sizeof(double));
            position += sizeof(double);
            memcpy(&dataspaceInfo.xMaxGlobal, buffer + position, sizeof(double));
            position += sizeof(double);
            memcpy(&dataspaceInfo.yMaxGlobal, buffer + position, sizeof(double));
            position += sizeof(double);

            if (position == bufferSize) {
                // all is well
            }
        }

        // APRIL
        void addAprilDataToApproximationDataMap(const uint sectionID, const uint recID, const AprilDataT &aprilData) {
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

        void addObjectToSectionMap(const uint sectionID, const uint recID) {
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

        void addIntervalsToAprilData(const uint sectionID, const uint recID, const int numIntervals, const std::vector<uint> &intervals, const bool ALL) {
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
    }DatasetT;

    typedef struct Query{
        spatial_lib::QueryTypeE type;
        int numberOfDatasets;
        spatial_lib::DatasetT R;         // R: left dataset
        spatial_lib::DatasetT S;         // S: right dataset
        bool boundsSet = false;
        // double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;  // global bounds based on dataset bounds
        DataspaceInfoT dataspaceInfo;
    }QueryT;


    void resetQueryOutput();
    void countAPRILResult(int result);
    void countResult();
    void countTopologyRelationResult(int relation);

    void addObjectToSectionMap(DatasetT &dataset, uint sectionID, uint recID);
    /**
     * @brief returns the APRIL data of an object based on section and rec IDs from a given dataset
     * 
     * @param dataset 
     * @param sectionID 
     * @param recID 
     * @return AprilDataT* 
     */
    AprilDataT* getAprilDataBySectionAndObjectIDs(Dataset &dataset, uint sectionID, uint recID);

    std::unordered_map<uint,unsigned long> loadOffsetMap(std::string &offsetMapPath);
    bg_polygon loadPolygonFromDiskBoostGeometry(uint recID, std::ifstream &fin, std::unordered_map<uint,unsigned long> &offsetMap);

    /**
     * @brief Returns a pointer to the section that corresponds to the argument ID from the dataset
     * 
     * @param dataset 
     * @param sectionID 
     * @return SectionT* 
     */
    SectionT* getSectionByID(DatasetT &dataset, uint sectionID);

    /**
     * @brief Returns a list of pointers to the sections that intersect with the given MBR
     * 
     * @return std::vector<uint> 
     */
    std::vector<SectionT*> getSectionsOfMBR(spatial_lib::DatasetT &dataset, double xMin, double yMin, double xMax, double yMax);

    inline uint getSectionIDFromIdxs(uint i, uint j, uint partitionsNum) {
        return (j * partitionsNum) + i;
    }

    /**
     * @brief returns a list of the common section ids between two objects of two datasets
     * 
     * @param datasetR 
     * @param datasetS 
     * @param idR 
     * @param idS 
     * @return std::vector<uint> 
     * 
     */
    std::vector<uint> getCommonSectionIDsOfObjects(Dataset &datasetR, Dataset &datasetS, uint idR, uint idS);

    /**
     * @brief Get the Section IDs Of an object by ID
     * 
     * @param dataset 
     * @param id 
     * @return std::vector<uint>* 
     */
    std::vector<uint>* getSectionIDsOfObject(DatasetT &dataset, uint id);


    void printBoostPolygon(bg_polygon &polygon);
}

#endif