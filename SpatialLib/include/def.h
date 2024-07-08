#ifndef SPATIAL_LIB_DEF_H
#define SPATIAL_LIB_DEF_H

#include <vector>
#include <sys/types.h>
#include <string>
#include <unordered_map>
#include <fstream>

#include "data.h"

#define EPS 1e-08

namespace spatial_lib
{
    typedef enum FileType {
        FT_INVALID,
        FT_BINARY,
        FT_CSV,
        FT_WKT,
    } FileTypeE;

    typedef enum FilterResult {
        TRUE_NEGATIVE,
        TRUE_HIT,
        INCONCLUSIVE,
    } FilterResultT;

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

        QueryOutput();


        void reset();
        void countAPRILresult(int result);
        void countResult();
        void countMBRresult();
        void countRefinementCandidate();
        void countTopologyRelationResult(int result);
        int getResultForTopologyRelation(TopologyRelationE relation);
        void setTopologyRelationResult(int relation, int result);
        /**
         * @brief copies the contents of the 'other' object into this struct
         */
        void shallowCopy(QueryOutput &other);
    } QueryOutputT;
    // global query output variable
    extern QueryOutputT g_queryOutput;

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
        double xExtent, yExtent, maxExtent;
        bool boundsSet = false;

        DataspaceInfo() {
            xMinGlobal = std::numeric_limits<int>::max();
            yMinGlobal = std::numeric_limits<int>::max();
            xMaxGlobal = -std::numeric_limits<int>::max();
            yMaxGlobal = -std::numeric_limits<int>::max();
        }

        void set(double xMinGlobal, double yMinGlobal, double xMaxGlobal, double yMaxGlobal) {
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

        void clear() {
            xMinGlobal = 0;
            yMinGlobal = 0;
            xMaxGlobal = 0;
            yMaxGlobal = 0;
            xExtent = 0;
            yExtent = 0;
        }
    } DataspaceInfoT;

    typedef enum TwoLayerClass {
        CLASS_A,
        CLASS_B,
        CLASS_C,
        CLASS_D,
    } TwoLayerClassE;

    typedef struct TwoLayerContainer {
        std::unordered_map<TwoLayerClassE, std::vector<PolygonT>> classIndex;

        std::vector<PolygonT>* getOrCreateContainerClassContents(TwoLayerClassE classType);
        std::vector<PolygonT>* getContainerClassContents(TwoLayerClassE classType);
        void createClassEntry(TwoLayerClassE classType);
        
    } TwoLayerContainerT;

    typedef struct TwoLayerIndex {
        std::unordered_map<int, TwoLayerContainerT> partitionIndex;
        std::vector<int> partitionIDs;
        // methods
        /** @brief add an object reference to the partition with partitionID, with the specified classType */
        void addObject(int partitionID, TwoLayerClassE classType, PolygonT &polRef);
        TwoLayerContainerT* getOrCreatePartition(int partitionID);
        TwoLayerContainerT* getPartition(int partitionID);
        void createPartitionEntry(int partitionID);
        void sortPartitionsOnY();

    } TwoLayerIndexT;


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
        // unique object count
        int totalObjects = 0;
        // two layer
        TwoLayerIndexT twoLayerIndex;
        /* approximations */ 
        ApproximationTypeE approxType;
        // APRIL
        spatial_lib::AprilConfigT aprilConfig;
        std::unordered_map<uint, SectionT> sectionMap;                        // map: k,v = sectionID,(unordered map of k,v = recID,aprilData)
        std::unordered_map<uint,std::vector<uint>> recToSectionIdMap;         // map: k,v = recID,vector<sectionID>: maps recs to sections 

        /**
         * Methods
        */

        /**
         * returns a reference to the polygon object of the given id
         */
        // PolygonT* getPolygonByID(int recID);
        /**
         * adds a polygon to all related data structures.
         */
        void addPolygon(PolygonT &polygon);
        // calculate the size needed for the serialization buffer
        int calculateBufferSize();
        /**
         * serialize dataset info (only specific stuff)
         */
        int serialize(char **buffer);
        /**
         * deserializes a serialized buffer that contains the dataset info, 
         * into this Dataset object
         */
        void deserialize(const char *buffer, int bufferSize);
        // APRIL
        void addAprilDataToApproximationDataMap(const uint sectionID, const uint recID, const AprilDataT &aprilData);
        void addObjectToSectionMap(const uint sectionID, const uint recID);
        void addIntervalsToAprilData(const uint sectionID, const uint recID, const int numIntervals, const std::vector<uint> &intervals, const bool ALL);
        AprilDataT* getAprilDataBySectionAndObjectID(uint sectionID, uint recID);
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
    std::vector<uint> getCommonSectionIDsOfObjects(Dataset *datasetR, Dataset *datasetS, uint idR, uint idS);

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