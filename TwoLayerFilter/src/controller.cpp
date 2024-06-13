#include "controller.h"


namespace two_layer
{
    int partitionsPerDimension = 1000;
    int runNumPartitions = partitionsPerDimension * partitionsPerDimension;
    Relation R, S, *pR, *pS;
    Relation *pRA, *pSA, *pRB, *pSB, *pRC, *pSC, *pRD, *pSD;
    size_t *pRA_size, *pSA_size, *pRB_size, *pSB_size, *pRC_size, *pSC_size, *pRD_size, *pSD_size;
    size_t *pR_size ,*pS_size;
    vector<ABrec> *pRABdec , *pSABdec;
    vector<Crec> *pRCdec, *pSCdec;
    vector<Drec> *pRDdec, *pSDdec;
    vector<Coord> *pRYEND, *pSYEND;     

    unsigned long long result = 0;
    
    void initTwoLayer(uint partitionsPerDimension, spatial_lib::MBRFilterTypeE mbrFilterType, spatial_lib::QueryTypeE queryType) {
        // set type
        g_mbrFilterType = mbrFilterType;
        g_queryType = queryType;
        
        // get global boundaries of datasets
        Coord minX = min(R.minX, S.minX);
        Coord maxX = max(R.maxX, S.maxX);
        Coord minY = min(R.minY, S.minY);
        Coord maxY = max(R.maxY, S.maxY);
        Coord diffX = maxX - minX;
        Coord diffY = maxY - minY;
        Coord maxExtend = (diffX<diffY)?diffY:diffX;
        //normalize for MBR filter
        R.normalize(minX, maxX, minY, maxY, maxExtend);
        S.normalize(minX, maxX, minY, maxY, maxExtend);

        //PREPARE MBR FILTER
        runNumPartitions = partitionsPerDimension * partitionsPerDimension;
        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];

        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];
        //MBR FILTER PRE-PROCESSING (partitioning and sorting)
        fs_2d::single::PartitionTwoDimensional(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, partitionsPerDimension);
        fs_2d::single::sort::oneArray::SortYStartOneArray(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size , pRD_size, pSD_size, runNumPartitions);
    }

    void initTwoLayer(uint partitionsPerDimension, double xMin, double yMin, double xMax, double yMax) {
        //fix the relation data space to be the same as our Hilbert data space
        Coord minX = xMin;
        Coord maxX = xMax;
        Coord minY = yMin;
        Coord maxY = yMax;
        Coord diffX = maxX - minX;
        Coord diffY = maxY - minY;
        Coord maxExtend = (diffX<diffY)?diffY:diffX;
        //normalize for MBR filter
        R.normalize(minX, maxX, minY, maxY, maxExtend);
        S.normalize(minX, maxX, minY, maxY, maxExtend);

        //PREPARE MBR FILTER
        runNumPartitions = partitionsPerDimension * partitionsPerDimension;
        pRA_size = new size_t[runNumPartitions];
        pRB_size = new size_t[runNumPartitions];
        pRC_size = new size_t[runNumPartitions];
        pRD_size = new size_t[runNumPartitions];

        pSA_size = new size_t[runNumPartitions];
        pSB_size = new size_t[runNumPartitions];
        pSC_size = new size_t[runNumPartitions];
        pSD_size = new size_t[runNumPartitions];

        memset(pRA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSA_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSB_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSC_size, 0, runNumPartitions*sizeof(size_t));
        memset(pRD_size, 0, runNumPartitions*sizeof(size_t));
        memset(pSD_size, 0, runNumPartitions*sizeof(size_t));

        pR = new Relation[runNumPartitions];
        pS = new Relation[runNumPartitions];
        //MBR FILTER PRE-PROCESSING (partitioning and sorting)
        fs_2d::single::PartitionTwoDimensional(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, partitionsPerDimension);
        fs_2d::single::sort::oneArray::SortYStartOneArray(pR, pS, pRB_size, pSB_size, pRC_size, pSC_size , pRD_size, pSD_size, runNumPartitions);
    }

    /**
     * Sets the (xMin,yMin),(xMax,yMax) values based on the datasets R,S min/max
     * giving the total boundaries that enclose both datasets
    */
    void getDatasetGlobalBounds(double &xMin, double &yMin, double &xMax, double &yMax) {
        xMin = min(R.minX, S.minX);
        yMin = min(R.minY, S.minY);
        xMax = max(R.maxX, S.maxX);
        yMax = max(R.maxY, S.maxY);
    }

    void loadDatasets(std::string &Rpath, std::string &Spath) {
        // Load inputs (creates MBRs from geometry files)
        #pragma omp parallel sections
        {
            #pragma omp section
            {
                R.load(Rpath);
            }
            #pragma omp section
            {
                S.load(Spath);
            }
        }
        cout << "R size: " << R.size() << endl;
        cout << "S size: " << S.size() << endl;
    }

    void addObjectToDataset(bool left, uint recID, double xMin, double yMin, double xMax, double yMax) {
        if (left) {
            // add to left relation (R)
            R.emplace_back(recID, xMin, yMin, xMax, yMax);
            R.minX = std::min(R.minX, xMin);
            R.maxX = std::max(R.maxX, xMax);
            R.minY = std::min(R.minY, yMin);
            R.maxY = std::max(R.maxY, yMax);
        } else {
            // add to right relation (S)
            S.emplace_back(recID, xMin, yMin, xMax, yMax);
            S.minX = std::min(S.minX, xMin);
            S.maxX = std::max(S.maxX, xMax);
            S.minY = std::min(S.minY, yMin);
            S.maxY = std::max(S.maxY, yMax);
        }
    }

    void setNextStage(spatial_lib::IntermediateFilterTypeE iFilterType) {
        g_iFilterType = iFilterType;
    }


    unsigned long long evaluateTwoLayer() {
        switch (g_mbrFilterType) {
            case spatial_lib::MF_INTERSECTION:
                return standard::ForwardScanBased_PlaneSweep_CNT_Y_Less(pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
            case spatial_lib::MF_TOPOLOGY:
                
                switch (g_queryType) {
                    // TODO: OPTIMIZE SO THAT SOME RELATIONS (LIKE INTERSECT) DONT HAVE TO GO THROUGH
                    // THE "OPTIMIZED" MBR FILTER, SINCE SIMPLE INTERSECTION IS ENOUGH FOR THEM
                    case spatial_lib::Q_CONTAINS:
                    case spatial_lib::Q_COVERED_BY:
                    case spatial_lib::Q_COVERS:
                    case spatial_lib::Q_DISJOINT:
                    case spatial_lib::Q_EQUAL:
                    case spatial_lib::Q_INSIDE:
                    case spatial_lib::Q_INTERSECT:
                    case spatial_lib::Q_MEET:
                        return optimized::relate::MBRFilter(pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
                        break;
                    case spatial_lib::Q_FIND_RELATION:
                        return optimized::find_relation::MBRFilter(pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
                        break;
                }
                
            case spatial_lib::MF_SCALABILITY:
                return optimized_scalability::MBRFilter(pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitions);
            default:
                printf("Two-layer filter error: unkown MBR filter type\n");
                exit(-1);
        }
        
    }


    
}