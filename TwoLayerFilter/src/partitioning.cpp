#include "partitioning.h"

#include "relation.h"

namespace two_layer 
{
    namespace fs_2d{
        
        int myQuotient2(double numer, double denom) {
            return int(numer/denom + EPS);
        };


        int getCellId2(int x, int y, int numCellsPerDimension) {
            return (y * numCellsPerDimension + x);
        };

        namespace single{
            namespace partition{
                namespace oneArray{
                    
                    void Partition_One_Array(const Relation& R, const Relation& S, Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitionsPerRelation)
                    {
                        int runNumPartitions = runNumPartitionsPerRelation*runNumPartitionsPerRelation;
                        Coord partitionExtent = 1.0/runNumPartitionsPerRelation;
    
                        double xStartCell, yStartCell, xEndCell, yEndCell;
                        int firstCell, lastCell;

                        for (size_t i = 0; i < R.size(); i++){
                            auto &r = R[i];

                            // cout << "R: object " << i << endl;

                            // Determine cell for (rec.xStart, rec.yStart)
                            // Determine cell for (rec.xStart, rec.yStart)
                            xStartCell = myQuotient2(r.xStart + EPS, partitionExtent);
                            yStartCell = myQuotient2(r.yStart + EPS, partitionExtent);
                            firstCell = getCellId2(xStartCell, yStartCell, runNumPartitionsPerRelation);

                            if (r.xEnd + EPS >= 1) {
                                xEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                xEndCell = myQuotient2(r.xEnd + EPS, partitionExtent);
                            }

                            if (r.yEnd + EPS >= 1) {
                                yEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                yEndCell = myQuotient2(r.yEnd + EPS, partitionExtent);
                            }

                            lastCell = getCellId2(xEndCell, yEndCell, runNumPartitionsPerRelation);


                            int x = 0, y = 0;

                            // Put record in cells.
                            if (firstCell == lastCell) {
                                pRA_size[firstCell]++;
                            }
                            else {
                                pRA_size[firstCell]++;
                                int cellNumber;
                                for ( int i = xStartCell ; i <= xEndCell ; i++ ){
                                    if ( i != xStartCell){
                                        cellNumber = getCellId2(i, yStartCell, runNumPartitionsPerRelation);
                                        pRC_size[cellNumber]++;
                                    }
                                    for ( int j = yStartCell + 1 ; j <= yEndCell ; j ++ ){
                                        cellNumber = getCellId2(i, j, runNumPartitionsPerRelation);
                                        if( i == xStartCell){
                                            pRB_size[cellNumber]++;
                                        }
                                        else{
                                            pRD_size[cellNumber]++;
                                        }
                                    }
                                }
                            }
                        }


                        for (size_t i = 0; i < S.size(); i++){
                            auto &s = S[i];


                            // cout << "S object " << i << ": " << s.xStart << "," << s.yStart << " and " << s.xEnd << "," << s.yEnd << endl;

                            // Determine cell for (rec.xStart, rec.yStart)
                            // Determine cell for (rec.xStart, rec.yStart)
                            xStartCell = myQuotient2(s.xStart + EPS, partitionExtent);
                            yStartCell = myQuotient2(s.yStart + EPS, partitionExtent);
                            firstCell = getCellId2(xStartCell, yStartCell, runNumPartitionsPerRelation);

                            if (s.xEnd + EPS >= 1) {
                                xEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                xEndCell = myQuotient2(s.xEnd + EPS, partitionExtent);
                            }

                            if (s.yEnd + EPS >= 1) {
                                yEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                yEndCell = myQuotient2(s.yEnd + EPS, partitionExtent);
                            }

                            lastCell = getCellId2(xEndCell, yEndCell, runNumPartitionsPerRelation);

                            int x = 0, y = 0;


                            // cout << "  " << xStartCell << "," << yStartCell << " and " << xEndCell << "," << yEndCell << endl;

                            // Put record in cells.
                            if (firstCell == lastCell) {
                                pSA_size[firstCell]++;
                            }
                            else {
                                pSA_size[firstCell]++;
                                int cellNumber;
                                for ( int i = xStartCell ; i <= xEndCell ; i++ ){
                                    if ( i != xStartCell){
                                        cellNumber = getCellId2(i, yStartCell, runNumPartitionsPerRelation);
                                        pSC_size[cellNumber]++;
                                    }
                                    for ( int j = yStartCell + 1 ; j <= yEndCell ; j ++ ){
                                        cellNumber = getCellId2(i, j, runNumPartitionsPerRelation);
                                        if( i == xStartCell){
                                            pSB_size[cellNumber]++;
                                        }
                                        else{
                                            // cout << "cellNumber: " << cellNumber << endl;
                                            pSD_size[cellNumber]++;
                                        }
                                    }
                                }
                            }
                        }     



                        int counter = 0;
                        for (int i = 0; i < runNumPartitions; i++){
                            counter = pRA_size[i] + pRB_size[i] + pRC_size[i] + pRD_size[i] ;
                            pR[i].resize(counter);  

                            pRB_size[i] = pRA_size[i] + pRB_size[i];
                            pRC_size[i] = counter - pRD_size[i];
                            pRD_size[i] = counter;

                            counter = pSA_size[i] + pSB_size[i] + pSC_size[i] + pSD_size[i] ;       
                            pS[i].resize(counter);       

                            pSB_size[i] = pSA_size[i] + pSB_size[i];
                            pSC_size[i] = counter - pSD_size[i];
                            pSD_size[i] = counter;

                        }

                        for (size_t i = 0; i < R.size(); i++){
                            auto &r = R[i];

                            xStartCell = myQuotient2(r.xStart + EPS, partitionExtent);
                            yStartCell = myQuotient2(r.yStart + EPS, partitionExtent);
                            firstCell = getCellId2(xStartCell, yStartCell, runNumPartitionsPerRelation);

                            if (r.xEnd + EPS >= 1) {
                                xEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                xEndCell = myQuotient2(r.xEnd + EPS, partitionExtent);
                            }

                            if (r.yEnd + EPS >= 1) {
                                yEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                yEndCell = myQuotient2(r.yEnd + EPS, partitionExtent);
                            }
                            lastCell = getCellId2(xEndCell, yEndCell, runNumPartitionsPerRelation);

                            int x = 0 , y = 0;

                            // Put record in cells.
                            if (firstCell == lastCell) {

                                //pR[firstCell][pRA_size[firstCell]] = r;

                                pRA_size[firstCell] = pRA_size[firstCell] - 1;

                                pR[firstCell][pRA_size[firstCell]].id = r.id;
                                pR[firstCell][pRA_size[firstCell]].xStart = r.xStart;
                                pR[firstCell][pRA_size[firstCell]].yStart = r.yStart;
                                pR[firstCell][pRA_size[firstCell]].xEnd = r.xEnd;
                                pR[firstCell][pRA_size[firstCell]].yEnd = r.yEnd;

                                //pRA_size[firstCell] = pRA_size[firstCell] + 1;

                                

                            }
                            else {

                                pRA_size[firstCell] = pRA_size[firstCell] - 1;

                                //pR[firstCell][pRA_size[firstCell]] = r;
                                pR[firstCell][pRA_size[firstCell]].id = r.id;
                                pR[firstCell][pRA_size[firstCell]].xStart = r.xStart;
                                pR[firstCell][pRA_size[firstCell]].yStart = r.yStart;
                                pR[firstCell][pRA_size[firstCell]].xEnd = r.xEnd;
                                pR[firstCell][pRA_size[firstCell]].yEnd = r.yEnd;

                                //pRA_size[firstCell] = pRA_size[firstCell] + 1;
                                

                                int cellNumber;
                                for ( int i = xStartCell ; i <= xEndCell ; i++ ){
                                    if ( i != xStartCell){
                                        cellNumber = getCellId2(i, yStartCell, runNumPartitionsPerRelation);

                                        //pR[cellNumber][pRC_size[cellNumber]] = r;

                                        pRC_size[cellNumber] = pRC_size[cellNumber] - 1;

                                        pR[cellNumber][pRC_size[cellNumber]].id = r.id;
                                        pR[cellNumber][pRC_size[cellNumber]].xStart = r.xStart;
                                        pR[cellNumber][pRC_size[cellNumber]].yStart = r.yStart;
                                        pR[cellNumber][pRC_size[cellNumber]].xEnd = r.xEnd;
                                        pR[cellNumber][pRC_size[cellNumber]].yEnd = r.yEnd;

                                        //pRC_size[cellNumber] = pRC_size[cellNumber] + 1;

                                        

                                    }
                                    for ( int j = yStartCell + 1 ; j <= yEndCell ; j ++ ){
                                        cellNumber = getCellId2(i, j, runNumPartitionsPerRelation);
                                        if( i == xStartCell){

                                            pRB_size[cellNumber] = pRB_size[cellNumber] - 1 ;

                                            //pR[cellNumber][pRB_size[cellNumber]] = r;

                                            pR[cellNumber][pRB_size[cellNumber]].id = r.id;
                                            pR[cellNumber][pRB_size[cellNumber]].xStart = r.xStart;
                                            pR[cellNumber][pRB_size[cellNumber]].yStart = r.yStart;
                                            pR[cellNumber][pRB_size[cellNumber]].xEnd = r.xEnd;
                                            pR[cellNumber][pRB_size[cellNumber]].yEnd = r.yEnd;

                                            //pRB_size[cellNumber] = pRB_size[cellNumber] + 1 ;

                                            

                                        }
                                        else{

                                            //pR[cellNumber][pRD_size[cellNumber]] = r;

                                            pRD_size[cellNumber] = pRD_size[cellNumber] - 1 ;

                                            pR[cellNumber][pRD_size[cellNumber]].id = r.id;
                                            pR[cellNumber][pRD_size[cellNumber]].xStart = r.xStart;
                                            pR[cellNumber][pRD_size[cellNumber]].yStart = r.yStart;
                                            pR[cellNumber][pRD_size[cellNumber]].xEnd = r.xEnd;
                                            pR[cellNumber][pRD_size[cellNumber]].yEnd = r.yEnd;

                                            //pRD_size[cellNumber] = pRD_size[cellNumber] + 1 ;

                                            
                                        }
                                    }
                                }
                            }
                        }

                        for (size_t i = 0; i < S.size(); i++){
                            auto &s = S[i];

                            xStartCell = myQuotient2(s.xStart + EPS, partitionExtent);
                            yStartCell = myQuotient2(s.yStart + EPS, partitionExtent);
                            firstCell = getCellId2(xStartCell, yStartCell, runNumPartitionsPerRelation);

                            if (s.xEnd + EPS >= 1) {
                                xEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                xEndCell = myQuotient2(s.xEnd + EPS, partitionExtent);
                            }

                            if (s.yEnd + EPS >= 1) {
                                yEndCell = runNumPartitionsPerRelation - 1;
                            }
                            else {
                                yEndCell = myQuotient2(s.yEnd + EPS, partitionExtent);
                            }
                            lastCell = getCellId2(xEndCell, yEndCell, runNumPartitionsPerRelation);

                            int x = 0 , y = 0;

                            // Put record in cells.
                            if (firstCell == lastCell) {

                                //pS[firstCell][pSA_size[firstCell]] = s;

                                pSA_size[firstCell] = pSA_size[firstCell] - 1;

                                pS[firstCell][pSA_size[firstCell]].id = s.id;
                                pS[firstCell][pSA_size[firstCell]].xStart = s.xStart;
                                pS[firstCell][pSA_size[firstCell]].yStart = s.yStart;
                                pS[firstCell][pSA_size[firstCell]].xEnd = s.xEnd;
                                pS[firstCell][pSA_size[firstCell]].yEnd = s.yEnd;
                            }
                            else {

                                pSA_size[firstCell] = pSA_size[firstCell] - 1;

                                pS[firstCell][pSA_size[firstCell]].id = s.id;
                                pS[firstCell][pSA_size[firstCell]].xStart = s.xStart;
                                pS[firstCell][pSA_size[firstCell]].yStart = s.yStart;
                                pS[firstCell][pSA_size[firstCell]].xEnd = s.xEnd;
                                pS[firstCell][pSA_size[firstCell]].yEnd = s.yEnd;
                            
                                int cellNumber;
                                for ( int i = xStartCell ; i <= xEndCell ; i++ ){
                                    if ( i != xStartCell){
                                        cellNumber = getCellId2(i, yStartCell, runNumPartitionsPerRelation);

                                        //pS[cellNumber][pSC_size[cellNumber]] = s;
                                        pSC_size[cellNumber] = pSC_size[cellNumber] - 1;

                                        pS[cellNumber][pSC_size[cellNumber]].id = s.id;
                                        pS[cellNumber][pSC_size[cellNumber]].xStart = s.xStart;
                                        pS[cellNumber][pSC_size[cellNumber]].yStart = s.yStart;
                                        pS[cellNumber][pSC_size[cellNumber]].xEnd = s.xEnd;
                                        pS[cellNumber][pSC_size[cellNumber]].yEnd = s.yEnd;
                                    }
                                    for ( int j = yStartCell + 1 ; j <= yEndCell ; j ++ ){
                                        cellNumber = getCellId2(i, j, runNumPartitionsPerRelation);
                                        if( i == xStartCell){

                                            //pS[cellNumber][pSB_size[cellNumber]] = s;

                                            pSB_size[cellNumber] = pSB_size[cellNumber] - 1 ;

                                            pS[cellNumber][pSB_size[cellNumber]].id = s.id;
                                            pS[cellNumber][pSB_size[cellNumber]].xStart = s.xStart;
                                            pS[cellNumber][pSB_size[cellNumber]].yStart = s.yStart;
                                            pS[cellNumber][pSB_size[cellNumber]].xEnd = s.xEnd;
                                            pS[cellNumber][pSB_size[cellNumber]].yEnd = s.yEnd;
                                        }
                                        else{

                                            pSD_size[cellNumber] = pSD_size[cellNumber] - 1 ;

                                            //pS[cellNumber][pSD_size[cellNumber]] = s;

                                            pS[cellNumber][pSD_size[cellNumber]].id = s.id;
                                            pS[cellNumber][pSD_size[cellNumber]].xStart = s.xStart;
                                            pS[cellNumber][pSD_size[cellNumber]].yStart = s.yStart;
                                            pS[cellNumber][pSD_size[cellNumber]].xEnd = s.xEnd;
                                            pS[cellNumber][pSD_size[cellNumber]].yEnd = s.yEnd;

                                            //pSD_size[cellNumber] = pSD_size[cellNumber] + 1 ;

                                            
                                        }
                                    }
                                }
                            }          
                        }                    
                    };
                };
            };
            namespace sort{

                struct myclass {
                    bool operator() (Record &i, Record &j) { return (i.yStart < j.yStart);}
                } myobject;
                
                namespace oneArray{
                    
                    void SortYStartOneArray(Relation *pR, Relation *pS, size_t *pRB_size, size_t *pSB_size,  size_t *pRC_size, size_t *pSC_size,size_t *pRD_size, size_t *pSD_size, int runNumPartitions){
                        for (int i = 0; i < runNumPartitions; i++){
                            auto pRStart = pR[i].begin();
                            std::sort(pRStart, pRStart + pRB_size[i], myobject);
                            std::sort(pRStart + pRC_size[i], pRStart + pRD_size[i], myobject);
                            
                            auto pSStart = pS[i].begin();

                            std::sort(pSStart, pSStart + pSB_size[i], myobject);
                            std::sort(pSStart + pSC_size[i], pSStart + pSD_size[i], myobject);
                        }         
                        
                    };
                };
                namespace decomposition{
                                        
                    void SortYStartOneArray(Relation *pR, Relation *pS, size_t *pRB_size , size_t *pSB_size , size_t *pRC_size, size_t *pSC_size, int runNumPartitions){
        
                        //cout<<"decomposition sort"<<endl;
                        for (int i = 0; i < runNumPartitions; i++){
                            std::sort(pR[i].begin(), pR[i].begin() + pRB_size[i], myobject);
                            std::sort(pS[i].begin(), pS[i].begin() + pSB_size[i], myobject);

                            std::sort(pR[i].begin() + pRC_size[i], pR[i].end(), myobject);
                            std::sort(pS[i].begin() + pSC_size[i], pS[i].end(), myobject);
                        }
                        
                    };
                }
            };
            

            void PartitionTwoDimensional(const Relation& R, const Relation& S, Relation *pR, Relation *pS, size_t *pRA_size, size_t *pSA_size, size_t *pRB_size, size_t *pSB_size, size_t *pRC_size, size_t *pSC_size, size_t *pRD_size, size_t *pSD_size, int runNumPartitionsPerRelation)
            {
                //cout<<"single Partitionnnnnnn" <<endl;
                fs_2d::single::partition::oneArray::Partition_One_Array(R, S, pR, pS, pRA_size, pSA_size, pRB_size, pSB_size, pRC_size, pSC_size, pRD_size, pSD_size, runNumPartitionsPerRelation);
            };

        };

        
    }
}
