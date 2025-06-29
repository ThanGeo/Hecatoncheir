#include <omp.h>
#include "TwoLayer/filter.h"
#include "APRIL/filter.h"

namespace twolayer
{
    namespace range_filter
    {
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            // forward to refinement
            ret = refinement::relate::refinementEntrypoint(objR, objS, g_config.queryPipeline.queryType, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Refinement failed.");
                return ret;
            }
            return ret;
        }

        static inline DB_STATUS internal_RangeCorners(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMin.y > (*pivot)->mbr.pMax.y || window->mbr.pMax.y < (*pivot)->mbr.pMin.y || window->mbr.pMin.x > (*pivot)->mbr.pMax.x || window->mbr.pMax.x < (*pivot)->mbr.pMin.x) {
                    pivot++;
                    continue;
                }
                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS internal_RangeB_Class(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMax.y < (*pivot)->mbr.pMin.y || window->mbr.pMin.y > (*pivot)->mbr.pMax.y) {
                    pivot++;
                    continue;
                }
                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS internal_RangeC_Class(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMin.x > (*pivot)->mbr.pMax.x || window->mbr.pMax.x < (*pivot)->mbr.pMin.x) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS internal_RangeCorners_ABCD(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMin.x > (*pivot)->mbr.pMax.x || window->mbr.pMin.y > (*pivot)->mbr.pMax.y) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        
        static inline DB_STATUS RangeCorners(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeCorners(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS RangeCorners_ABCD(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeCorners_ABCD(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS internal_RangeCorners_AB(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMin.y > (*pivot)->mbr.pMax.y || window->mbr.pMax.x < (*pivot)->mbr.pMin.x) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS internal_RangeCorners_AC(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMax.y < (*pivot)->mbr.pMin.y || window->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }


        static inline DB_STATUS RangeCorners_AB(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeCorners_AB(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS RangeCorners_AC(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeCorners_AC(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS internal_RangeCorners_A(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMax.y < (*pivot)->mbr.pMin.y || window->mbr.pMax.x < (*pivot)->mbr.pMin.x) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS RangeCorners_A(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeCorners_A(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS internal_RangeBorders_A_Horizontally(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMax.y < (*pivot)->mbr.pMin.y) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS RangeBorders_A_Horizontally(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeBorders_A_Horizontally(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS internal_RangeBorders_AC(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS RangeBorders_AC(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeBorders_AC(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }
        
        static inline DB_STATUS internal_RangeBorders_AB(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMin.y > (*pivot)->mbr.pMax.y) {
                    pivot++;
                    continue;
                }
                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS RangeBorders_AB(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeBorders_AB(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners failed.");
                return ret;
            }
            
            return ret;
        }

        static inline DB_STATUS RangeB_Class(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeB_Class(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners B failed.");
                return ret;
            }

            return ret;
        }

        static inline DB_STATUS RangeC_Class(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeC_Class(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners B failed.");
                return ret;
            }

            return ret;
        }

        static inline DB_STATUS internal_RangeBorders_A_Vertically(std::vector<Shape*>::iterator &firstFS, Shape* window, std::vector<Shape*>::iterator &lastFS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while(pivot < lastFS) {
                if (window->mbr.pMax.x < (*pivot)->mbr.pMin.x) {
                    pivot++;
                    continue;
                }

                ret = forwardPair(*pivot, window, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS RangeBorders_A_Vertically(std::vector<Shape*>* objectsR, Shape* objectS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto lastR = objectsR->end();

            ret = internal_RangeBorders_A_Vertically(r, objectS, lastR, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Internal range corners B failed.");
                return ret;
            }

            return ret;
        }

        DB_STATUS evaluate(hec::RangeQuery *rangeQuery, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* dataset = g_config.datasetOptions.getDatasetByIdx(rangeQuery->getDatasetID());
            // create shape object for window
            Shape window;
            ret = shape_factory::createEmpty((DataType) rangeQuery->getShapeType(), window);
            if (ret != DBERR_OK) {
                return ret;
            }
            ret = window.setFromWKT(rangeQuery->getWKT());
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Setting window shape from WKT failed. WKT:", rangeQuery->getWKT());
                return ret;
            }
            window.setMBR();
            
            // get cells range
            int partitionMinX = std::floor((window.mbr.pMin.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
            int partitionMinY = std::floor((window.mbr.pMin.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
            int partitionMaxX = std::floor((window.mbr.pMax.x - g_config.datasetOptions.dataspaceMetadata.xMinGlobal) / g_config.partitioningMethod->getPartPartionExtentX());
            int partitionMaxY = std::floor((window.mbr.pMax.y - g_config.datasetOptions.dataspaceMetadata.yMinGlobal) / g_config.partitioningMethod->getPartPartionExtentY());
            
            int partitionID = -1;
            PartitionBase* partition = nullptr;
            // if only one cell
            if (partitionMinX == partitionMaxX && partitionMinY == partitionMaxY) {
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    // valid partition
                    // check A,B,C,D
                    for (int tl_class=CLASS_A; tl_class<=CLASS_D; tl_class++) {    
                        ret = RangeCorners(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }
            } else if (partitionMinY == partitionMaxY) {
                // first tile in the row
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    // valid partition
                    // check A,B,C,D
                    for (int tl_class=CLASS_A; tl_class<=CLASS_D; tl_class++) {    
                        ret = RangeCorners(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }
                //intermidiate tiles in the row
                for (int i=partitionMinX+1; i<partitionMaxX; i++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(i, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition != nullptr) {
                        // range B class, check A,B
                        for (int tl_class=CLASS_A; tl_class<=CLASS_B; tl_class++) {    
                            ret = RangeB_Class(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                            if (ret != DBERR_OK) {
                                return ret;
                            }
                        }
                    }
                }
                //end tile in the row
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMaxX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    // check A,B
                    for (int tl_class=CLASS_A; tl_class<=CLASS_B; tl_class++) {    
                        ret = RangeCorners(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }
            } else if (partitionMinX == partitionMaxX) {
                // first tile in the column
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    // valid partition
                    // check A,B,C,D
                    for (int tl_class=CLASS_A; tl_class<=CLASS_D; tl_class++) {    
                        ret = RangeCorners(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }
                //intermidiate tiles in the column
                for (int j=partitionMinY+1; j<partitionMaxY; j++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition != nullptr) {
                        // check A,C
                        ret = RangeC_Class(partition->getContents((TwoLayerClass) CLASS_A), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        ret = RangeC_Class(partition->getContents((TwoLayerClass) CLASS_C), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }
                //end tile in the column
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMaxY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    // check A
                    ret = RangeCorners(partition->getContents((TwoLayerClass) CLASS_A), &window, queryResult.get());
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    // check C
                    ret = RangeCorners(partition->getContents((TwoLayerClass) CLASS_C), &window, queryResult.get());
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
            } else {
                //first row
                //first tile in the row
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    // valid partition
                    for (int tl_class=CLASS_A; tl_class<=CLASS_D; tl_class++) {    
                        ret = RangeCorners_ABCD(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }
                //intermidiate tiles in the row
                for (int i=partitionMinX+1; i<partitionMaxX; i++) {
                    // AB
                    partitionID = g_config.partitioningMethod->getPartitionID(i, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition != nullptr) {
                        for (int tl_class=CLASS_A; tl_class<=CLASS_B; tl_class++) {    
                            ret = RangeBorders_AB(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                            if (ret != DBERR_OK) {
                                return ret;
                            }
                        }
                    }
                }
                //end tile in the row
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMaxX, partitionMinY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    for (int tl_class=CLASS_A; tl_class<=CLASS_B; tl_class++) {    
                        // range borders AB
                        ret = RangeCorners_AB(partition->getContents((TwoLayerClass) tl_class), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }

                //intermidiate rows
                for (int j=partitionMinY+1; j<partitionMaxY; j++) {
                    //first tile in the row
                    // range borders AC
                    partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition != nullptr) {
                        ret = RangeBorders_AC(partition->getContents(CLASS_A), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                        ret = RangeBorders_AC(partition->getContents(CLASS_C), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }

                    //intermidiate tiles in the row
                    for (int i=partitionMinX+1; i<partitionMaxX; i++) {
                        // get A results true
                        partitionID = g_config.partitioningMethod->getPartitionID(i, j, g_config.partitioningMethod->getGlobalPPD());
                        partition = dataset->index->getPartition(partitionID);
                        if (partition != nullptr) {
                            /** @todo GET ONLY A RESULTS, NO CHECKS */
                            std::vector<Shape*>* contents = partition->getContents(CLASS_A);
                            for (auto &it: *contents) {
                                queryResult->addResult(it->recID);
                            }
                        }
                    }

                    //end tile in the row   
                    partitionID = g_config.partitioningMethod->getPartitionID(partitionMaxX, j, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition != nullptr) {
                        ret = RangeBorders_A_Vertically(partition->getContents(CLASS_A), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }
                    }
                }

                //last row 
                //first tile in the row
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMinX, partitionMaxY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {
                    ret = RangeCorners_AC(partition->getContents(CLASS_A), &window, queryResult.get());
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    ret = RangeCorners_AC(partition->getContents(CLASS_C), &window, queryResult.get());
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }

                //intermidiate tiles in the row
                for (int i=partitionMinX+1; i<partitionMaxX; i++) {
                    partitionID = g_config.partitioningMethod->getPartitionID(i, partitionMaxY, g_config.partitioningMethod->getGlobalPPD());
                    partition = dataset->index->getPartition(partitionID);
                    if (partition != nullptr) {
                        // check A horizontally
                        ret = RangeBorders_A_Horizontally(partition->getContents(CLASS_A), &window, queryResult.get());
                        if (ret != DBERR_OK) {
                            return ret;
                        }

                    }
                }

                //last tile in the row
                partitionID = g_config.partitioningMethod->getPartitionID(partitionMaxX, partitionMaxY, g_config.partitioningMethod->getGlobalPPD());
                partition = dataset->index->getPartition(partitionID);
                if (partition != nullptr) {  
                    // check A corners  
                    ret = RangeCorners_A(partition->getContents(CLASS_A), &window, queryResult.get());
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
            }
            
            return ret;
        }
    } // range query mbr filter
}