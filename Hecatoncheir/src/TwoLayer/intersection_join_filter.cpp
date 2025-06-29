#include <omp.h>
#include "TwoLayer/filter.h"
#include "APRIL/filter.h"

namespace twolayer
{
    namespace intersection_join_filter
    {
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            // check if intermediate filter is enabled
            if (g_config.queryPipeline.IntermediateFilter) {
                // forward to intermediate filter
                ret = APRIL::standard::IntermediateFilterEntrypoint(objR, objS, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Intermediate filter failed.");
                    return ret;
                }
            } else {
                // forward to refinement
                ret = refinement::relate::refinementEntrypoint(objR, objS, g_config.queryPipeline.queryType, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Refinement failed.");
                    return ret;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {               
                // disjoint, skip
                if (((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) || ((*rec)->mbr.pMax.x < (*pivot)->mbr.pMin.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {               
                if (((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) || ((*rec)->mbr.pMax.x < (*pivot)->mbr.pMin.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {                
                if ((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                // forward pair
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {       
                if ((*pivot)->mbr.pMin.x > (*rec)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_4(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {                       
                if ((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_5(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QResultBase* queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {                
                if ((*rec)->mbr.pMax.x < (*pivot)->mbr.pMin.x) {
                    pivot++;
                    continue;
                }
                // forward pair
                if (flag) {
                    // pivot is R, rec is S
                    if ((*pivot)->recID == 129032 && (*rec)->recID == 2292762) {
                        logger::log_task("Spotted in filter: mbrs ", (*pivot)->mbr.pMin.x, (*pivot)->mbr.pMin.y, (*pivot)->mbr.pMax.x, (*pivot)->mbr.pMax.y, "and" , (*rec)->mbr.pMin.x, (*rec)->mbr.pMin.y, (*rec)->mbr.pMax.x, (*rec)->mbr.pMax.y);
                    }
                    ret = forwardPair(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    if ((*pivot)->recID == 2292762 && (*rec)->recID == 129032) {
                        logger::log_task("Spotted in filter: mbrs ",  (*rec)->mbr.pMin.x, (*rec)->mbr.pMin.y, (*rec)->mbr.pMax.x, (*rec)->mbr.pMax.y,  "and" , (*pivot)->mbr.pMin.x, (*pivot)->mbr.pMin.y, (*pivot)->mbr.pMax.x, (*pivot)->mbr.pMax.y);
                    }
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS sweepRollY_1(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0 || objectsS->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            std::vector<Shape*>::iterator r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR) && (s != lastS)) {
                if ((*r)->mbr.pMin.y < (*s)->mbr.pMin.y) {
                    // internal loop
                    ret = internal_sweepRollY_1(r, s, lastS, 0, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // internal loop
                    ret = internal_sweepRollY_1(s, r, lastR, 1, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_2(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0 || objectsS->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) {
                ret = internal_sweepRollY_2(r, s, lastS, flag, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_3(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0 || objectsS->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR) && (s != lastS)) {
                if ((*r)->mbr.pMin.y < (*s)->mbr.pMin.y) {
                    // Run internal loop.
                    ret = internal_sweepRollY_3_1(r, s, lastS, flag, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // Run internal loop.
                    // warning: dont remove flag^1, it is required to define which is r and which is s
                    ret = internal_sweepRollY_3_2(s, r, lastR, flag^1, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_4(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0 || objectsS->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_4(r, s, lastS, flag, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }

            return ret;
        }

        
        inline DB_STATUS sweepRollY_5(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QResultBase* queryResult) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            if (objectsR->size() == 0 || objectsS->size() == 0) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_5(r, s, lastS, flag, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        DB_STATUS evaluate(hec::PredicateJoinQuery *joinQuery, std::unique_ptr<hec::QResultBase>& queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetOptions.getDatasetByIdx(joinQuery->getDatasetRid());
            Dataset* S = g_config.datasetOptions.getDatasetByIdx(joinQuery->getDatasetSid());
            #pragma omp parallel num_threads(MAX_THREADS) reduction(query_output_reduction:queryResult)
            {
                DB_STATUS local_ret = DBERR_OK;
                // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
                std::vector<PartitionBase *>* partitions = R->index->getPartitions();
                #pragma omp for
                for (int i=0; i<partitions->size(); i++) {
                    // get partition ID and S container
                    /** @todo optimize: get the partitions with the fewer contents and begin from there, instead of getting R's always */
                    PartitionBase* partitionR = partitions->at(i);
                    PartitionBase* partitionS = S->index->getPartition(partitions->at(i)->partitionID);
                    if (partitionS != nullptr) {
                        // common partition found
                        // printf("Comparing partitions %ld and %ld \n", partitionR->partitionID, partitionS->partitionID);
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("Comparing partition", partitionR->partitionID);
                        // }

                        // R_A - S_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SA");
                        // }
                        // printf("RA-SA\n");
                        local_ret = sweepRollY_1(partitionR->getContents(CLASS_A), partitionS->getContents(CLASS_A), queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        // printf("SB-RA\n");
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SB");
                        // }
                        local_ret = sweepRollY_2(partitionS->getContents(CLASS_B), partitionR->getContents(CLASS_A), 1, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        // printf("RA-SC\n");
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SC");
                        // }
                        local_ret = sweepRollY_3(partitionR->getContents(CLASS_A), partitionS->getContents(CLASS_C), 0, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SD");
                        // }
                        // printf("SD-RA\n");
                        local_ret = sweepRollY_5(partitionS->getContents(CLASS_D), partitionR->getContents(CLASS_A), 1, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RB-SA");
                        // }
                        // printf("RA-SA\n");
                        local_ret = sweepRollY_2(partitionR->getContents(CLASS_B), partitionS->getContents(CLASS_A), 0, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RB-SC");
                        // }
                        // printf("RB-SC\n");
                        local_ret = sweepRollY_4(partitionR->getContents(CLASS_B), partitionS->getContents(CLASS_C), 0, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RC-SA");
                        // }
                        // printf("SA-RC\n");
                        local_ret = sweepRollY_3(partitionS->getContents(CLASS_A), partitionR->getContents(CLASS_C), 1, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_A - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_C
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RC-SB");
                        // }
                        // printf("SB-RC\n");
                        local_ret = sweepRollY_4(partitionS->getContents(CLASS_B), partitionR->getContents(CLASS_C), 1, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RD-SA");
                        // }
                        // printf("RD-SA\n");
                        local_ret = sweepRollY_5(partitionR->getContents(CLASS_D), partitionS->getContents(CLASS_A), 0, queryResult.get());
                        if (local_ret != DBERR_OK) {
                            logger::log_error(local_ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
            }
            // logger::log_success("Computed:", queryResult->getResultCount(), "results.");
            return ret;
        }
    } // intersection mbr filter
}