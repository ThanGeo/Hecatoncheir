#include "TwoLayer/filter.h"


namespace twolayer
{   
    namespace mbr_topological_join_filter
    {
        
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, MBRRelationCase mbrRelationCase, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            if (mbrRelationCase != MBR_CROSS) {
                // count as MBR filter result
                // if intermediate filter is enabled
                if (g_config.queryMetadata.IntermediateFilter) {
                    // forward to intermediate filter
                    ret = APRIL::topology::IntermediateFilterEntrypoint(objR, objS, mbrRelationCase, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Topology intermediate filter failed.");
                        return ret;
                    }
                } else {
                    // count refinement candidates
                    // forward to refinement
                    ret = refinement::topology::specializedRefinementEntrypoint(objR, objS, mbrRelationCase, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Refinement failed");
                        return ret;
                    }
                }
            } else {
                // they cross, true hit intersect (skip intermediate filter)
                queryResult.countTopologyRelationResult(TR_INTERSECT, objR->recID, objS->recID);
            }
            return ret;
        }

        
        static inline DB_STATUS relateMBRs(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            // compute deltas
            double d_xmin = objR->mbr.pMin.x - objS->mbr.pMin.x;
            double d_ymin = objR->mbr.pMin.y - objS->mbr.pMin.y;
            double d_xmax = objR->mbr.pMax.x - objS->mbr.pMax.x;
            double d_ymax = objR->mbr.pMax.y - objS->mbr.pMax.y;

            // check for equality using an error margin because doubles
            if (abs(d_xmin) < EPS) {
                if (abs(d_xmax) < EPS) {
                    if (abs(d_ymin) < EPS) {
                        if (abs(d_ymax) < EPS) {
                            // equal MBRs
                            ret = forwardPair(objR, objS, MBR_EQUAL, queryResult);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for equal MBRs stopped with error.");
                            }
                            return ret;
                        }
                    }
                }
            }
            // not equal MBRs, check other relations
            if (d_xmin <= 0) {
                if (d_xmax >= 0) {
                    if (d_ymin <= 0) {
                        if (d_ymax >= 0) {
                            // MBR(s) inside MBR(r)
                            ret = forwardPair(objR, objS, MBR_S_IN_R, queryResult);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBR(s) inside MBR(r) stopped with error.");
                            }
                            return ret;
                        }
                    } else {
                        if (d_ymax < 0 && d_xmax > 0 && d_xmin < 0 && d_ymin < 0) {
                            // MBRs cross each other
                            ret = forwardPair(objR, objS, MBR_CROSS, queryResult);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBRs cross stopped with error.");
                            }
                            return ret;
                        }
                    }
                }
            } 
            if (d_xmin >= 0) {
                if (d_xmax <= 0) {
                    if (d_ymin >= 0) {
                        if (d_ymax <= 0) {
                            // MBR(r) inside MBR(s)
                            ret = forwardPair(objR, objS, MBR_R_IN_S, queryResult);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBR(r) inside MBR(s) stopped with error.");
                            }
                            return ret;
                        }
                    } else {
                        if (d_ymax > 0 && d_xmax < 0 && d_xmin > 0 && d_ymin > 0) {
                            // MBRs cross each other
                            ret = forwardPair(objR, objS, MBR_CROSS, queryResult);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBRs cross stopped with error.");
                            }
                            return ret;
                        }
                    }
                }
            }
            // MBRs intersect generally
            ret = forwardPair(objR, objS, MBR_INTERSECT, queryResult);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forward for MBRs intersect stopped with error.");
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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
                    ret = relateMBRs(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }

                pivot++;
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {               
                if (((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) || ((*rec)->mbr.pMax.x < (*pivot)->mbr.pMin.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = relateMBRs(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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
                    ret = relateMBRs(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {       
                if ((*pivot)->mbr.pMin.x > (*rec)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = relateMBRs(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_4(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {                       
                if ((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = relateMBRs(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_5(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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
                    ret = relateMBRs(*pivot, *rec, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS sweepRollY_1(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS sweepRollY_2(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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
            while ((r != lastR))
            {
                ret = internal_sweepRollY_2(r, s, lastS, flag, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_3(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS sweepRollY_4(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        
        inline DB_STATUS sweepRollY_5(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        /**
         * optimized two-layer MBR filter for find topological relation queries, with intermediate filter forwarding
         */
        
        static DB_STATUS evaluate(hec::JoinQuery *joinQuery, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetOptions.getDatasetByIdx(joinQuery->getDatasetRid());
            Dataset* S = g_config.datasetOptions.getDatasetByIdx(joinQuery->getDatasetSid());
            // here the final results will be stored
            #pragma omp parallel reduction(query_output_reduction:queryResult)
            {
                DB_STATUS local_ret = DBERR_OK;
                hec::QueryResult threadQueryResults(queryResult.getID(), queryResult.getQueryType(), queryResult.getResultType());
                // loop common partitions (@todo: optimize to start from the dataset that has the fewer ones)
                std::vector<PartitionBase *>* partitions = R->index->getPartitions();
                #pragma omp for
                for (int i=0; i<partitions->size(); i++) {
                    // get partition ID and S container
                    PartitionBase* partitionR = partitions->at(i);
                    PartitionBase* partitionS = S->index->getPartition(partitions->at(i)->partitionID);
                    // if relation S has any objects for this partition (non-empty container)
                    if (partitionS != nullptr) {
                        // common partition found
                        // R_A - S_A
                        local_ret = sweepRollY_1(partitionR->getContents(CLASS_A), partitionS->getContents(CLASS_A), threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        local_ret = sweepRollY_2(partitionS->getContents(CLASS_B), partitionR->getContents(CLASS_A), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        local_ret = sweepRollY_3(partitionR->getContents(CLASS_A), partitionS->getContents(CLASS_C), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        local_ret = sweepRollY_5(partitionS->getContents(CLASS_D), partitionR->getContents(CLASS_A), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        local_ret = sweepRollY_2(partitionR->getContents(CLASS_B), partitionS->getContents(CLASS_A), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        local_ret = sweepRollY_4(partitionR->getContents(CLASS_B), partitionS->getContents(CLASS_C), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        local_ret = sweepRollY_3(partitionS->getContents(CLASS_A), partitionR->getContents(CLASS_C), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_A - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_C
                        local_ret = sweepRollY_4(partitionS->getContents(CLASS_B), partitionR->getContents(CLASS_C), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        local_ret = sweepRollY_5(partitionR->getContents(CLASS_D), partitionS->getContents(CLASS_A), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
                // reduct thread results to global
                queryResult.mergeTopologyRelationResults(threadQueryResults);
            }
            return ret;
        }
    }

    namespace mbr_intersection_join_filter
    {
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            // check if intermediate filter is enabled
            if (g_config.queryMetadata.IntermediateFilter) {
                // forward to intermediate filter
                ret = APRIL::standard::IntermediateFilterEntrypoint(objR, objS, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Intermediate filter failed.");
                    return ret;
                }
            } else {
                // forward to refinement
                ret = refinement::relate::refinementEntrypoint(objR, objS, g_config.queryMetadata.queryType, queryResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Refinement failed.");
                    return ret;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS internal_sweepRollY_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS internal_sweepRollY_4(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS internal_sweepRollY_5(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS sweepRollY_1(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS sweepRollY_2(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS sweepRollY_3(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        
        static inline DB_STATUS sweepRollY_4(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        
        inline DB_STATUS sweepRollY_5(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, hec::QueryResult &queryResult) {
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

        /**
         * simple two-layer MBR intersection filter with intermediate filter forwarding
         */
        
        static DB_STATUS evaluate(hec::JoinQuery *joinQuery, hec::QueryResult &queryResult) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetOptions.getDatasetByIdx(joinQuery->getDatasetRid());
            Dataset* S = g_config.datasetOptions.getDatasetByIdx(joinQuery->getDatasetSid());
            #pragma omp parallel reduction(query_output_reduction:queryResult)
            {
                DB_STATUS local_ret = DBERR_OK;
                hec::QueryResult threadQueryResults(queryResult.getID(), queryResult.getQueryType(), queryResult.getResultType());
                // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
                std::vector<PartitionBase *>* partitions = R->index->getPartitions();
                #pragma omp for
                for (int i=0; i<partitions->size(); i++) {
                    // get partition ID and S container
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
                        local_ret = sweepRollY_1(partitionR->getContents(CLASS_A), partitionS->getContents(CLASS_A), threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        // printf("SB-RA\n");
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SB");
                        // }
                        local_ret = sweepRollY_2(partitionS->getContents(CLASS_B), partitionR->getContents(CLASS_A), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        // printf("RA-SC\n");
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SC");
                        // }
                        local_ret = sweepRollY_3(partitionR->getContents(CLASS_A), partitionS->getContents(CLASS_C), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RA-SD");
                        // }
                        // printf("SD-RA\n");
                        local_ret = sweepRollY_5(partitionS->getContents(CLASS_D), partitionR->getContents(CLASS_A), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RB-SA");
                        // }
                        // printf("RA-SA\n");
                        local_ret = sweepRollY_2(partitionR->getContents(CLASS_B), partitionS->getContents(CLASS_A), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RB-SC");
                        // }
                        // printf("RB-SC\n");
                        local_ret = sweepRollY_4(partitionR->getContents(CLASS_B), partitionS->getContents(CLASS_C), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RC-SA");
                        // }
                        // printf("SA-RC\n");
                        local_ret = sweepRollY_3(partitionS->getContents(CLASS_A), partitionR->getContents(CLASS_C), 1, threadQueryResults);
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
                        local_ret = sweepRollY_4(partitionS->getContents(CLASS_B), partitionR->getContents(CLASS_C), 1, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        // if (partitionR->partitionID == 651299 || partitionR->partitionID == 651300) {
                        //     logger::log_task("RD-SA");
                        // }
                        // printf("RD-SA\n");
                        local_ret = sweepRollY_5(partitionR->getContents(CLASS_D), partitionS->getContents(CLASS_A), 0, threadQueryResults);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
                // reduct thread results to global
                queryResult.mergeResults(threadQueryResults);
            }
            return ret;
        }
    }

    DB_STATUS processQuery(hec::Query* query, hec::QueryResult &queryResult) {
        DB_STATUS ret = DBERR_OK;
        // set to global config
        g_config.queryMetadata.queryType = (hec::QueryType) query->getQueryType();

        // switch based on query type
        switch (query->getQueryType()) {
            case hec::Q_RANGE:
                /** @brief todo: range query */
                return DBERR_FEATURE_UNSUPPORTED;
                break;
            case hec::Q_INTERSECTION_JOIN:
            case hec::Q_INSIDE_JOIN:
            case hec::Q_DISJOINT_JOIN:
            case hec::Q_EQUAL_JOIN:
            case hec::Q_MEET_JOIN:
            case hec::Q_CONTAINS_JOIN:
            case hec::Q_COVERS_JOIN:
            case hec::Q_COVERED_BY_JOIN: 
            {
                // cast
                hec::JoinQuery* joinQuery = dynamic_cast<hec::JoinQuery*>(query);
                // evaluate
                ret = mbr_intersection_join_filter::evaluate(joinQuery, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
                break;
            case hec::Q_FIND_RELATION_JOIN: 
            {
                // cast
                hec::JoinQuery* joinQuery = dynamic_cast<hec::JoinQuery*>(query);
                // evaluate
                ret = mbr_topological_join_filter::evaluate(joinQuery, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }
}