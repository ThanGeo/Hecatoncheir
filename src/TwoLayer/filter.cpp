#include "TwoLayer/filter.h"


namespace twolayer
{   
    namespace topologyMBRfilter
    {
        
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, MBRRelationCase mbrRelationCase, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            if (mbrRelationCase != MBR_CROSS) {
                // count as MBR filter result
                queryOutput.countMBRresult();
                // if intermediate filter is enabled
                if (g_config.queryInfo.IntermediateFilter) {
                    // forward to intermediate filter
                    ret = APRIL::topology::IntermediateFilterEntrypoint(objR, objS, mbrRelationCase, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Topology intermediate filter failed.");
                        return ret;
                    }
                } else {
                    // count refinement candidates
                    queryOutput.countRefinementCandidate();
                    // forward to refinement
                    ret = refinement::topology::specializedRefinementEntrypoint(objR, objS, mbrRelationCase, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Refinement failed");
                        return ret;
                    }
                }
            } else {
                // they cross, true hit intersect (skip intermediate filter)
                queryOutput.countTopologyRelationResult(TR_INTERSECT);
            }
            return ret;
        }

        
        static inline DB_STATUS relateMBRs(Shape* objR, Shape* objS, QueryOutput &queryOutput) {
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
                            ret = forwardPair(objR, objS, MBR_EQUAL, queryOutput);
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
                            ret = forwardPair(objR, objS, MBR_S_IN_R, queryOutput);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBR(s) inside MBR(r) stopped with error.");
                            }
                            return ret;
                        }
                    } else {
                        if (d_ymax < 0 && d_xmax > 0 && d_xmin < 0 && d_ymin < 0) {
                            // MBRs cross each other
                            ret = forwardPair(objR, objS, MBR_CROSS, queryOutput);
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
                            ret = forwardPair(objR, objS, MBR_R_IN_S, queryOutput);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBR(r) inside MBR(s) stopped with error.");
                            }
                            return ret;
                        }
                    } else {
                        if (d_ymax > 0 && d_xmax < 0 && d_xmin > 0 && d_ymin > 0) {
                            // MBRs cross each other
                            ret = forwardPair(objR, objS, MBR_CROSS, queryOutput);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBRs cross stopped with error.");
                            }
                            return ret;
                        }
                    }
                }
            }
            // MBRs intersect generally
            ret = forwardPair(objR, objS, MBR_INTERSECT, queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forward for MBRs intersect stopped with error.");
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
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
                    ret = relateMBRs(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }

                pivot++;
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {               
                if (((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) || ((*rec)->mbr.pMax.x < (*pivot)->mbr.pMin.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = relateMBRs(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
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
                    ret = relateMBRs(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {       
                if ((*pivot)->mbr.pMin.x > (*rec)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = relateMBRs(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_4(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {                       
                if ((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = relateMBRs(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_5(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
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
                    ret = relateMBRs(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = relateMBRs(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS sweepRollY_1(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
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
                    ret = internal_sweepRollY_1(r, s, lastS, 0, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // internal loop
                    ret = internal_sweepRollY_1(s, r, lastR, 1, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_2(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR))
            {
                ret = internal_sweepRollY_2(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_3(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
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
                    ret = internal_sweepRollY_3_1(r, s, lastS, flag, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // Run internal loop.
                    // warning: dont remove flag^1, it is required to define which is r and which is s
                    ret = internal_sweepRollY_3_2(s, r, lastR, flag^1, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_4(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_4(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }

            return ret;
        }

        
        inline DB_STATUS sweepRollY_5(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_5(r, s, lastS, flag, queryOutput);
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
        
        static DB_STATUS evaluate(QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetInfo.getDatasetR();
            Dataset* S = g_config.datasetInfo.getDatasetS();
            // here the final results will be stored
            #pragma omp parallel reduction(query_output_reduction:queryOutput)
            {
                DB_STATUS local_ret = DBERR_OK;
                QueryOutput localQueryOutput;
                // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
                #pragma omp for
                for (int i=0; i<R->twoLayerIndex.partitions.size(); i++) {
                    // get partition ID and S container
                    int partitionID = R->twoLayerIndex.partitions[i].partitionID;
                    Partition* tlContainerS = S->twoLayerIndex.getPartition(partitionID);
                    // if relation S has any objects for this partition (non-empty container)
                    if (tlContainerS != nullptr) {
                        // common partition found
                        Partition* tlContainerR = &R->twoLayerIndex.partitions[i];
                        // R_A - S_A
                        local_ret = sweepRollY_1(tlContainerR->getContainerClassContents(CLASS_A), tlContainerS->getContainerClassContents(CLASS_A), localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        local_ret = sweepRollY_2(tlContainerS->getContainerClassContents(CLASS_B), tlContainerR->getContainerClassContents(CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        local_ret = sweepRollY_3(tlContainerR->getContainerClassContents(CLASS_A), tlContainerS->getContainerClassContents(CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        local_ret = sweepRollY_5(tlContainerS->getContainerClassContents(CLASS_D), tlContainerR->getContainerClassContents(CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        local_ret = sweepRollY_2(tlContainerR->getContainerClassContents(CLASS_B), tlContainerS->getContainerClassContents(CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        local_ret = sweepRollY_4(tlContainerR->getContainerClassContents(CLASS_B), tlContainerS->getContainerClassContents(CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        local_ret = sweepRollY_3(tlContainerS->getContainerClassContents(CLASS_A), tlContainerR->getContainerClassContents(CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_A - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_C
                        local_ret = sweepRollY_4(tlContainerS->getContainerClassContents(CLASS_B), tlContainerR->getContainerClassContents(CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        local_ret = sweepRollY_5(tlContainerR->getContainerClassContents(CLASS_D), tlContainerS->getContainerClassContents(CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
                // add results  
                queryOutput.queryResults += localQueryOutput.queryResults;
                queryOutput.postMBRFilterCandidates += localQueryOutput.postMBRFilterCandidates;
                queryOutput.refinementCandidates += localQueryOutput.refinementCandidates;
                for (auto &it : localQueryOutput.topologyRelationsResultMap) {
                    queryOutput.topologyRelationsResultMap[it.first] += it.second;
                }
            }
            // make a copy to store in the global variable
            return ret;
        }
    }

    namespace intersectionMBRfilter
    {
        
        static inline DB_STATUS forwardPair(Shape* objR, Shape* objS, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            // count as MBR filter result
            queryOutput.countMBRresult();
            // check if intermediate filter is enabled
            if (g_config.queryInfo.IntermediateFilter) {
                // forward to intermediate filter
                ret = APRIL::standard::IntermediateFilterEntrypoint(objR, objS, queryOutput);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Intermediate filter failed");
                }
            } else {
                // count refinement candidates
                queryOutput.countRefinementCandidate();
                // forward to refinement
                ret = refinement::relate::refinementEntrypoint(objR, objS, g_config.queryInfo.type, queryOutput);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Refinement failed");
                }
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
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
                    ret = forwardPair(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }
            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {               
                if (((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) || ((*rec)->mbr.pMax.x < (*pivot)->mbr.pMin.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
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
                    ret = forwardPair(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {       
                if ((*pivot)->mbr.pMin.x > (*rec)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_4(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && ((*rec)->mbr.pMax.y >= (*pivot)->mbr.pMin.y)) {                       
                if ((*rec)->mbr.pMin.x > (*pivot)->mbr.pMax.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS internal_sweepRollY_5(std::vector<Shape*>::iterator &rec, std::vector<Shape*>::iterator &firstFS, std::vector<Shape*>::iterator &lastFS, int flag, QueryOutput &queryOutput) {
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
                    ret = forwardPair(*pivot, *rec, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        
        static inline DB_STATUS sweepRollY_1(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
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
                    ret = internal_sweepRollY_1(r, s, lastS, 0, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // internal loop
                    ret = internal_sweepRollY_1(s, r, lastR, 1, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_2(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) {
                ret = internal_sweepRollY_2(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_3(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
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
                    ret = internal_sweepRollY_3_1(r, s, lastS, flag, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // Run internal loop.
                    // warning: dont remove flag^1, it is required to define which is r and which is s
                    ret = internal_sweepRollY_3_2(s, r, lastR, flag^1, queryOutput);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        
        static inline DB_STATUS sweepRollY_4(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_4(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }

            return ret;
        }

        
        inline DB_STATUS sweepRollY_5(std::vector<Shape*>* objectsR, std::vector<Shape*>* objectsS, int flag, QueryOutput &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r != lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_5(r, s, lastS, flag, queryOutput);
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
        
        static DB_STATUS evaluate(QueryOutput &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            Dataset* R = g_config.datasetInfo.getDatasetR();
            Dataset* S = g_config.datasetInfo.getDatasetS();
            // here the final results will be stored
            #pragma omp parallel reduction(query_output_reduction:queryOutput)
            {
                DB_STATUS local_ret = DBERR_OK;
                QueryOutput localQueryOutput;
                // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
                #pragma omp for
                for (int i=0; i<R->twoLayerIndex.partitions.size(); i++) {
                    // get partition ID and S container
                    int partitionID = R->twoLayerIndex.partitions[i].partitionID;
                    Partition* tlContainerS = S->twoLayerIndex.getPartition(partitionID);
                    // if relation S has any objects for this partition (non-empty container)
                    if (tlContainerS != nullptr) {
                        // common partition found
                        Partition* tlContainerR = &R->twoLayerIndex.partitions[i];
                        // R_A - S_A
                        local_ret = sweepRollY_1(tlContainerR->getContainerClassContents(CLASS_A), tlContainerS->getContainerClassContents(CLASS_A), localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        local_ret = sweepRollY_2(tlContainerS->getContainerClassContents(CLASS_B), tlContainerR->getContainerClassContents(CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        local_ret = sweepRollY_3(tlContainerR->getContainerClassContents(CLASS_A), tlContainerS->getContainerClassContents(CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        local_ret = sweepRollY_5(tlContainerS->getContainerClassContents(CLASS_D), tlContainerR->getContainerClassContents(CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        local_ret = sweepRollY_2(tlContainerR->getContainerClassContents(CLASS_B), tlContainerS->getContainerClassContents(CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        local_ret = sweepRollY_4(tlContainerR->getContainerClassContents(CLASS_B), tlContainerS->getContainerClassContents(CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        local_ret = sweepRollY_3(tlContainerS->getContainerClassContents(CLASS_A), tlContainerR->getContainerClassContents(CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_A - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_C
                        local_ret = sweepRollY_4(tlContainerS->getContainerClassContents(CLASS_B), tlContainerR->getContainerClassContents(CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        local_ret = sweepRollY_5(tlContainerR->getContainerClassContents(CLASS_D), tlContainerS->getContainerClassContents(CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                    }
                }
                // add results  
                queryOutput.queryResults += localQueryOutput.queryResults;
                queryOutput.postMBRFilterCandidates += localQueryOutput.postMBRFilterCandidates;
                queryOutput.refinementCandidates += localQueryOutput.refinementCandidates;
                queryOutput.trueHits += localQueryOutput.trueHits;
                queryOutput.trueNegatives += localQueryOutput.trueNegatives;
            }
            // make a copy to store in the global variable
            return ret;
        }
    }

    DB_STATUS processQuery(QueryOutput &queryOutput) {
        DB_STATUS ret = DBERR_OK;
        // first, reset any query outputs
        g_queryOutput.reset();
        // process based on query type
        switch (g_config.queryInfo.type) {
            case Q_RANGE:
                // todo: double check with dimitris. Is the filter the same for range? what about point data?
                ret = intersectionMBRfilter::evaluate(queryOutput);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Intersection MBR filter failed");
                    return ret;
                }
                break;
            case Q_INTERSECT:
            case Q_INSIDE:
            case Q_DISJOINT:
            case Q_EQUAL:
            case Q_MEET:
            case Q_CONTAINS:
            case Q_COVERS:
            case Q_COVERED_BY:
                ret = intersectionMBRfilter::evaluate(queryOutput);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Intersection MBR filter failed");
                    return ret;
                }
                break;
            case Q_FIND_RELATION:
                ret = topologyMBRfilter::evaluate(queryOutput);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Topology MBR filter failed");
                    return ret;
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported:", mapping::queryTypeIntToStr(g_config.queryInfo.type));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }
}