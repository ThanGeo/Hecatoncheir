#include "TwoLayer/filter.h"


namespace twolayer
{   
    namespace topologyMBRfilterWithForwarding
    {
        #define EPS 1e-08

        static inline DB_STATUS forwardPair(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::MBRRelationCaseE mbrRelationCase, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            if (mbrRelationCase != spatial_lib::MBR_CROSS) {
                // count as MBR filter result
                queryOutput.countMBRresult();
                // forward to intermediate filter
                ret = APRIL::topology::IntermediateFilterEntrypoint(polR, polS, mbrRelationCase, queryOutput);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Topology intermediate filter failed.");
                    return ret;
                }
            } else {
                // they cross, true hit intersect (skip intermediate filter)
                queryOutput.countTopologyRelationResult(spatial_lib::TR_INTERSECT);
            }
            return ret;
        }

        static inline DB_STATUS relateMBRs(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            // compute deltas
            double d_xmin = polR.mbr.minPoint.x - polS.mbr.minPoint.x;
            double d_ymin = polR.mbr.minPoint.y - polS.mbr.minPoint.y;
            double d_xmax = polR.mbr.maxPoint.x - polS.mbr.maxPoint.x;
            double d_ymax = polR.mbr.maxPoint.y - polS.mbr.maxPoint.y;

            // check for equality using an error margin because doubles
            if (abs(d_xmin) < EPS) {
                if (abs(d_xmax) < EPS) {
                    if (abs(d_ymin) < EPS) {
                        if (abs(d_ymax) < EPS) {
                            // equal MBRs
                            ret = forwardPair(polR, polS, spatial_lib::MBR_EQUAL, queryOutput);
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
                            ret = forwardPair(polR, polS, spatial_lib::MBR_S_IN_R, queryOutput);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBR(s) inside MBR(r) stopped with error.");
                            }
                            return ret;
                        }
                    } else {
                        if (d_ymax < 0 && d_xmax > 0 && d_xmin < 0 && d_ymin < 0) {
                            // MBRs cross each other
                            ret = forwardPair(polR, polS, spatial_lib::MBR_CROSS, queryOutput);
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
                            ret = forwardPair(polR, polS, spatial_lib::MBR_R_IN_S, queryOutput);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBR(r) inside MBR(s) stopped with error.");
                            }
                            return ret;
                        }
                    } else {
                        if (d_ymax > 0 && d_xmax < 0 && d_xmin > 0 && d_ymin > 0) {
                            // MBRs cross each other
                            ret = forwardPair(polR, polS, spatial_lib::MBR_CROSS, queryOutput);
                            if (ret != DBERR_OK) {
                                logger::log_error(ret, "Forward for MBRs cross stopped with error.");
                            }
                            return ret;
                        }
                    }
                }
            }
            // MBRs intersect generally
            ret = forwardPair(polR, polS, spatial_lib::MBR_INTERSECT, queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Forward for MBRs intersect stopped with error.");
            }
            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                // disjoint, skip
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
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

        static inline DB_STATUS internal_sweepRollY_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
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

        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
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

        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {       
                if (pivot->mbr.minPoint.x > rec->mbr.maxPoint.x) {
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

        static inline DB_STATUS internal_sweepRollY_4(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                       
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
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

        static inline DB_STATUS internal_sweepRollY_5(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                
                if (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x) {
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

        static inline DB_STATUS sweepRollY_1(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR) && (s < lastS)) {
                if (r->mbr.minPoint.y < s->mbr.minPoint.y) {
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

        static inline DB_STATUS sweepRollY_2(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR))
            {
                ret = internal_sweepRollY_2(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        static inline DB_STATUS sweepRollY_3(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR) && (s < lastS)) {
                if (r->mbr.minPoint.y < s->mbr.minPoint.y) {
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

        static inline DB_STATUS sweepRollY_4(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_4(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }

            return ret;
        }

        inline DB_STATUS sweepRollY_5(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) { 
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
        static DB_STATUS evaluate(spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            spatial_lib::DatasetT* R = g_config.datasetInfo.getDatasetR();
            spatial_lib::DatasetT* S = g_config.datasetInfo.getDatasetS();
            // here the final results will be stored
            #pragma omp parallel reduction(query_output_reduction:queryOutput)
            {
                DB_STATUS local_ret = DBERR_OK;
                spatial_lib::QueryOutputT localQueryOutput;
                // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
                #pragma omp for
                for (int i=0; i<R->twoLayerIndex.partitionIDs.size(); i++) {
                    // get partition ID and S container
                    int partitionID = R->twoLayerIndex.partitionIDs[i];
                    spatial_lib::TwoLayerContainerT* tlContainerS = S->twoLayerIndex.getPartition(partitionID);
                    // if relation S has any objects for this partition (non-empty container)
                    if (tlContainerS != nullptr) {
                        // common partition found
                        spatial_lib::TwoLayerContainerT* tlContainerR = R->twoLayerIndex.getPartition(partitionID);
                        // R_A - S_A
                        local_ret = sweepRollY_1(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        local_ret = sweepRollY_2(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        local_ret = sweepRollY_3(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        local_ret = sweepRollY_5(tlContainerS->getContainerClassContents(spatial_lib::CLASS_D), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        local_ret = sweepRollY_2(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        local_ret = sweepRollY_4(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        local_ret = sweepRollY_3(tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_A - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_C
                        local_ret = sweepRollY_4(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        local_ret = sweepRollY_5(tlContainerR->getContainerClassContents(spatial_lib::CLASS_D), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // logger::log_success("thread", omp_get_thread_num(), "partition", partitionID, "results:", spatial_lib::g_queryOutput.queryResults, "MBR results:", spatial_lib::g_queryOutput.postMBRFilterCandidates);
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

    namespace intersectionMBRfilterWithForwarding
    {
        static inline DB_STATUS forwardPair(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
            // count as MBR filter result
            queryOutput.countMBRresult();
            // forward to intermediate filter
            DB_STATUS ret = APRIL::standard::IntermediateFilterEntrypoint(polR, polS, queryOutput);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Intermediate filter failed");
            }
            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                // disjoint, skip
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
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

        static inline DB_STATUS internal_sweepRollY_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
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

        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
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

        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {       
                if (pivot->mbr.minPoint.x > rec->mbr.maxPoint.x) {
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

        static inline DB_STATUS internal_sweepRollY_4(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                       
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
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

        static inline DB_STATUS internal_sweepRollY_5(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                
                if (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x) {
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

        static inline DB_STATUS sweepRollY_1(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR) && (s < lastS)) {
                if (r->mbr.minPoint.y < s->mbr.minPoint.y) {
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

        static inline DB_STATUS sweepRollY_2(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) {
                ret = internal_sweepRollY_2(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        static inline DB_STATUS sweepRollY_3(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR) && (s < lastS)) {
                if (r->mbr.minPoint.y < s->mbr.minPoint.y) {
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

        static inline DB_STATUS sweepRollY_4(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) { 
                // Run internal loop.
                ret = internal_sweepRollY_4(r, s, lastS, flag, queryOutput);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }

            return ret;
        }

        inline DB_STATUS sweepRollY_5(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag, spatial_lib::QueryOutputT &queryOutput) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return DBERR_OK;
            }
            DB_STATUS ret = DBERR_OK;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) { 
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
        static DB_STATUS evaluate(spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            spatial_lib::DatasetT* R = g_config.datasetInfo.getDatasetR();
            spatial_lib::DatasetT* S = g_config.datasetInfo.getDatasetS();
            // here the final results will be stored
            #pragma omp parallel reduction(query_output_reduction:queryOutput)
            {
                DB_STATUS local_ret = DBERR_OK;
                spatial_lib::QueryOutputT localQueryOutput;
                // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
                #pragma omp for
                for (int i=0; i<R->twoLayerIndex.partitionIDs.size(); i++) {
                    // get partition ID and S container
                    int partitionID = R->twoLayerIndex.partitionIDs[i];
                    spatial_lib::TwoLayerContainerT* tlContainerS = S->twoLayerIndex.getPartition(partitionID);
                    // if relation S has any objects for this partition (non-empty container)
                    if (tlContainerS != nullptr) {
                        // common partition found
                        spatial_lib::TwoLayerContainerT* tlContainerR = R->twoLayerIndex.getPartition(partitionID);
                        // R_A - S_A
                        local_ret = sweepRollY_1(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_A
                        local_ret = sweepRollY_2(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_A - S_C
                        local_ret = sweepRollY_3(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_A - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_D - R_A
                        local_ret = sweepRollY_5(tlContainerS->getContainerClassContents(spatial_lib::CLASS_D), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_D - R_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_A
                        local_ret = sweepRollY_2(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_B - S_C
                        local_ret = sweepRollY_4(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_B - S_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_A - R_C
                        local_ret = sweepRollY_3(tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_A - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // S_B - R_C
                        local_ret = sweepRollY_4(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "S_B - R_C sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // R_D - S_A
                        local_ret = sweepRollY_5(tlContainerR->getContainerClassContents(spatial_lib::CLASS_D), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0, localQueryOutput);
                        if (local_ret != DBERR_OK) {
                            logger::log_error(ret, "R_D - S_A sweep roll failed");
                            #pragma omp cancel for
                            ret = local_ret;
                        }
                        // logger::log_success("thread", omp_get_thread_num(), "partition", partitionID, "results:", spatial_lib::g_queryOutput.queryResults, "MBR results:", spatial_lib::g_queryOutput.postMBRFilterCandidates);
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


    DB_STATUS processQuery(spatial_lib::QueryOutputT &queryOutput) {
        DB_STATUS ret = DBERR_OK;
        // first, reset any query outputs
        spatial_lib::g_queryOutput.reset();

        // process based on query type
        switch (g_config.queryInfo.type) {
            case spatial_lib::Q_INTERSECT:
            case spatial_lib::Q_INSIDE:
            case spatial_lib::Q_DISJOINT:
            case spatial_lib::Q_EQUAL:
            case spatial_lib::Q_MEET:
            case spatial_lib::Q_CONTAINS:
            case spatial_lib::Q_COVERS:
            case spatial_lib::Q_COVERED_BY:
                if (g_config.queryInfo.IntermediateFilter) {
                    // APRIL filter enabled
                    ret = intersectionMBRfilterWithForwarding::evaluate(queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Intersection MBR filter failed");
                        return ret;
                    }
                } else {
                    // todo: APRIL filter disabled
                }
                break;
            case spatial_lib::Q_FIND_RELATION:
                if (g_config.queryInfo.IntermediateFilter) {
                    // APRIL filter enabled
                    ret = topologyMBRfilterWithForwarding::evaluate(queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Topology MBR filter failed");
                        return ret;
                    }
                } else {
                    // todo: APRIL filter disabled
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported:", spatial_lib::mapping::queryTypeIntToStr(g_config.queryInfo.type));
                break;
        }

        return ret;
    }
}