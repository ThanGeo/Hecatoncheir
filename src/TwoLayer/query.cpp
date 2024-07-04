#include "TwoLayer/query.h"


namespace twolayer
{   
    namespace intersectionMBRfilter
    {
        static inline DB_STATUS forwardPair(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS) {
            // printf("%d,%d\n", idR, idS);
            // count as MBR filter result
            spatial_lib::g_queryOutput.countMBRresult();
            // forward to intermediate filter
            DB_STATUS ret = APRIL::standard::IntermediateFilterEntrypoint(polR, polS);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Intermediate filter failed");
            }
            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
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
                    ret = forwardPair(*pivot, *rec);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }

                pivot++;
            }
            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_3_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
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
                    ret = forwardPair(*pivot, *rec);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_3_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {       
                if (pivot->mbr.minPoint.x > rec->mbr.maxPoint.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_4(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            DB_STATUS ret = DBERR_OK;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                       
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    ret = forwardPair(*pivot, *rec);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        static inline DB_STATUS internal_sweepRollY_5(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
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
                    ret = forwardPair(*pivot, *rec);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                } else {
                    // rec is R, pivot is S
                    ret = forwardPair(*rec, *pivot);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                pivot++;
            }

            return ret;
        }

        static inline DB_STATUS sweepRollY_1(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS) {
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
                    ret = internal_sweepRollY_1(r, s, lastS, 0);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // internal loop
                    ret = internal_sweepRollY_1(s, r, lastR, 1);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        static inline DB_STATUS sweepRollY_2(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
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
                ret = internal_sweepRollY_2(r, s, lastS, flag);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }
            return ret;
        }

        static inline DB_STATUS sweepRollY_3(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
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
                    ret = internal_sweepRollY_3_1(r, s, lastS, flag);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    r++;
                } else {
                    // Run internal loop.
                    // warning: dont remove flag^1, it is required to define which is r and which is s
                    ret = internal_sweepRollY_3_2(s, r, lastR, flag^1);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                    s++;
                }
            }
            return ret;
        }

        static inline DB_STATUS sweepRollY_4(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
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
                ret = internal_sweepRollY_4(r, s, lastS, flag);
                if (ret != DBERR_OK) {
                    return ret;
                }
                r++;
            }

            return ret;
        }

        inline DB_STATUS sweepRollY_5(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
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
                ret = internal_sweepRollY_5(r, s, lastS, flag);
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
        static DB_STATUS evaluate() {
            DB_STATUS ret = DBERR_OK;
            spatial_lib::DatasetT* R = g_config.datasetInfo.getDatasetR();
            spatial_lib::DatasetT* S = g_config.datasetInfo.getDatasetS();
            // loop common partitions (todo: optimize to start from the dataset that has the fewer ones)
            for (auto &partitionIT: R->twoLayerIndex.partitionIndex) {
                spatial_lib::TwoLayerContainerT* tlContainerS = S->twoLayerIndex.getPartition(partitionIT.first);
                if (tlContainerS != nullptr) {
                    // common partition found
                    spatial_lib::TwoLayerContainerT* tlContainerR = R->twoLayerIndex.getPartition(partitionIT.first);
                    // R_A - S_A
                    ret = sweepRollY_1(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A));
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "R_A - S_A sweep roll failed");
                        return ret;
                    }
                    // S_B - R_A
                    ret = sweepRollY_2(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "S_B - R_A sweep roll failed");
                        return ret;
                    }
                    // R_A - S_C
                    ret = sweepRollY_3(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "R_A - S_C sweep roll failed");
                        return ret;
                    }
                    // S_D - R_A
                    ret = sweepRollY_5(tlContainerS->getContainerClassContents(spatial_lib::CLASS_D), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "S_D - R_A sweep roll failed");
                        return ret;
                    }
                    // R_B - S_A
                    ret = sweepRollY_2(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "R_B - S_A sweep roll failed");
                        return ret;
                    }
                    // R_B - S_C
                    ret = sweepRollY_4(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "R_B - S_C sweep roll failed");
                        return ret;
                    }
                    // S_A - R_C
                    ret = sweepRollY_3(tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "S_A - R_C sweep roll failed");
                        return ret;
                    }
                    // S_B - R_C
                    ret = sweepRollY_4(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "S_B - R_C sweep roll failed");
                        return ret;
                    }
                    // R_D - S_A
                    ret = sweepRollY_5(tlContainerR->getContainerClassContents(spatial_lib::CLASS_D), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "R_D - S_A sweep roll failed");
                        return ret;
                    }
                }
            }
            return ret;
        }
    }


    DB_STATUS processQuery() {
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
                    ret = intersectionMBRfilter::evaluate();
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Intersection MBR filter failed");
                        return ret;
                    }
                    logger::log_success("MBR results:", spatial_lib::g_queryOutput.postMBRFilterCandidates);
                    logger::log_success("APRIL Result:", (spatial_lib::g_queryOutput.trueHits / (double) spatial_lib::g_queryOutput.postMBRFilterCandidates * 100), (spatial_lib::g_queryOutput.trueNegatives / (double) spatial_lib::g_queryOutput.postMBRFilterCandidates * 100), (spatial_lib::g_queryOutput.refinementCandidates / (double) spatial_lib::g_queryOutput.postMBRFilterCandidates * 100));
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