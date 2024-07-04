#include "TwoLayer/query.h"


namespace twolayer
{   
    namespace intersection_with_intermediate_forwarding
    {
        static void forwardPair(int idR, int idS) {
            // printf("%d,%d\n", idR, idS);

        }

        static inline unsigned long long internal_sweepRollY_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            unsigned long long result = 0;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                // disjoint, skip
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    // todo: forward pair
                    forwardPair(pivot->recID, rec->recID);
                } else {
                    // rec is R, pivot is S
                    // todo: forward pair
                    forwardPair(rec->recID, pivot->recID);
                }

                result++;
                pivot++;
            }
            return result;
        }

        static inline unsigned long long internal_sweepRollY_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            unsigned long long result = 0;
            auto pivot = firstFS;

            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {               
                if ((rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) || (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x)) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    // todo: forward pair
                    forwardPair(pivot->recID, rec->recID);
                } else {
                    // rec is R, pivot is S
                    // todo: forward pair
                    forwardPair(rec->recID, pivot->recID);
                }
                result++;
                pivot++;
            }

            return result;
        }

        inline unsigned long long internal_sweepRollY_3_1(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            // printf("internal_sweepRollY_3_1\n");
            unsigned long long result = 0;
            auto pivot = firstFS;

            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
                    pivot++;
                    continue;
                }

                // forward pair
                if (flag) {
                    // pivot is R, rec is S
                    // todo: forward pair
                    forwardPair(pivot->recID, rec->recID);
                } else {
                    // rec is R, pivot is S
                    // todo: forward pair
                    forwardPair(rec->recID, pivot->recID);
                }
                result++;
                pivot++;
            }

            return result;
        }

        static inline unsigned long long internal_sweepRollY_3_2(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            unsigned long long result = 0;
            auto pivot = firstFS;

            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {       
                if (pivot->mbr.minPoint.x > rec->mbr.maxPoint.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    // todo: forward pair
                    forwardPair(pivot->recID, rec->recID);
                } else {
                    // rec is R, pivot is S
                    // todo: forward pair
                    forwardPair(rec->recID, pivot->recID);
                }
                result++;
                pivot++;
            }

            return result;
        }

        static inline unsigned long long internal_sweepRollY_4(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            unsigned long long result = 0;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                       
                if (rec->mbr.minPoint.x > pivot->mbr.maxPoint.x) {
                    pivot++;
                    continue;
                }
                if (flag) {
                    // pivot is R, rec is S
                    // todo: forward pair
                    forwardPair(pivot->recID, rec->recID);
                } else {
                    // rec is R, pivot is S
                    // todo: forward pair
                    forwardPair(rec->recID, pivot->recID);
                }
                result++;
                pivot++;
            }

            return result;
        }

        static inline unsigned long long internal_sweepRollY_5(std::vector<spatial_lib::PolygonT>::iterator &rec, std::vector<spatial_lib::PolygonT>::iterator &firstFS, std::vector<spatial_lib::PolygonT>::iterator &lastFS, int flag) {
            unsigned long long result = 0;
            auto pivot = firstFS;
            while ((pivot < lastFS) && (rec->mbr.maxPoint.y >= pivot->mbr.minPoint.y)) {                
                if (rec->mbr.maxPoint.x < pivot->mbr.minPoint.x) {
                    pivot++;
                    continue;
                }
                // forward pair
                if (flag) {
                    // pivot is R, rec is S
                    // todo: forward pair
                    forwardPair(pivot->recID, rec->recID);
                } else {
                    // rec is R, pivot is S
                    // todo: forward pair
                    forwardPair(rec->recID, pivot->recID);
                }
                result++;
                pivot++;

            }

            return result;
        }

        static inline unsigned long long sweepRollY_1(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return 0;
            }
            unsigned long long result = 0;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR) && (s < lastS)) {
                if (r->mbr.minPoint.y < s->mbr.minPoint.y) {
                    // internal loop
                    result += internal_sweepRollY_1(r, s, lastS, 0);
                    r++;
                } else {
                    // internal loop
                    result += internal_sweepRollY_1(s, r, lastR, 1);
                    s++;
                }
            }
            return result;
        }

        static inline unsigned long long sweepRollY_2(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return 0;
            }
            unsigned long long result = 0;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR))
            {
                result += internal_sweepRollY_2(r, s, lastS, flag);
                r++;
            }
            return result;
        }

        static inline unsigned long long sweepRollY_3(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return 0;
            }
            unsigned long long result = 0;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();

            while ((r < lastR) && (s < lastS)) {
                if (r->mbr.minPoint.y < s->mbr.minPoint.y) {
                    // Run internal loop.
                    result += internal_sweepRollY_3_1(r, s, lastS, flag);
                    r++;
                } else {
                    // Run internal loop.
                    // warning: dont remove flag^1, it is required to define which is r and which is s
                    result += internal_sweepRollY_3_2(s, r, lastR, flag^1);
                    s++;
                }
            }
            
            return result;
        }

        static inline unsigned long long sweepRollY_4(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return 0;
            }
            unsigned long long result = 0;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) { 
                // Run internal loop.
                result += internal_sweepRollY_4(r, s, lastS, flag);
                r++;
            }

            return result;
        }

        inline unsigned long long sweepRollY_5(std::vector<spatial_lib::PolygonT>* objectsR, std::vector<spatial_lib::PolygonT>* objectsS, int flag) {
            if (objectsR == nullptr || objectsS == nullptr) {
                return 0;
            }
            unsigned long long result = 0;
            auto r = objectsR->begin();
            auto s = objectsS->begin();
            auto lastR = objectsR->end();
            auto lastS = objectsS->end();
            while ((r < lastR)) { 
                // Run internal loop.
                result += internal_sweepRollY_5(r, s, lastS, flag);
                r++;
            }
            return result;
        }

        /**
         * simple two-layer MBR intersection filter with intermediate filter forwarding
         */
        static DB_STATUS evaluate(unsigned long long &result) {
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
                    result += sweepRollY_1(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A));
                    // S_B - R_A
                    result += sweepRollY_2(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1);
                    // R_A - S_C
                    result += sweepRollY_3(tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0);
                    // S_D - R_A
                    result += sweepRollY_5(tlContainerS->getContainerClassContents(spatial_lib::CLASS_D), tlContainerR->getContainerClassContents(spatial_lib::CLASS_A), 1);
                    // R_B - S_A
                    result += sweepRollY_2(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0);
                    // R_B - S_C
                    result += sweepRollY_4(tlContainerR->getContainerClassContents(spatial_lib::CLASS_B), tlContainerS->getContainerClassContents(spatial_lib::CLASS_C), 0);
                    // S_A - R_C
                    result += sweepRollY_3(tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1);
                    // S_B - R_C
                    result += sweepRollY_4(tlContainerS->getContainerClassContents(spatial_lib::CLASS_B), tlContainerR->getContainerClassContents(spatial_lib::CLASS_C), 1);
                    // R_D - S_A
                    result += sweepRollY_5(tlContainerR->getContainerClassContents(spatial_lib::CLASS_D), tlContainerS->getContainerClassContents(spatial_lib::CLASS_A), 0);
                }
            }
            return ret;
        }
    }


    DB_STATUS processQuery(unsigned long long &result) {
        DB_STATUS ret = DBERR_OK;
        switch (g_config.queryInfo.type) {
            case spatial_lib::Q_INTERSECT:
                if (g_config.queryInfo.IntermediateFilter) {
                    // APRIL filter enabled
                    ret = intersection_with_intermediate_forwarding::evaluate(result);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Intersection MBR filter failed");
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