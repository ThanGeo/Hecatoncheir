#include "APRIL/filter.h"


namespace APRIL
{
    namespace standard
    {
        DB_STATUS IntermediateFilterEntrypoint(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS) {
            DB_STATUS ret = DBERR_OK;
            // get common sections
            std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), polR.recID, polS.recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                spatial_lib::AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, polR.recID);
                spatial_lib::AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, polS.recID);

                int iFilterResult = -1;
                // use appropriate query function
                switch (g_config.queryInfo.type) {
                    case spatial_lib::Q_INTERSECT:
                        ret = uncompressed::intersectionJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL intersection join failed for pair", polR.recID, "and", polS.recID);
                            return ret;
                        }
                        break;
                    case spatial_lib::Q_COVERED_BY:   
                    case spatial_lib::Q_INSIDE:
                        ret = uncompressed::insideCoveredByJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL inside/covered by join failed for pair", polR.recID, "and", polS.recID);
                            return ret;
                        }
                        break;
                    case spatial_lib::Q_DISJOINT:
                        ret = uncompressed::disjointJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL disjoint join failed for pair", polR.recID, "and", polS.recID);
                            return ret;
                        }
                        break;
                    case spatial_lib::Q_EQUAL:
                        ret = uncompressed::equalJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL equality join failed for pair", polR.recID, "and", polS.recID);
                            return ret;
                        }
                        break;
                    case spatial_lib::Q_MEET:
                        ret = uncompressed::meetJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL meet join failed for pair", polR.recID, "and", polS.recID);
                            return ret;
                        }
                        break;
                    case spatial_lib::Q_CONTAINS:
                    case spatial_lib::Q_COVERS:
                        ret = uncompressed::containsCoversJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL contains/covers join failed for pair", polR.recID, "and", polS.recID);
                            return ret;
                        }
                        break;
                    default:
                        // not supported/unknown
                        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query for standard APRIL intermediate filter. Query type:", spatial_lib::mapping::queryTypeIntToStr(g_config.queryInfo.type));
                        return DBERR_QUERY_INVALID_TYPE;
                }

                // if true negative or true hit, return
                if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                    // measure time
                    // spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                    // count result
                    spatial_lib::g_queryOutput.countAPRILresult(iFilterResult);
                    return ret;
                }
            }
            // i filter ended, measure time
            // spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
            // count refinement candidate (inconclusive)
            spatial_lib::g_queryOutput.countAPRILresult(spatial_lib::INCONCLUSIVE);
            
            // todo: send to refinement
            // spatial_lib::time::markRefinementFilterTimer();
            // spatial_lib::refineIntersectionJoin(idR, idS);
            // spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);

            return ret;
        }
    }
}