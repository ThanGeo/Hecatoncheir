#include "APRIL/filter.h"


namespace APRIL
{
    namespace topology
    {
        static DB_STATUS specializedTopologyRinSContainment(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = -1;
            // get common sections
            std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), polR.recID, polS.recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                spatial_lib::AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, polR.recID);
                spatial_lib::AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, polS.recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBRRinSContainmentJoinAPRIL(aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "R in S containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case spatial_lib::TR_INSIDE:
                    case spatial_lib::TR_DISJOINT:
                    case spatial_lib::TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(spatial_lib::INCONCLUSIVE);
            int relation = -1;
            // switch based on result
            switch(iFilterResult) {            
                // inconclusive, do selective refinement
                // result holds what type of refinement needs to be made
                case spatial_lib::REFINE_INSIDE_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = spatial_lib::refineInsideCoveredbyTruehitIntersect(polR, polS);
                    break;
                case spatial_lib::REFINE_DISJOINT_INSIDE_COVEREDBY_MEET_INTERSECT:
                    // refine
                    relation = spatial_lib::refineDisjointInsideCoveredbyMeetIntersect(polR, polS);
                    break;
                default:
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "R in S containment APRIL filter failed with unexpected relation:", relation, spatial_lib::mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
                
            }
            // count the refinement result
            queryOutput.countTopologyRelationResult(relation);
            // if (relation == spatial_lib::TR_MEET) {
            //     printf("%d,%d\n", polR.recID, polS.recID);
            // }
            return ret;
        }

        static DB_STATUS specializedTopologySinRContainment(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = -1;
            // get common sections
            std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), polR.recID, polS.recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                spatial_lib::AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, polR.recID);
                spatial_lib::AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, polS.recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBRSinRContainmentJoinAPRIL(aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "S in R containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case spatial_lib::TR_CONTAINS:
                    case spatial_lib::TR_DISJOINT:
                    case spatial_lib::TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(spatial_lib::INCONCLUSIVE);
            int relation = -1;
            // switch based on result
            switch(iFilterResult) {            
                // inconclusive, do selective refinement
                // result holds what type of refinement needs to be made
                case spatial_lib::REFINE_CONTAINS_COVERS_TRUEHIT_INTERSECT:
                    // refine
                    relation = spatial_lib::refineContainsCoversTruehitIntersect(polR, polS);
                    break;
                case spatial_lib::REFINE_DISJOINT_CONTAINS_COVERS_MEET_INTERSECT:
                    // refine
                    relation = spatial_lib::refineDisjointContainsCoversMeetIntersect(polR, polS);
                    break;
                default:
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "S in R containment APRIL filter failed with unexpected relation:", relation, spatial_lib::mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
                
            }
            // if (relation == spatial_lib::TR_MEET) {
            //     printf("%d,%d\n", polR.recID, polS.recID);
            // }
            // count the refinement result
            queryOutput.countTopologyRelationResult(relation);
            return ret;
        }

        static DB_STATUS specializedTopologyEqual(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = -1;
            // get common sections
            std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), polR.recID, polS.recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                spatial_lib::AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, polR.recID);
                spatial_lib::AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, polS.recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBREqualJoinAPRIL(polR, polS, aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "S in R containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case spatial_lib::TR_DISJOINT:
                    case spatial_lib::TR_COVERED_BY:
                    case spatial_lib::TR_COVERS:
                    case spatial_lib::TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(spatial_lib::INCONCLUSIVE);
            int relation = -1;
            // switch based on result
            switch(iFilterResult) {            
                // inconclusive, do selective refinement
                // result holds what type of refinement needs to be made
                case spatial_lib::REFINE_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = spatial_lib::refineCoveredbyTrueHitIntersect(polR, polS);
                    break;
                case spatial_lib::REFINE_COVERS_TRUEHIT_INTERSECT:
                    // refine
                    relation = spatial_lib::refineCoversTrueHitIntersect(polR, polS);
                    break;
                case spatial_lib::REFINE_COVERS_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = spatial_lib::refineCoversCoveredByTrueHitIntersect(polR, polS);
                    break;
                case spatial_lib::REFINE_EQUAL_COVERS_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = spatial_lib::refineEqualCoversCoveredByTrueHitIntersect(polR, polS);
                    break;
                default:
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "Equal APRIL filter failed with unexpected relation:", relation, spatial_lib::mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
            }
            // count the refinement result
            queryOutput.countTopologyRelationResult(relation);
            return ret;
        }

        static DB_STATUS specializedTopologyIntersection(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = -1;
            // get common sections
            std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), polR.recID, polS.recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                spatial_lib::AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, polR.recID);
                spatial_lib::AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, polS.recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBRIntersectionJoinAPRIL(aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "S in R containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case spatial_lib::TR_DISJOINT:
                    case spatial_lib::TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(spatial_lib::INCONCLUSIVE);
            // refine
            int relation = spatial_lib::refineDisjointMeetIntersect(polR, polS);
            // if (relation == spatial_lib::TR_MEET) {
            //     printf("%d,%d\n", polR.recID, polS.recID);
            // }
            // count the refinement relation result
            queryOutput.countTopologyRelationResult(relation);
            return ret;
        }

        DB_STATUS IntermediateFilterEntrypoint(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::MBRRelationCaseE mbrRelationCase, spatial_lib::QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            // switch based on how the MBRs intersect, to the appropriate intermediate filter
            switch (mbrRelationCase) {
                case spatial_lib::MBR_R_IN_S:
                    ret = specializedTopologyRinSContainment(polR, polS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology R in S containment filter failed.");
                    }
                    break;
                case spatial_lib::MBR_S_IN_R:
                    ret = specializedTopologySinRContainment(polR, polS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                case spatial_lib::MBR_EQUAL:
                    ret = specializedTopologyEqual(polR, polS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                case spatial_lib::MBR_INTERSECT:
                    ret = specializedTopologyIntersection(polR, polS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                default:
                    // not supported/unknown
                    logger::log_error(DBERR_INVALID_PARAMETER, "Unknown MBR relation case:", mbrRelationCase);
                    return DBERR_INVALID_PARAMETER;
            }
            return ret;
        }
    }

    namespace standard
    {
        DB_STATUS IntermediateFilterEntrypoint(spatial_lib::PolygonT &polR, spatial_lib::PolygonT &polS, spatial_lib::QueryOutputT &queryOutput) {
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
                    // count APRIL result
                    queryOutput.countAPRILresult(iFilterResult);
                    return ret;
                }
            }
            // count refinement candidate (inconclusive)
            queryOutput.countAPRILresult(spatial_lib::INCONCLUSIVE);

            // refine based on query type
            switch (g_config.queryInfo.type) {
                case spatial_lib::Q_INTERSECT:
                    spatial_lib::refineIntersectionJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_COVERED_BY:
                    spatial_lib::refineCoveredByJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_INSIDE:
                    spatial_lib::refineInsideJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_DISJOINT:
                    spatial_lib::refineDisjointJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_EQUAL:
                    spatial_lib::refineEqualJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_MEET:
                    spatial_lib::refineMeetJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_CONTAINS:
                    spatial_lib::refineContainsJoin(polR, polS, queryOutput);
                    break;
                case spatial_lib::Q_COVERS:
                    spatial_lib::refineCoversJoin(polR, polS, queryOutput);
                    break;
                default:
                    // not supported/unknown
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query for refinement. Query type:", spatial_lib::mapping::queryTypeIntToStr(g_config.queryInfo.type));
                    return DBERR_QUERY_INVALID_TYPE;
            }
            return ret;
        }
    }
}