#include "APRIL/filter.h"


namespace APRIL
{
    namespace topology
    {
        
        static DB_STATUS specializedTopologyRinSContainment(Shape* objR, Shape* objS, QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // get common sections
            std::vector<int> commonSections = getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), objR->recID, objS->recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, objR->recID);
                AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, objS->recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBRRinSContainmentJoinAPRIL(aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "R in S containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case TR_INSIDE:
                    case TR_DISJOINT:
                    case TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(INCONCLUSIVE);
            int relation = -1;
            // switch based on result
            switch(iFilterResult) {            
                // inconclusive, do selective refinement
                // result holds what type of refinement needs to be made
                case REFINE_INSIDE_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = refinement::topology::refineInsideCoveredbyTruehitIntersect(objR, objS);
                    break;
                case REFINE_DISJOINT_INSIDE_COVEREDBY_MEET_INTERSECT:
                    // refine
                    relation = refinement::topology::refineDisjointInsideCoveredbyMeetIntersect(objR, objS);
                    break;
                default:
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "R in S containment APRIL filter failed with unexpected relation:", relation, mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
                
            }
            // count the refinement result
            queryOutput.countTopologyRelationResult(relation);
            // if (relation == TR_MEET) {
            //     printf("%d,%d\n", objR->recID, objS->recID);
            // }
            return ret;
        }

        
        static DB_STATUS specializedTopologySinRContainment(Shape* objR, Shape* objS, QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // get common sections
            std::vector<int> commonSections = getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), objR->recID, objS->recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, objR->recID);
                AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, objS->recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBRSinRContainmentJoinAPRIL(aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "S in R containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case TR_CONTAINS:
                    case TR_DISJOINT:
                    case TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(INCONCLUSIVE);
            int relation = -1;
            // switch based on result
            switch(iFilterResult) {            
                // inconclusive, do selective refinement
                // result holds what type of refinement needs to be made
                case REFINE_CONTAINS_COVERS_TRUEHIT_INTERSECT:
                    // refine
                    relation = refinement::topology::refineContainsCoversTruehitIntersect(objR, objS);
                    break;
                case REFINE_DISJOINT_CONTAINS_COVERS_MEET_INTERSECT:
                    // refine
                    relation = refinement::topology::refineDisjointContainsCoversMeetIntersect(objR, objS);
                    break;
                default:
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "S in R containment APRIL filter failed with unexpected relation:", relation, mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
                
            }
            // if (relation == TR_MEET) {
            //     printf("%d,%d\n", objR->recID, objS->recID);
            // }
            // count the refinement result
            queryOutput.countTopologyRelationResult(relation);
            return ret;
        }

        
        static DB_STATUS specializedTopologyEqual(Shape* objR, Shape* objS, QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // get common sections
            std::vector<int> commonSections = getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), objR->recID, objS->recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, objR->recID);
                AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, objS->recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBREqualJoinAPRIL(objR, objS, aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "S in R containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case TR_DISJOINT:
                    case TR_COVERED_BY:
                    case TR_COVERS:
                    case TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(INCONCLUSIVE);
            int relation = -1;
            // switch based on result
            switch(iFilterResult) {            
                // inconclusive, do selective refinement
                // result holds what type of refinement needs to be made
                case REFINE_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = refinement::topology::refineCoveredbyTrueHitIntersect(objR, objS);
                    break;
                case REFINE_COVERS_TRUEHIT_INTERSECT:
                    // refine
                    relation = refinement::topology::refineCoversTrueHitIntersect(objR, objS);
                    break;
                case REFINE_COVERS_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation =refinement::topology::refineCoversCoveredByTrueHitIntersect(objR, objS);
                    break;
                case REFINE_EQUAL_COVERS_COVEREDBY_TRUEHIT_INTERSECT:
                    // refine
                    relation = refinement::topology::refineEqualCoversCoveredByTrueHitIntersect(objR, objS);
                    break;
                default:
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "Equal APRIL filter failed with unexpected relation:", relation, mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
            }
            // count the refinement result
            queryOutput.countTopologyRelationResult(relation);
            return ret;
        }

        
        static DB_STATUS specializedTopologyIntersection(Shape* objR, Shape* objS, QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // get common sections
            std::vector<int> commonSections = getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), objR->recID, objS->recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, objR->recID);
                AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, objS->recID);
                // use APRIL intermediate filter
                ret = uncompressed::topology::MBRIntersectionJoinAPRIL(aprilR, aprilS, iFilterResult);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "S in R containment APRIL filter failed.");
                    return ret;
                }
                // switch based on result
                switch(iFilterResult) {
                    // true hits, count and return
                    case TR_DISJOINT:
                    case TR_INTERSECT:
                        // result
                        queryOutput.countTopologyRelationResult(iFilterResult);
                        return ret;
                }
            }
            // count refinement candidate
            queryOutput.countAPRILresult(INCONCLUSIVE);
            // refine
            int relation = refinement::topology::refineDisjointMeetIntersect(objR, objS);
            // if (relation == TR_MEET) {
            //     printf("%d,%d\n", objR->recID, objS->recID);
            // }
            // count the refinement relation result
            queryOutput.countTopologyRelationResult(relation);
            return ret;
        }

        
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, MBRRelationCaseE mbrRelationCase, QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            // switch based on how the MBRs intersect, to the appropriate intermediate filter
            switch (mbrRelationCase) {
                case MBR_R_IN_S:
                    ret = specializedTopologyRinSContainment(objR, objS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology R in S containment filter failed.");
                    }
                    break;
                case MBR_S_IN_R:
                    ret = specializedTopologySinRContainment(objR, objS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                case MBR_EQUAL:
                    ret = specializedTopologyEqual(objR, objS, queryOutput);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                case MBR_INTERSECT:
                    ret = specializedTopologyIntersection(objR, objS, queryOutput);
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
        
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, QueryOutputT &queryOutput) {
            DB_STATUS ret = DBERR_OK;
            // get common sections
            std::vector<int> commonSections = getCommonSectionIDsOfObjects(g_config.datasetInfo.getDatasetR(), g_config.datasetInfo.getDatasetS(), objR->recID, objS->recID);
            // for each common section
            for (auto &sectionID : commonSections) {
                // fetch the APRIL of R and S for this section
                AprilDataT* aprilR = g_config.datasetInfo.getDatasetR()->getAprilDataBySectionAndObjectID(sectionID, objR->recID);
                AprilDataT* aprilS = g_config.datasetInfo.getDatasetS()->getAprilDataBySectionAndObjectID(sectionID, objS->recID);
                int iFilterResult = INCONCLUSIVE;
                // use appropriate query function
                switch (g_config.queryInfo.type) {
                    case Q_INTERSECT:
                        ret = uncompressed::intersectionJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL intersection join failed for pair", objR->recID, "and", objS->recID);
                            return ret;
                        }
                        break;
                    case Q_COVERED_BY:   
                    case Q_INSIDE:
                        ret = uncompressed::insideCoveredByJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL inside/covered by join failed for pair", objR->recID, "and", objS->recID);
                            return ret;
                        }
                        break;
                    case Q_DISJOINT:
                        ret = uncompressed::disjointJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL disjoint join failed for pair", objR->recID, "and", objS->recID);
                            return ret;
                        }
                        break;
                    case Q_EQUAL:
                        ret = uncompressed::equalJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL equality join failed for pair", objR->recID, "and", objS->recID);
                            return ret;
                        }
                        break;
                    case Q_MEET:
                        ret = uncompressed::meetJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL meet join failed for pair", objR->recID, "and", objS->recID);
                            return ret;
                        }
                        break;
                    case Q_CONTAINS:
                    case Q_COVERS:
                        ret = uncompressed::containsCoversJoinAPRIL(aprilR, aprilS, iFilterResult);
                        if (ret != DBERR_OK) {
                            logger::log_error(ret, "APRIL contains/covers join failed for pair", objR->recID, "and", objS->recID);
                            return ret;
                        }
                        break;
                    default:
                        // not supported/unknown
                        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query for standard APRIL intermediate filter. Query type:", mapping::queryTypeIntToStr(g_config.queryInfo.type));
                        return DBERR_QUERY_INVALID_TYPE;
                }
                // if true negative or true hit, return
                if (iFilterResult != INCONCLUSIVE) {
                    // count APRIL result
                    queryOutput.countAPRILresult(iFilterResult);
                    // if (iFilterResult == TRUE_HIT) {
                    //     printf("%d,%d\n", objR->recID, objS->recID);
                    // }
                    return ret;
                }
            }
            // count refinement candidate (inconclusive)
            queryOutput.countAPRILresult(INCONCLUSIVE);

            // refine based on query type
            switch (g_config.queryInfo.type) {
                case Q_INTERSECT:
                    refinement::relate::refineIntersectionJoin(objR, objS, queryOutput);
                    break;
                case Q_COVERED_BY:
                    refinement::relate::refineCoveredByJoin(objR, objS, queryOutput);
                    break;
                case Q_INSIDE:
                    refinement::relate::refineInsideJoin(objR, objS, queryOutput);
                    break;
                case Q_DISJOINT:
                    refinement::relate::refineDisjointJoin(objR, objS, queryOutput);
                    break;
                case Q_EQUAL:
                    refinement::relate::refineEqualJoin(objR, objS, queryOutput);
                    break;
                case Q_MEET:
                    refinement::relate::refineMeetJoin(objR, objS, queryOutput);
                    break;
                case Q_CONTAINS:
                    refinement::relate::refineContainsJoin(objR, objS, queryOutput);
                    break;
                case Q_COVERS:
                    refinement::relate::refineCoversJoin(objR, objS, queryOutput);
                    break;
                default:
                    // not supported/unknown
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query for refinement. Query type:", mapping::queryTypeIntToStr(g_config.queryInfo.type));
                    return DBERR_QUERY_INVALID_TYPE;
            }
            return ret;
        }
    }
}