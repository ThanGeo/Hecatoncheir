#include "APRIL/filter.h"
#include "APRIL/generate.h"


namespace APRIL
{
    std::vector<int> getCommonSectionIDsOfObjects(Dataset *datasetR, Dataset *datasetS, size_t idR, size_t idS) {
        auto itR = datasetR->recToSectionIdMap.find(idR);
        auto itS = datasetS->recToSectionIdMap.find(idS);
        std::vector<int> commonSectionIDs;
        set_intersection(itR->second.begin(),itR->second.end(),itS->second.begin(),itS->second.end(),back_inserter(commonSectionIDs));
        return commonSectionIDs;
    }

    namespace topology
    {
        
        static DB_STATUS specializedTopologyRinSContainment(Shape* objR, Shape* objS, hec::QResultBase*  queryResult) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // use APRIL intermediate filter
            ret = uncompressed::topology::MBRRinSContainmentJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
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
                    queryResult->addResult(iFilterResult, objR->recID, objS->recID);
                    return ret;
            }
            // count refinement candidate
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
            queryResult->addResult(relation, objR->recID, objS->recID);

            return ret;
        }

        
        static DB_STATUS specializedTopologySinRContainment(Shape* objR, Shape* objS, hec::QResultBase*  queryResult) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // use APRIL intermediate filter
            ret = uncompressed::topology::MBRSinRContainmentJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
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
                    queryResult->addResult(iFilterResult, objR->recID, objS->recID);
                    return ret;
            }
            // count refinement candidate
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
            
            // count the refinement result
            queryResult->addResult(relation, objR->recID, objS->recID);

            return ret;
        }

        
        static DB_STATUS specializedTopologyEqual(Shape* objR, Shape* objS, hec::QResultBase*  queryResult) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // use APRIL intermediate filter
            ret = uncompressed::topology::MBREqualJoinAPRIL(objR, objS, iFilterResult);
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
                case TR_MEET:
                    // result
                    queryResult->addResult(iFilterResult, objR->recID, objS->recID);
                    return ret;
            }
            // count refinement candidate
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
                    logger::log_error(DBERR_APRIL_UNEXPECTED_RESULT, "Equal APRIL filter failed with unexpected relation:", iFilterResult, mapping::relationIntToStr(relation));
                    return DBERR_APRIL_UNEXPECTED_RESULT;
            }
            // count the refinement result
            queryResult->addResult(relation, objR->recID, objS->recID);
            return ret;
        }

        
        static DB_STATUS specializedTopologyIntersection(Shape* objR, Shape* objS, hec::QResultBase*  queryResult) {
            DB_STATUS ret = DBERR_OK;
            int iFilterResult = INCONCLUSIVE;
            // use APRIL intermediate filter
            ret = uncompressed::topology::MBRIntersectionJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
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
                    queryResult->addResult(iFilterResult, objR->recID, objS->recID);
                    return ret;
            }
            // count refinement candidate
            // refine
            int relation = refinement::topology::refineDisjointMeetIntersect(objR, objS);
            // count the refinement relation result
            queryResult->addResult(relation, objR->recID, objS->recID);
            return ret;
        }

        
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, MBRRelationCase mbrRelationCase, hec::QResultBase*  queryResult) {
            DB_STATUS ret = DBERR_OK;
            // switch based on how the MBRs intersect, to the appropriate intermediate filter
            switch (mbrRelationCase) {
                case MBR_R_IN_S:
                    ret = specializedTopologyRinSContainment(objR, objS, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology R in S containment filter failed.");
                    }
                    break;
                case MBR_S_IN_R:
                    ret = specializedTopologySinRContainment(objR, objS, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                case MBR_EQUAL:
                    ret = specializedTopologyEqual(objR, objS, queryResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "specialized topology S in R containment filter failed.");
                    }
                    break;
                case MBR_INTERSECT:
                    ret = specializedTopologyIntersection(objR, objS, queryResult);
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
        
        DB_STATUS IntermediateFilterEntrypoint(Shape* objR, Shape* objS, hec::QResultBase*  queryResult) {
            DB_STATUS ret = DBERR_OK;
            // fetch the APRIL of R and S for this section
            int iFilterResult = INCONCLUSIVE;
            // use appropriate query function
            switch (g_config.queryPipeline.queryType) {
                case hec::Q_RANGE:
                    ret = APRIL::generation::memory::createAPRILforObject(objS, objS->getSpatialType(), g_config.approximationMetadata.aprilConfig, objS->aprilData);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to create APRIL for range query.");
                        return ret;
                    }
                    ret = uncompressed::standard::intersectionJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL intersection join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                case hec::Q_INTERSECTION_JOIN:
                    ret = uncompressed::standard::intersectionJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL intersection join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                case hec::Q_COVERED_BY_JOIN:   
                case hec::Q_INSIDE_JOIN:
                    ret = uncompressed::standard::insideCoveredByJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL inside/covered by join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                case hec::Q_DISJOINT_JOIN:
                    ret = uncompressed::standard::disjointJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL disjoint join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                case hec::Q_EQUAL_JOIN:
                    ret = uncompressed::standard::equalJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL equality join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                case hec::Q_MEET_JOIN:
                    ret = uncompressed::standard::meetJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL meet join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                case hec::Q_CONTAINS_JOIN:
                case hec::Q_COVERS_JOIN:
                    ret = uncompressed::standard::containsCoversJoinAPRIL(&objR->aprilData, &objS->aprilData, iFilterResult);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "APRIL contains/covers join failed for pair", objR->recID, "and", objS->recID);
                        return ret;
                    }
                    break;
                default:
                    // not supported/unknown
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query for standard APRIL intermediate filter. Query type:", mapping::queryTypeIntToStr(g_config.queryPipeline.queryType));
                    return DBERR_QUERY_INVALID_TYPE;
            }
            // if true negative or true hit, return
            if (iFilterResult != INCONCLUSIVE) {
                // count APRIL result
                if (iFilterResult == TRUE_HIT) {
                    queryResult->addResult(objR->recID, objS->recID);
                }
                return ret;
            }
            // refine based on query type
            switch (g_config.queryPipeline.queryType) {
                case hec::Q_RANGE:
                    refinement::relate::refineIntersectionJoin(objR, objS, queryResult);
                    break;
                case hec::Q_INTERSECTION_JOIN:
                    refinement::relate::refineIntersectionJoin(objR, objS, queryResult);
                    break;
                case hec::Q_COVERED_BY_JOIN:
                    refinement::relate::refineCoveredByJoin(objR, objS, queryResult);
                    break;
                case hec::Q_INSIDE_JOIN:
                    refinement::relate::refineInsideJoin(objR, objS, queryResult);
                    break;
                case hec::Q_DISJOINT_JOIN:
                    refinement::relate::refineDisjointJoin(objR, objS, queryResult);
                    break;
                case hec::Q_EQUAL_JOIN:
                    refinement::relate::refineEqualJoin(objR, objS, queryResult);
                    break;
                case hec::Q_MEET_JOIN:
                    refinement::relate::refineMeetJoin(objR, objS, queryResult);
                    break;
                case hec::Q_CONTAINS_JOIN:
                    refinement::relate::refineContainsJoin(objR, objS, queryResult);
                    break;
                case hec::Q_COVERS_JOIN:
                    refinement::relate::refineCoversJoin(objR, objS, queryResult);
                    break;
                default:
                    // not supported/unknown
                    logger::log_error(DBERR_QUERY_INVALID_TYPE, "Unsupported query for refinement. Query type:", mapping::queryTypeIntToStr(g_config.queryPipeline.queryType));
                    return DBERR_QUERY_INVALID_TYPE;
            }
            return ret;
        }
    }
}