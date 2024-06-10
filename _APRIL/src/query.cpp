#include "query.h"

namespace APRIL
{
    spatial_lib::QueryT* g_query;

    void setupAPRILIntermediateFilter(spatial_lib::QueryT *query) {
        // store query info
        g_query = query;
    }
    
    namespace optimized
    {
        namespace relate
        {
            /**
             * WARNING: standard and optimized relate functions are THE SAME right now, but I keep separated because future optimization might happen.
             * 
            */

            static void APRILIntersectionFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = intersectionAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);
                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineIntersectionJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILInsideFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = insideAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineInsideJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILDisjointFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = disjointAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineDisjointJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILEqualFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = equalAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineEqualJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILMeetFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = meetAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);
                
                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineMeetJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILContainsFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = containsAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineContainsJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILCoversFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = coversAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);
                
                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineCoversJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILCoveredByFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = coveredByAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineCoveredByJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }
        }


        clock_t temp_timer;
        namespace find_relation
        {
            static void specializedTopologyRinSContainment(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                int iFilterResult = -1;
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    iFilterResult = RinSContainmentAPRILUncompressed(aprilR, aprilS);
                    // switch based on result
                    switch(iFilterResult) {
                        // true hits, count and return
                        case spatial_lib::TR_INSIDE:
                        case spatial_lib::TR_DISJOINT:
                        case spatial_lib::TR_INTERSECT:
                            // result
                            spatial_lib::countTopologyRelationResult(iFilterResult);

                            // time
                            spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                            return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                int relation = -1;
                // switch based on result
                switch(iFilterResult) {            
                    // inconclusive, do selective refinement
                    // result holds what type of refinement needs to be made
                    case spatial_lib::REFINE_INSIDE_COVEREDBY_TRUEHIT_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineInsideCoveredbyTruehitIntersect(idR, idS);
                        break;
                    case spatial_lib::REFINE_DISJOINT_INSIDE_COVEREDBY_MEET_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineDisjointInsideCoveredbyMeetIntersect(idR, idS);
                        break;
                    default:
                        fprintf(stderr, "Error, relation value is %d",relation);
                        break;
                    
                }
                // time
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
                // count the result
                spatial_lib::countTopologyRelationResult(relation);
            }

            static void specializedTopologySinRContainment(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                int iFilterResult = -1;
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    iFilterResult = SinRContainmentAPRILUncompressed(aprilR, aprilS);
                    // switch based on result
                    switch(iFilterResult) {
                        // true hits, count and return
                        case spatial_lib::TR_CONTAINS:
                        case spatial_lib::TR_DISJOINT:
                        case spatial_lib::TR_INTERSECT:
                            // if(iFilterResult == spatial_lib::TR_INTERSECT){
                            //     printf("%u,%u\n", idR, idS);
                            // }
                            // result
                            spatial_lib::countTopologyRelationResult(iFilterResult);
                            // time
                            spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                            return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                int relation = -1;
                // switch based on result
                switch(iFilterResult) {            
                    // inconclusive, do selective refinement
                    // result holds what type of refinement needs to be made
                    case spatial_lib::REFINE_CONTAINS_COVERS_TRUEHIT_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineContainsCoversTruehitIntersect(idR, idS);
                        break;
                    case spatial_lib::REFINE_DISJOINT_CONTAINS_COVERS_MEET_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineDisjointContainsCoversMeetIntersect(idR, idS);
                        break;
                    
                }
                // time
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
                // count the result
                spatial_lib::countTopologyRelationResult(relation);
                // if(relation == spatial_lib::TR_INTERSECT){
                //     printf("%u,%u\n", idR, idS);
                // }
            }

            static void specializedTopologyIntersection(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                int iFilterResult;
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    iFilterResult = MBRIntersectionAPRILUncompressed(aprilR, aprilS);
                    // switch based on result
                    switch(iFilterResult) {
                        // true hits, count and return
                        case spatial_lib::TR_DISJOINT:
                        case spatial_lib::TR_INTERSECT:
                            // if(iFilterResult == spatial_lib::TR_INTERSECT){
                            //     printf("%u,%u\n", idR, idS);
                            // }
                            // result
                            spatial_lib::countTopologyRelationResult(iFilterResult);
                            // time
                            spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                            return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // time 
                spatial_lib::time::markRefinementFilterTimer();
                // refine
                int relation = spatial_lib::refineDisjointMeetIntersect(idR, idS);
                // time
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
                // count the result
                spatial_lib::countTopologyRelationResult(relation);
                // if(relation == spatial_lib::TR_INTERSECT){
                //     printf("%u,%u\n", idR, idS);
                // }
            }

            static void specializedTopologyEqual(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                int iFilterResult = -1;
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    iFilterResult = equalMBRsAPRILUncompressed(idR, idS, aprilR, aprilS);
                
                    // switch based on result
                    switch(iFilterResult) {
                        // true hits, count and return
                        case spatial_lib::TR_DISJOINT:
                        // case spatial_lib::TR_EQUAL:
                        case spatial_lib::TR_COVERED_BY:
                        case spatial_lib::TR_COVERS:
                        case spatial_lib::TR_INTERSECT:
                            // if(iFilterResult == spatial_lib::TR_INTERSECT) {
                            //     printf("%u,%u\n",idR,idS);
                            // }
                            // result
                            spatial_lib::countTopologyRelationResult(iFilterResult);
                            // time
                            spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                            return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                int relation = -1;
                // switch based on result
                switch(iFilterResult) {            
                    // inconclusive, do selective refinement
                    // result holds what type of refinement needs to be made
                    case spatial_lib::REFINE_COVEREDBY_TRUEHIT_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineCoveredbyTrueHitIntersect(idR, idS);
                        break;
                    case spatial_lib::REFINE_COVERS_TRUEHIT_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineCoversTrueHitIntersect(idR, idS);
                        break;
                    case spatial_lib::REFINE_COVERS_COVEREDBY_TRUEHIT_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineCoversCoveredByTrueHitIntersect(idR, idS);
                        break;
                    case spatial_lib::REFINE_EQUAL_COVERS_COVEREDBY_TRUEHIT_INTERSECT:
                        // time 
                        spatial_lib::time::markRefinementFilterTimer();
                        // refine
                        relation = spatial_lib::refineEqualCoversCoveredByTrueHitIntersect(idR, idS);
                        break;
                    default:
                        // error
                        fprintf(stderr, "Error, relation value is %d",relation);
                        break;
                }
                // time
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
                // count the result
                spatial_lib::countTopologyRelationResult(relation);
                // if(relation == spatial_lib::TR_INTERSECT) {
                //     printf("%u,%u\n",idR,idS);
                // }
            }

            static void APRILFindRelation(uint idR, uint idS, int relationCase) {
                // use appropriate query function
                switch (relationCase) {
                    case spatial_lib::MBR_R_IN_S:
                        specializedTopologyRinSContainment(idR, idS);
                        break;
                    case spatial_lib::MBR_S_IN_R:
                        specializedTopologySinRContainment(idR, idS);
                        break;
                    case spatial_lib::MBR_EQUAL:
                        specializedTopologyEqual(idR, idS);
                        break;
                    case spatial_lib::MBR_INTERSECT:
                        specializedTopologyIntersection(idR, idS);
                        break;
                    default:
                        // not supported/unknown
                        exit(-1);
                        break;
                }
            }
        }

        void IntermediateFilterEntrypoint(uint idR, uint idS, int relationCase) {
            // time 
            spatial_lib::time::markiFilterTimer();
            // count post mbr candidate
            spatial_lib::g_queryOutput.postMBRFilterCandidates += 1;
            
            // if (idR == 220594 && idS == 357429) {
            // if (idR == 206434 && idS == 9173759) {
            //     temp_timer = spatial_lib::time::getNewTimer();
            // }

            switch (g_query->type) {
                case spatial_lib::Q_INTERSECT:
                    relate::APRILIntersectionFilter(idR, idS);
                    break;
                case spatial_lib::Q_INSIDE:
                    relate::APRILInsideFilter(idR, idS);
                    break;
                case spatial_lib::Q_DISJOINT:
                    relate::APRILDisjointFilter(idR, idS);
                    break;
                case spatial_lib::Q_EQUAL:
                    relate::APRILEqualFilter(idR, idS);
                    break;
                case spatial_lib::Q_MEET:
                    relate::APRILMeetFilter(idR, idS);
                    break;
                case spatial_lib::Q_CONTAINS:
                    relate::APRILContainsFilter(idR, idS);
                    break;
                case spatial_lib::Q_COVERS:
                    relate::APRILCoversFilter(idR, idS);
                    break;
                case spatial_lib::Q_COVERED_BY:   
                    relate::APRILCoveredByFilter(idR, idS);
                    break;
                case spatial_lib::Q_FIND_RELATION:
                    find_relation::APRILFindRelation(idR, idS, relationCase);
                    break;
                default:
                    fprintf(stderr, "Invalid query and APRIL filter combination.\n");
                    exit(-1);
                    break;
            }
        }   
    }





    namespace standard
    {
        clock_t temp_timer;
        namespace relate
        {
            /**
             * WARNING: standard and optimized relate functions are THE SAME right now, but I keep separated because future optimization might happen.
             * 
            */

            static void APRILIntersectionFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = intersectionAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);
                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineIntersectionJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILInsideFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = insideAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineInsideJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILDisjointFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = disjointAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineDisjointJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILEqualFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = equalAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineEqualJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILMeetFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = meetAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);
                
                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineMeetJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILContainsFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = containsAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineContainsJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILCoversFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = coversAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);
                
                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineCoversJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }

            static void APRILCoveredByFilter(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    int iFilterResult = coveredByAPRILUncompressed(aprilR, aprilS);
                    if (iFilterResult != spatial_lib::INCONCLUSIVE) {
                        // true negative or true hit, return
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countAPRILResult(iFilterResult);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count result
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // refinement
                spatial_lib::time::markRefinementFilterTimer();
                spatial_lib::refineCoveredByJoin(idR, idS);
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }
        }
        
        namespace find_relation
        {
            /**
             * 
             * STANDARD APRIL (only disjoint/intersect) used for find relation queries.
             * All non-disjoint pairs are refined using DE-9IM
             * 
             */
            static void APRILFindRelation(uint idR, uint idS) {
                // get common sections
                std::vector<uint> commonSections = spatial_lib::getCommonSectionIDsOfObjects(g_query->R, g_query->S, idR, idS);
                // for each common section
                int iFilterResult;
                for (auto &sectionID : commonSections) {
                    // fetch the APRIL of R and S for this section
                    spatial_lib::AprilDataT* aprilR = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->R, sectionID, idR);
                    spatial_lib::AprilDataT* aprilS = spatial_lib::getAprilDataBySectionAndObjectIDs(g_query->S, sectionID, idS);

                    // use APRIL intermediate filter
                    // TODO: optimize (create new func that only checks AA)
                    iFilterResult = intersectionAPRILUncompressed(aprilR, aprilS);
                    // switch based on result
                    if (iFilterResult == spatial_lib::TRUE_NEGATIVE) {
                        // true negative, count disjoint pair
                        // measure time
                        spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
                        // count result
                        spatial_lib::countTopologyRelationResult(spatial_lib::TR_DISJOINT);
                        return;
                    }
                }
                // i filter ended, measure time
                spatial_lib::g_queryOutput.iFilterTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.iFilterTimer);
            
                // count refinement candidate
                spatial_lib::countAPRILResult(spatial_lib::INCONCLUSIVE);

                // time 
                spatial_lib::time::markRefinementFilterTimer();
                // refine
                spatial_lib::refineAllRelations(idR, idS);

                // if (idR == 220594 && idS == 357429) {
                //     printf("Time passed evaluating pair %u,%u: %f seconds.\n", idR, idS, spatial_lib::time::getElapsedTime(temp_timer));
                // }


                // time
                spatial_lib::g_queryOutput.refinementTime += spatial_lib::time::getElapsedTime(spatial_lib::time::g_timer.refTimer);
            }
        }


        void IntermediateFilterEntrypoint(uint idR, uint idS) {
            // time 
            spatial_lib::time::markiFilterTimer();

            // if (idR == 220594 && idS == 357429) {
            //     temp_timer = spatial_lib::time::getNewTimer();
            // }

            // count post mbr candidate
            spatial_lib::g_queryOutput.postMBRFilterCandidates += 1;
            // use appropriate query function
            switch (g_query->type) {
                case spatial_lib::Q_INTERSECT:
                    relate::APRILIntersectionFilter(idR, idS);
                    break;
                case spatial_lib::Q_INSIDE:
                    relate::APRILInsideFilter(idR, idS);
                    break;
                case spatial_lib::Q_DISJOINT:
                    relate::APRILDisjointFilter(idR, idS);
                    break;
                case spatial_lib::Q_EQUAL:
                    relate::APRILEqualFilter(idR, idS);
                    break;
                case spatial_lib::Q_MEET:
                    relate::APRILMeetFilter(idR, idS);
                    break;
                case spatial_lib::Q_CONTAINS:
                    relate::APRILContainsFilter(idR, idS);
                    break;
                case spatial_lib::Q_COVERS:
                    relate::APRILCoversFilter(idR, idS);
                    break;
                case spatial_lib::Q_COVERED_BY:   
                    relate::APRILCoveredByFilter(idR, idS);
                    break;
                case spatial_lib::Q_FIND_RELATION:
                    find_relation::APRILFindRelation(idR, idS);
                    break;
                default:
                    // not supported/unknown
                    printf("This combination of MBR and Intermediate filter for this query type has not been implemented yet.\n");
                    exit(-1);
                    break;
            }
        }
    }
}