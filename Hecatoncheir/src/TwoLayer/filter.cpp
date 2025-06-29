#include "TwoLayer/filter.h"


namespace twolayer
{   
    DB_STATUS processQuery(hec::Query* query, std::unique_ptr<hec::QResultBase>& queryResult) {
        DB_STATUS ret = DBERR_OK;
        // set to global config
        g_config.queryPipeline.queryType = (hec::QueryType) query->getQueryType();

        // switch based on query type
        switch (query->getQueryType()) {
            case hec::Q_RANGE:
                {
                    // cast
                    hec::RangeQuery* rangeQuery = dynamic_cast<hec::RangeQuery*>(query);
                    // evaluate
                    ret = range_filter::evaluate(rangeQuery, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
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
                hec::PredicateJoinQuery* joinQuery = dynamic_cast<hec::PredicateJoinQuery*>(query);
                // evaluate
                ret = intersection_join_filter::evaluate(joinQuery, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
                break;
            case hec::Q_FIND_RELATION_JOIN: 
            {
                // cast
                hec::PredicateJoinQuery* joinQuery = dynamic_cast<hec::PredicateJoinQuery*>(query);
                // evaluate
                ret = topological_join_filter::evaluate(joinQuery, queryResult);
                if (ret != DBERR_OK) {
                    return ret;
                }
            }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported for Two Layer index:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }
}
