#include "UniformGrid/filter.h"

namespace uniform_grid
{
    DB_STATUS processQuery(hec::Query* query, std::unordered_map<int, DJBatch>& borderObjectsMap, std::unique_ptr<hec::QResultBase>& queryResult) {
        DB_STATUS ret = DBERR_OK;
        // set to global config
        g_config.queryPipeline.queryType = (hec::QueryType) query->getQueryType();

        // switch based on query type
        switch (query->getQueryType()) {
            case hec::Q_DISTANCE_JOIN:
                {
                    // cast
                    hec::DistanceJoinQuery* distanceQuery = dynamic_cast<hec::DistanceJoinQuery*>(query);
                    // evaluate
                    ret = distance_filter::evaluate(distanceQuery, borderObjectsMap, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type must be distance join for this version of uniform index query processing. Type:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }

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
            case hec::Q_KNN:
                {
                    // cast
                    hec::KNNQuery* kNNQuery = dynamic_cast<hec::KNNQuery*>(query);
                    // evaluate
                    ret = knn_filter::evaluate(kNNQuery, queryResult);
                    if (ret != DBERR_OK) {
                        return ret;
                    }
                }
                break;
            default:
                logger::log_error(DBERR_FEATURE_UNSUPPORTED, "Query type not supported for Uniform Grid index:", mapping::queryTypeIntToStr((hec::QueryType) query->getQueryType()));
                return DBERR_FEATURE_UNSUPPORTED;
        }

        return ret;
    }

    
}