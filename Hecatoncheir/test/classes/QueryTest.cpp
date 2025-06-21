#include <QueryTest.h>

void QueryTest::prepare() {

}

void QueryTest::test1() {
    // prepare datasets
    std::string polygonsR = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_1.wkt";
    std::string polygonsS = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_2.wkt";
    int datasetRID = hec::prepareDataset(polygonsR, "WKT", "POLYGON", false);
    int datasetSID = hec::prepareDataset(polygonsS, "WKT", "POLYGON", false);
    ASSERT_EQ(0, datasetRID);
    ASSERT_EQ(1, datasetSID);
    // partition datasets
    int ret = hec::partition({datasetRID, datasetSID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID, datasetSID}, hec::IT_TWO_LAYER);
    ASSERT_EQ(DBERR_OK, ret);
    // run query
    hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.FIND_RELATION(), hec::queryResultTypes.COUNT());
    hec::QResultBase* result;
    result = hec::query(&intersectionJoinQuery);
    // check results
    ASSERT_NE(result, nullptr);
    std::vector<size_t> results = result->getResultList();
    ASSERT_EQ(results[TR_DISJOINT], 0);
    ASSERT_EQ(results[TR_INTERSECT], 4);
    ASSERT_EQ(results[TR_INSIDE], 1);
    ASSERT_EQ(results[TR_CONTAINS], 0);
    ASSERT_EQ(results[TR_COVERED_BY], 0);
    ASSERT_EQ(results[TR_COVERS], 0);
    ASSERT_EQ(results[TR_EQUAL], 0);
    ASSERT_EQ(results[TR_MEET], 0);
    // free memory
    delete result;
    // unload datasets
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
    ret = hec::unloadDataset(datasetSID);
    ASSERT_EQ(DBERR_OK, ret);
}

void QueryTest::test2() {
    // prepare datasets
    std::string polygonsR = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_1.wkt";
    std::string polygonsS = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_2.wkt";
    int datasetRID = hec::prepareDataset(polygonsR, "WKT", "POLYGON", false);
    int datasetSID = hec::prepareDataset(polygonsS, "WKT", "POLYGON", false);
    ASSERT_EQ(0, datasetRID);
    ASSERT_EQ(1, datasetSID);
    // partition datasets
    int ret = hec::partition({datasetRID, datasetSID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID, datasetSID}, hec::IT_TWO_LAYER);
    ASSERT_EQ(DBERR_OK, ret);
    // run query
    hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::Q_INTERSECTION_JOIN, hec::QR_COLLECT);
    hec::QResultBase* result;
    result = hec::query(&intersectionJoinQuery);
    // check results
    ASSERT_NE(result, nullptr);
    std::vector<size_t> results = result->getResultList();
    int resultsSize = results.size()/2;
    ASSERT_EQ(resultsSize, 5);
    // free memory
    delete result;
    // unload datasets
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
    ret = hec::unloadDataset(datasetSID);
    ASSERT_EQ(DBERR_OK, ret);
}

void QueryTest::test3() {
    // prepare datasets
    std::string polygonsR = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_1.wkt";
    std::string polygonsS = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_linestrings.wkt";
    int datasetRID = hec::prepareDataset(polygonsR, "WKT", "POLYGON", false);
    int datasetSID = hec::prepareDataset(polygonsS, "WKT", "LINESTRING", false);
    ASSERT_EQ(0, datasetRID);
    ASSERT_EQ(1, datasetSID);
    // partition datasets
    int ret = hec::partition({datasetRID, datasetSID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID, datasetSID}, hec::IT_TWO_LAYER);
    ASSERT_EQ(DBERR_OK, ret);
    // run query
    hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::Q_INTERSECTION_JOIN, hec::QR_COLLECT);
    hec::QResultBase* result;
    result = hec::query(&intersectionJoinQuery);
    // check results
    ASSERT_NE(result, nullptr);
    std::vector<size_t> results = result->getResultList();
    int resultsSize = results.size()/2;
    ASSERT_EQ(resultsSize, 0);
    // free memory
    delete result;
    // unload datasets
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
    ret = hec::unloadDataset(datasetSID);
    ASSERT_EQ(DBERR_OK, ret);
}

void QueryTest::test4() {
    // prepare datasets
    std::string points = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_points.wkt";
    int datasetRID = hec::prepareDataset(points, "WKT", "POINT", false);
    ASSERT_EQ(0, datasetRID);
    // partition datasets
    int ret = hec::partition({datasetRID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID}, hec::IT_UNIFORM_GRID);
    ASSERT_EQ(DBERR_OK, ret);
    // run query batch
    std::string queriesPath = std::string(HECATONCHEIR_DIR) + "/test/samples/query_sample_points.wkt";
    int k = 2;
    std::vector<hec::Query *> batch = hec::loadKNNQueriesFromFile(queriesPath, "WKT", datasetRID, k);
    int batchSize = batch.size();
    ASSERT_EQ(batchSize, 2);
    std::unordered_map<int, hec::QResultBase *> results = hec::query(batch, hec::Q_KNN);
    // results
    int resultsSize = results.size();
    ASSERT_EQ(resultsSize, 2);
    // query 0
    ASSERT_NE(batch[0], nullptr);
    std::vector<size_t> ids = results[0]->getResultList();
    // for (auto &it: ids) {
    //     printf("%ld\n", it);
    // }
    int totalIds1 = ids.size();
    ASSERT_EQ(totalIds1, 2);
    ASSERT_EQ(ids[0], 33);
    ASSERT_EQ(ids[1], 31);
    ids.clear();
    // query 1
    ASSERT_NE(batch[1], nullptr);
    ids = results[1]->getResultList();
    // for (auto &it: ids) {
    //     printf("%ld\n", it);
    // }
    int totalIds2 = ids.size();
    ASSERT_EQ(totalIds2, 2);
    ASSERT_EQ(ids[0], 53);
    ASSERT_EQ(ids[1], 58);
    // free memory
    delete results[0];
    delete batch[0];
    delete results[1];
    delete batch[1];
    // clear 
    batch.clear();
    results.clear();
    // unload datasets
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
}

void QueryTest::test5() {
    // prepare datasets
    std::string points = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_points.wkt";
    int datasetRID = hec::prepareDataset(points, "WKT", "POINT", false);
    ASSERT_EQ(0, datasetRID);
    // partition datasets
    int ret = hec::partition({datasetRID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID}, hec::IT_UNIFORM_GRID);
    ASSERT_EQ(DBERR_OK, ret);
    // run query batch
    std::string queriesPath = std::string(HECATONCHEIR_DIR) + "/test/samples/query_sample_polygons.wkt";
    std::vector<hec::Query *> batch = hec::loadRangeQueriesFromFile(queriesPath, "WKT", datasetRID, hec::QR_COUNT);
    int batchSize = batch.size();
    ASSERT_EQ(batchSize, 2);
    std::unordered_map<int, hec::QResultBase *> results = hec::query(batch, hec::Q_RANGE);
    // results
    // query 0
    ASSERT_NE(batch[0], nullptr);
    size_t resultCount = results[0]->getResultCount();
    ASSERT_EQ(resultCount, 0);
    // query 1
    ASSERT_NE(batch[1], nullptr);
    resultCount = results[1]->getResultCount();
    ASSERT_EQ(resultCount, 1);
    // free memory
    delete results[0];
    delete batch[0];
    delete results[1];
    delete batch[1];
    // clear 
    batch.clear();
    results.clear();
    // unload datasets
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
}

void QueryTest::test6() {
    // prepare datasets
    std::string polygons = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_2.wkt";
    int datasetRID = hec::prepareDataset(polygons, "WKT", "POLYGON", false);
    ASSERT_EQ(0, datasetRID);
    // partition datasets
    int ret = hec::partition({datasetRID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID}, hec::IT_TWO_LAYER);
    ASSERT_EQ(DBERR_OK, ret);
    // run query batch
    std::string queriesPath = std::string(HECATONCHEIR_DIR) + "/test/samples/query_sample_polygons.wkt";
    std::vector<hec::Query *> batch = hec::loadRangeQueriesFromFile(queriesPath, "WKT", datasetRID, hec::QR_COLLECT);
    int batchSize = batch.size();
    ASSERT_EQ(batchSize, 2);
    std::unordered_map<int, hec::QResultBase *> results = hec::query(batch, hec::Q_RANGE);
    // results
    // query 0
    ASSERT_NE(batch[0], nullptr);
    size_t resultCount = results[0]->getResultCount();
    ASSERT_EQ(resultCount, 0);
    // query 1
    ASSERT_NE(batch[1], nullptr);
    resultCount = results[1]->getResultCount();
    ASSERT_EQ(resultCount, 0);
    // free memory
    delete results[0];
    delete batch[0];
    delete results[1];
    delete batch[1];
    // clear 
    batch.clear();
    results.clear();
    // unload datasets
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
}

void QueryTest::run() {
    // prepare
    current_test_name = "QueryTest::prepare";  // Set global name
    this->prepare();
    // run all tests in the class
    for (const auto& [name, func] : tests) {
        current_test_name = name;  // Set global name
        try {
            func();
            std::cout << GREEN "[PASS]" NC ": QueryTest::" << name << "\n";
        } catch (const std::exception& ex) {
            std::cerr << RED "[FAILURE]" NC ": QueryTest::" << name << " - " << ex.what() << "\n";
        }
    }
    // finalize
    current_test_name = "QueryTest::prepare";  // Set global name
    this->fin();
}

void QueryTest::fin() {
    
}