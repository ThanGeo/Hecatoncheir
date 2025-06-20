#include <CoreTest.h>

void CoreTest::prepare() {
    
}

void CoreTest::test1() {
    // partition and index polygons
    std::string polygonsR = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_1.wkt";
    std::string polygonsS = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_polygons_2.wkt";
    // prepare datasets
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
    // unload
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
    ret = hec::unloadDataset(datasetSID);
    ASSERT_EQ(DBERR_OK, ret);
    
}

void CoreTest::test2() {
    // partition and index linestrings
    std::string linestrings = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_linestrings.wkt";
    // prepare datasets
    int datasetRID = hec::prepareDataset(linestrings, "WKT", "LINESTRING", false);
    ASSERT_EQ(0, datasetRID);
    // partition datasets
    int ret = hec::partition({datasetRID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID}, hec::IT_TWO_LAYER);
    ASSERT_EQ(DBERR_OK, ret);
    // unload
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
}

void CoreTest::test3() {
    // partition and index points
    std::string points = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample_points.wkt";
    int datasetRID = hec::prepareDataset(points, "WKT", "POINT", false);
    ASSERT_EQ(0, datasetRID);
    // partition datasets
    int ret = hec::partition({datasetRID});
    ASSERT_EQ(DBERR_OK, ret);
    // index
    ret = hec::buildIndex({datasetRID}, hec::IT_UNIFORM_GRID);
    ASSERT_EQ(DBERR_OK, ret);
    // unload
    ret = hec::unloadDataset(datasetRID);
    ASSERT_EQ(DBERR_OK, ret);
}

void CoreTest::run() {
    // perform any preparation
    current_test_name = "CoreTest::prepare";  // Set global name
    this->prepare();

    // run tests
    for (const auto& [name, func] : tests) {
        current_test_name = name;  // Set global name
        try {
            func();
            std::cout << GREEN "[PASS]" NC ": CoreTest::" << name << "\n";
        } catch (const std::exception& ex) {
            std::cerr << RED "[FAILURE]" NC ": CoreTest::" << name << " - " << ex.what() << "\n";
        }
    }
    
    // finalize
    current_test_name = "CoreTest::fin";  // Set global name
    this->fin();
}

void CoreTest::fin() {
    
}