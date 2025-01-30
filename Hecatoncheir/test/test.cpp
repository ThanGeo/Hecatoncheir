#include <stdio.h>
#include <fstream>
#include <iostream>

#include <chrono>

#include <../API/Hecatoncheir.h>
#include "../include/def.h"
#include "../include/utils.h"


bool test6(hec::DatasetID datasetRID, hec::DatasetID datasetSID) {
    logger::log_task("Running Test6...");
    int ret = hec::finalize();
    if (ret != 0) {
        return false;
    }
    return true;
}

bool test5(hec::DatasetID datasetRID, hec::DatasetID datasetSID) {
    logger::log_task("Running Test5...");
    hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.INTERSECTS(), hec::queryResultTypes.COUNT());
    hec::QueryResult result =  hec::query(&intersectionJoinQuery);

    if (result.getResultCount() != 5) {
        logger::log_error(DBERR_OPERATION_FAILED, "Expected 5 results in join query, got", result.getResultCount());
        return false;
    }

    return true;
}

bool test4(hec::DatasetID datasetRID, hec::DatasetID datasetSID) {
    logger::log_task("Running Test4...");
    int ret = hec::buildIndex({datasetRID, datasetSID}, hec::IT_TWO_LAYER);
    if (ret != 0) {
        return false;
    }
    return true;
}

bool test3(hec::DatasetID datasetRID, hec::DatasetID datasetSID) {
    logger::log_task("Running Test3...");
    int ret = hec::partition({datasetRID, datasetSID});
    if (ret != 0) {
        return false;
    }
    return true;
}

bool test2(hec::DatasetID &datasetRID, hec::DatasetID &datasetSID) {
    logger::log_task("Running Test2...");
    std::string datasetFullPathR = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample1.tsv";
    std::string datasetFullPathS = std::string(HECATONCHEIR_DIR) + "/test/samples/data_sample2.tsv";
    datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON", false);
    datasetSID = hec::prepareDataset(datasetFullPathS, "WKT", "POLYGON", false);
    if (datasetRID == 0 && datasetSID == 1) {
        // all well
        return true;
    }
    return false;
}

bool test1(std::vector<std::string> &nodefile) {
    logger::log_task("Running Test1...");
    int ret = hec::init(nodefile.size(), nodefile);
    if (ret != 0) {
        return false;
    }
    return true;
}

void showFinalResults(std::vector<bool> &resultList) {
    int passed = 0;
    for (int i=0; i<resultList.size(); i++) {
        if (resultList[i]) {
            passed++;
        }
    }
    logger::log_task("---------------------------------");
    logger::log_task("     ", passed, "/", resultList.size(), "tests passed");
    logger::log_task("---------------------------------");
    
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        logger::log_error(DBERR_MISSING_FILE, "Please provide the nodefile containing the available machines as an argument for testing.");
        return 1;
    }
    // parse nodefile
    std::ifstream nodefile(argv[1]);
    std::vector<std::string> nodes;
    std::string line;
    if (!nodefile) {
        logger::log_error(DBERR_OPEN_FILE_FAILED, "Failed to open the provided nodefile. Might be corrupted or permission locked.");
        return 1;
    }
    while (std::getline(nodefile, line)) {
        nodes.push_back(line+":1");
    }
    nodefile.close();
    if (nodes.size() < 1) {
        logger::log_error(DBERR_INVALID_PARAMETER, "Nodefile must contain at least one address/alias.");
        return DBERR_INVALID_PARAMETER;
    }

    // run
    std::vector<bool> resultList;
    bool result;

    /** @test Test1: Initialize MPI environment */
    auto start = std::chrono::high_resolution_clock::now();
    result = test1(nodes);
    resultList.emplace_back(result);
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    if (result) {
        std::cout << GREEN "[SUCCESS]" NC ": " << "Test1 finished in: " << duration.count() << " seconds" << std::endl;
    } else {
        std::cout << RED "[FAILURE]" NC ": " << "Test1 finished in: " << duration.count() << " seconds" << std::endl;
    }
    
    /** @test Test2: Prepare a datasets */
    hec::DatasetID datasetRID;
    hec::DatasetID datasetSID;
    start = std::chrono::high_resolution_clock::now();
    result = test2(datasetRID, datasetSID);
    resultList.emplace_back(result);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    if (result) {
        std::cout << GREEN "[SUCCESS]" NC ": " << "Test2 finished in: " << duration.count() << " seconds" << std::endl;
    } else {
        std::cout << RED "[FAILURE]" NC ": " << "Test2 finished in: " << duration.count() << " seconds" << std::endl;
    }

    /** @test Test3: Partition datasets*/
    start = std::chrono::high_resolution_clock::now();
    result = test3(datasetRID, datasetSID);
    resultList.emplace_back(result);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    if (result) {
        std::cout << GREEN "[SUCCESS]" NC ": " << "Test3 finished in: " << duration.count() << " seconds" << std::endl;
    } else {
        std::cout << RED "[FAILURE]" NC ": " << "Test3 finished in: " << duration.count() << " seconds" << std::endl;
    }

    /** @test Test4: Index datasets and create APRIL */
    start = std::chrono::high_resolution_clock::now();
    result = test4(datasetRID, datasetSID);
    resultList.emplace_back(result);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    if (result) {
        std::cout << GREEN "[SUCCESS]" NC ": " << "Test4 finished in: " << duration.count() << " seconds" << std::endl;
    } else {
        std::cout << RED "[FAILURE]" NC ": " << "Test4 finished in: " << duration.count() << " seconds" << std::endl;
    }

    /** @test Test5: Index datasets and create APRIL */
    start = std::chrono::high_resolution_clock::now();
    result = test5(datasetRID, datasetSID);
    resultList.emplace_back(result);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    if (result) {
        std::cout << GREEN "[SUCCESS]" NC ": " << "Test5 finished in: " << duration.count() << " seconds" << std::endl;
    } else {
        std::cout << RED "[FAILURE]" NC ": " << "Test5 finished in: " << duration.count() << " seconds" << std::endl;
    }

    /** @test Test6: Index datasets and create APRIL */
    start = std::chrono::high_resolution_clock::now();
    result = test6(datasetRID, datasetSID);
    resultList.emplace_back(result);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    if (result) {
        std::cout << GREEN "[SUCCESS]" NC ": " << "Test6 finished in: " << duration.count() << " seconds" << std::endl;
    } else {
        std::cout << RED "[FAILURE]" NC ": " << "Test6 finished in: " << duration.count() << " seconds" << std::endl;
    }

    /** show final */
    showFinalResults(resultList);

    return 0;
}