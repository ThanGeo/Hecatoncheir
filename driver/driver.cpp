#include <stdio.h>
#include <fstream>
#include <iostream>

#include <chrono>

#include <Hecatoncheir.h>

static std::vector<std::string> read_hostfile(const std::string& hostfile) {
    std::vector<std::string> hosts;
    std::ifstream file(hostfile);
    if (!file.is_open()) {
        std::cerr << "Error: Could not open hostfile " << hostfile << std::endl;
        exit(-1);
    }

    std::string line;
    while (std::getline(file, line)) {
        // Ignore empty lines or comments
        if (!line.empty() && line[0] != '#') {
            hosts.push_back(line);
        }
    }
    file.close();
    return hosts;
}

int main(int argc, char* argv[]) {
    std::string datasetFullPathR = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/datasets/T1.wkt";
    // std::string datasetFullPathS = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/datasets/T2.wkt";
    std::string datasetFullPathS = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/datasets/T8.wkt";
    std::vector<std::string> hosts = {"node1:1", "node2:1", "node3:1", "node4:1", "node5:1"};
    // std::vector<std::string> hosts = {"node1:1"};

    // Initialize Hecatoncheir. Must use this method before any other calls to the framework.
    hec::init(hosts.size(), hosts);

    // prepare datasets
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON");
    hec::DatasetID datasetSID = hec::prepareDataset(datasetFullPathS, "WKT", "LINESTRING");
    // printf("Dataset ids: %d and %d\n", datasetRID, datasetSID);

    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    
    // hec::partition({datasetRID, datasetSID});

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // load datasets
    hec::load({datasetRID, datasetSID});
    
    // run query (@todo maybe merge creating a query object and calling hec::query into a single thing)
    // -87.906508 32.896858,-87.906483 32.896926
    hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.INTERSECTS(), hec::queryResultTypes.COUNT());
    
    start = std::chrono::high_resolution_clock::now();

    hec::QueryResult result =  hec::query(&intersectionJoinQuery);

    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    printf("Total results: %lu\n", result.getResultCount());





    hec::JoinQuery findRelationJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.FIND_RELATION(), hec::queryResultTypes.COUNT());
    start = std::chrono::high_resolution_clock::now();

    result =  hec::query(&findRelationJoinQuery);

    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    
    result.print();

    // finalize
    hec::finalize();

    return 0;
}