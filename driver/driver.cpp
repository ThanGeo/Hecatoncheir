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

void runJoins() {
    std::string datasetFullPathR = "/home/hec/thanasis/datasets/T1.wkt";
    std::string datasetFullPathS = "/home/hec/thanasis/datasets/T2.wkt";
    // prepare datasets
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON", false);
    hec::DatasetID datasetSID = hec::prepareDataset(datasetFullPathS, "WKT", "POLYGON", false);
    // printf("Dataset ids: %d and %d\n", datasetRID, datasetSID);

    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    hec::partition({datasetRID, datasetSID});
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // build index
    start = std::chrono::high_resolution_clock::now();
    hec::buildIndex({datasetRID, datasetSID}, hec::IT_TWO_LAYER);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Index building time: " << duration.count() << " seconds" << std::endl;


    // load datasets
    // hec::load({datasetRID, datasetSID});
    
    hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.INTERSECTS(), hec::queryResultTypes.COUNT());
    start = std::chrono::high_resolution_clock::now();
    hec::QueryResult result =  hec::query(&intersectionJoinQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    printf("Total results: %lu\n", result.getResultCount());

    // hec::JoinQuery findRelationJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.FIND_RELATION(), hec::queryResultTypes.COUNT());
    // start = std::chrono::high_resolution_clock::now();
    // result =  hec::query(&findRelationJoinQuery);
    // end = std::chrono::high_resolution_clock::now();
    // duration = end - start;
    // std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    // result.print();

}

void runRange() {
    std::string datasetFullPathR = "/home/hec/thanasis/datasets/T1_points.wkt";
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POINT", false);

    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    hec::partition({datasetRID});
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // load datasets
    // hec::load({datasetRID});



    // hec::RangeQuery rangeQuery(datasetRID, 0, );
    // start = std::chrono::high_resolution_clock::now();
    // hec::QueryResult result =  hec::query(&findRelationJoinQuery);
    // end = std::chrono::high_resolution_clock::now();
    // duration = end - start;
    // std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    // result.print();
}

int main(int argc, char* argv[]) {
    
    // std::string datasetFullPathS = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/datasets/T8.wkt";
    // std::vector<std::string> hosts = {"node1:1", "node2:1", "node3:1", "node4:1", "node5:1"};
    // std::vector<std::string> hosts = {"node1:1"};
    std::vector<std::string> hosts = {"node1:1", "node2:1", "node3:1"};

    // Initialize Hecatoncheir. Must use this method before any other calls to the framework.
    hec::init(hosts.size(), hosts);

    if (true) {
        // join queries
        runJoins();
    } else {
        // range
        runRange();
    }
    
    // finalize
    hec::finalize();

    return 0;
}