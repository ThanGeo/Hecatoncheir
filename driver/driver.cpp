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
    // TIGER
    std::string datasetFullPathR = "/home/hec/datasets/T1.tsv";
    std::string datasetFullPathS = "/home/hec/datasets/T2.tsv";
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON", -180.0, -15.0, 180.0, 72.0, false);
    hec::DatasetID datasetSID = hec::prepareDataset(datasetFullPathS, "WKT", "POLYGON", -180.0, -15.0, 180.0, 72.0, false);

    // OSM
    // std::string datasetFullPathR = "/home/hec/datasets/OSM/O5_lakes.tsv";
    // std::string datasetFullPathS = "/home/hec/datasets/OSM/O6_parks.tsv";
    // hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON", -180.0, -180.0, 180.0, 180.0, false);
    // hec::DatasetID datasetSID = hec::prepareDataset(datasetFullPathS, "WKT", "POLYGON", -180.0, -180.0, 180.0, 180.0, false);


    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    hec::partition({datasetRID, datasetSID});
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // // build index
    // start = std::chrono::high_resolution_clock::now();
    // hec::buildIndex({datasetRID, datasetSID}, hec::IT_TWO_LAYER);
    // end = std::chrono::high_resolution_clock::now();
    // duration = end - start;
    // std::cout << "Index building time: " << duration.count() << " seconds" << std::endl;


    // // load datasets
    // // hec::load({datasetRID, datasetSID});
    
    // hec::JoinQuery intersectionJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.INTERSECTS(), hec::queryResultTypes.COUNT());
    // start = std::chrono::high_resolution_clock::now();
    // hec::QueryResult result =  hec::query(&intersectionJoinQuery);
    // end = std::chrono::high_resolution_clock::now();
    // duration = end - start;
    // std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    // printf("Total results: %lu\n", result.getResultCount());

    // hec::JoinQuery findRelationJoinQuery(datasetRID, datasetSID, 0, hec::spatialQueries.FIND_RELATION(), hec::queryResultTypes.COUNT());
    // start = std::chrono::high_resolution_clock::now();
    // result =  hec::query(&findRelationJoinQuery);
    // end = std::chrono::high_resolution_clock::now();
    // duration = end - start;
    // std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    // result.print();

}

void runRangePoints() {
    std::string datasetFullPathR = "/home/hec/datasets/T1_points.tsv";
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POINT", false);

    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    hec::partition({datasetRID});
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // build index
    start = std::chrono::high_resolution_clock::now();
    hec::buildIndex({datasetRID}, hec::IT_TWO_LAYER);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Index building time: " << duration.count() << " seconds" << std::endl;

    std::string queryWKT = "POLYGON ((-80.8467 43.8295), (-80.6066 43.8295), (-80.6066 44.0407), (-80.8467 44.0407), (-80.8467 43.8295))";
    hec::RangeQuery rangeQuery(datasetRID, 0, queryWKT, hec::queryResultTypes.COUNT());
    start = std::chrono::high_resolution_clock::now();
    hec::QueryResult result =  hec::query(&rangeQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    result.print();
}

void runRangePolygons() {
    std::string datasetFullPathR = "/home/hec/datasets/T2.tsv";
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON", false);

    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    hec::partition({datasetRID});
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // build index
    start = std::chrono::high_resolution_clock::now();
    hec::buildIndex({datasetRID}, hec::IT_TWO_LAYER);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Index building time: " << duration.count() << " seconds" << std::endl;

    std::string queryWKT = "POLYGON ((-87.82 32.82,-87.92 32.82,-87.92 32.92,-87.82 32.92,-87.82 32.82))";
    hec::RangeQuery rangeQuery(datasetRID, 0, queryWKT, hec::queryResultTypes.COUNT());
    start = std::chrono::high_resolution_clock::now();
    hec::QueryResult result =  hec::query(&rangeQuery);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    result.print();
}

void runRangePolygonsBatch() {
    std::string datasetFullPathR = "/home/hec/datasets/T2.tsv";
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, "WKT", "POLYGON", false);

    // partition datasets
    auto start = std::chrono::high_resolution_clock::now();
    hec::partition({datasetRID});
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> duration = end - start;
    std::cout << "Partitioning time: " << duration.count() << " seconds" << std::endl;

    // build index
    start = std::chrono::high_resolution_clock::now();
    hec::buildIndex({datasetRID}, hec::IT_TWO_LAYER);
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Index building time: " << duration.count() << " seconds" << std::endl;

    std::vector<std::string> query_batch = {
        "POLYGON ((-87.82 32.82,-87.92 32.82,-87.92 32.92,-87.82 32.92,-87.82 32.82))",
        "POLYGON ((-84.96 31.33,-84.94 31.33,-84.94 31.37,-84.96 31.37,-84.96 31.33))"
    };
    
    hec::RangeQuery rangeQueryA(datasetRID, 0, query_batch[0], hec::queryResultTypes.COUNT());
    hec::RangeQuery rangeQueryB(datasetRID, 1, query_batch[1], hec::queryResultTypes.COUNT());

    start = std::chrono::high_resolution_clock::now();
    std::vector<hec::QueryResult> result =  hec::query({rangeQueryA, rangeQueryB});
    end = std::chrono::high_resolution_clock::now();
    duration = end - start;
    std::cout << "Query Evaluation time: " << duration.count() << " seconds" << std::endl;
    // result.print();
    for (auto &it: result) {
        it.print();
    }
}

int main(int argc, char* argv[]) {
    std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1", "vm5:1", "vm6:1", "vm7:1", "vm8:1", "vm9:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1"};

    // Initialize Hecatoncheir. Must use this method before any other calls to the framework.
    hec::init(hosts.size(), hosts);

    switch (0) {
        case 0:
            // join queries
            runJoins();
            break;
        case 1:
            // range polygons
            runRangePolygons();
            break;
        
        case 2:
            // range points
            runRangePoints();
            break;
        
        case 3:
            // range points
            runRangePolygonsBatch();
            break;
    }
    
    // finalize
    hec::finalize();

    return 0;
}