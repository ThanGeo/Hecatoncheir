#include <stdio.h>
#include <fstream>
#include <iostream>

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
    std::string datasetFullPathS = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/datasets/T2.wkt";
    std::vector<std::string> hosts = {"node1:1", "node2:1", "node3:1"};
    // std::vector<std::string> hosts = {"node1:1", "node2:1"};

    // Initialize Hecatoncheir. Must use this method before any other calls to the framework.
    hec::init(hosts.size(), hosts);

    // prepare datasets
    hec::DatasetID datasetRID = hec::prepareDataset(datasetFullPathR, hec::WKT, hec::POLYGON);
    hec::DatasetID datasetSID = hec::prepareDataset(datasetFullPathS, hec::WKT, hec::POLYGON);
    // printf("Dataset ids: %d and %d\n", datasetRID, datasetSID);

    // partition datasets
    hec::partitionDataset({datasetRID, datasetSID});
    
    
    // run query

    // finalize
    hec::finalize();

    return 0;
}