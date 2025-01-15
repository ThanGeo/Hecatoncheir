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
    std::string datasetFullPathR = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/data/T1.wkt";
    std::string datasetFullPathS = "/home/hec/thanasis/Hecatoncheir/Hecatoncheir/data/T2.wkt";

    // Initialize Hecatoncheir. Must use this method before any other calls to the framework.
    std::vector<std::string> hosts = {"node1:1", "node2:1"};
    hec::init(2, hosts);

    // Prepare/Analyze: Hecatoncheir must first analyze the dataset(s) to determine its metadata
    // bounding boxes etc. This must be done for all datasets that will participate together in a query

    // partition datasets
    // hec::partitionDataset(datasetFullPathR, hec::WKT, hec::POLYGON);
    // hec::partitionDataset(datasetFullPathS, hec::WKT, hec::POLYGON);
    
    // run query

    // finalize
    hec::finalize();

    return 0;
}