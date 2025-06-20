#include <stdio.h>
#include <fstream>
#include <iostream>

#include <chrono>

#include <../API/Hecatoncheir.h>
#include "../include/def.h"
#include "../include/utils.h"

#include <BaseTest.h>
#include <CoreTest.h>
#include <QueryTest.h>

std::vector<std::string> loadNodes(const std::string& nodefilePath) {
    std::ifstream nodefile(nodefilePath);
    std::vector<std::string> nodes;
    std::string line;
    if (!nodefile) {
        logger::log_error(DBERR_OPEN_FILE_FAILED, "Failed to open the provided nodefile. Might be corrupted or permission locked.");
        return nodes;
    }
    while (std::getline(nodefile, line)) {
        nodes.push_back(line+":1");
    }
    nodefile.close();
    if (nodes.size() < 1) {
        logger::log_error(DBERR_INVALID_PARAMETER, "Nodefile must contain at least one address/alias.");
        return nodes;
    }
    return nodes;
} 

int main(int argc, char* argv[]) {
    // check input
    if (argc < 2) {
        logger::log_error(DBERR_MISSING_FILE, "Please provide the nodefile containing the available machines as an argument for testing.");
        return -1;
    }

    // parse nodefile
    std::vector<std::string> nodes = loadNodes(argv[1]);
    if (nodes.size() == 0) {
        return -1;
    } 

    // init
    BaseTest::init(nodes);

    // set tests
    std::vector<std::unique_ptr<BaseTest>> testCases;
    testCases.emplace_back(std::make_unique<CoreTest>(nodes));
    testCases.emplace_back(std::make_unique<QueryTest>(nodes));
    // run tests
    for (const auto& test : testCases) {
        test->run();
    }

    // fin
    BaseTest::fin();

    return 0;
}