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

    // Initialize Hecatoncheir. Must use this method before any other calls to the framework.
    std::vector<std::string> hosts = {"node1:1", "node2:1"};
    hecatoncheir::init(2, hosts);

    // load data

    // run query

    // finalize
    hecatoncheir::finalize();


    return 0;
}