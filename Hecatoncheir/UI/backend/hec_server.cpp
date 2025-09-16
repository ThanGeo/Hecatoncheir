#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <nlohmann/json.hpp>
#include "../../API/Hecatoncheir.h" 
#include "../../include/def.h"
#include "../../include/utils.h"

using json = nlohmann::json;


std::vector<std::string> hosts;
std::string pointsPath;
std::string queriesPath;
int pointDatasetID;
int k;
std::vector<hec::Query*> batch;
std::unordered_map<int, hec::QResultBase*> results;

bool hec_initialized = false;
bool data_prepared = false;
std::string current_query_type;
double start, total_time;
int ret;

std::mutex hec_mutex;


int main() {
    std::string line;
    while (std::getline(std::cin, line)) {
        try {
            auto cmd = json::parse(line);
            std::string action = cmd["action"];
            json response;

            std::lock_guard<std::mutex> lock(hec_mutex);

            if (action == "init") {
                hosts = cmd["hosts"].get<std::vector<std::string>>();
                std::cout << hosts.size() << " " << hosts[0] << std::endl;
                ret = hec::init(hosts.size(), hosts);
                if(ret != DBERR_OK){
                    logger::log_error(ret, "AAA:");
                }
                hec_initialized = (ret == DBERR_OK);
                response["status"] = hec_initialized ? "success" : "error";

            } else if (action == "prepare") {
                current_query_type = cmd["queryType"];
                std::string dataset = cmd.value("dataset", "");
                std::string queryDataset = cmd.value("queryDataset", "");
                k = cmd.value("kValue", 5);

                if (!hec_initialized) {
                    response["status"] = "error";
                    response["message"] = "HEC not initialized";
                } else if (current_query_type == "knnQuery") {
                    pointsPath = dataset;
                    pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);
                    start = hec::time::getTime();
                    ret = hec::partition({pointDatasetID});
                    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
                    queriesPath = queryDataset;
                    batch = hec::loadKNNQueriesFromFile(queriesPath, "WKT", pointDatasetID, k);
                    data_prepared = true;
                    response["status"] = "success";
                }
                // ... handle rangeQuery and spatialJoins similarly

            } else if (action == "execute") {
                if (!data_prepared) {
                    response["status"] = "error";
                    response["message"] = "Data not prepared";
                } else {
                    start = hec::time::getTime();
                    // ret = hec::query(batch, ...) based on current_query_type
                    total_time = hec::time::getTime() - start;

                    // cleanup batch
                    for (auto &q : batch) {
                        delete results[q->getQueryID()];
                        delete q;
                    }
                    batch.clear();
                    results.clear();
                    hec::unloadDataset(pointDatasetID);

                    data_prepared = false;
                    current_query_type = "";

                    response["status"] = "success";
                    response["executionTime"] = total_time;
                }

            } else if (action == "terminate") {
                if (hec_initialized) {
                    hec::finalize();
                    hec_initialized = false;
                    data_prepared = false;
                }
                response["status"] = "terminated";
                std::cout << response.dump() << std::endl;
                break;

            } else {
                response["status"] = "error";
                response["message"] = "Unknown action";
            }

            std::cout << response.dump() << std::endl;

        } catch (const std::exception& e) {
            json err;
            err["status"] = "error";
            err["message"] = e.what();
            std::cout << err.dump() << std::endl;
        }
    }
    return 0;
}
