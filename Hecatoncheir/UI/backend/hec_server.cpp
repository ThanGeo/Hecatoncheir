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
int pointDatasetID = -1;
int secondPointDatasetID = -1;
int predicateValue;
int k;
std::string fileFormat;
std::string geometryType;
std::string queryFileFormat;
std::string queryGeometryType;
std::vector<hec::Query*> batch;
std::unordered_map<int, hec::QResultBase*> results;
hec::DistanceJoinQuery* distanceQuery = nullptr;

bool hec_initialized = false;
bool data_prepared = false;
std::string current_query_type;
double start, total_time;
int ret;
std::mutex hec_mutex;

int main() {
    std::ios::sync_with_stdio(false);
    std::cout.setf(std::ios::unitbuf);

    std::string line;
    while (std::getline(std::cin, line)) {
        try {
            auto cmd = json::parse(line);
            std::string action = cmd["action"];
            json response;
            std::lock_guard<std::mutex> lock(hec_mutex);
            if (action == "init") {
                hosts = cmd["hosts"].get<std::vector<std::string>>();
                ret = hec::init(hosts.size(), hosts);
                if(ret != DBERR_OK){
                    logger::log_error(ret, "Error in init:");
                }
                hec_initialized = (ret == DBERR_OK);
                response["status"] = hec_initialized ? "success" : "error";
            } 
            else if (action == "prepare") {
                if (data_prepared) {
                    if (pointDatasetID >= 0) {
                        hec::unloadDataset(pointDatasetID);
                        pointDatasetID = -1;
                    }
                    if (secondPointDatasetID >= 0) {
                        hec::unloadDataset(secondPointDatasetID);
                        secondPointDatasetID = -1;
                    }
                    if (distanceQuery != nullptr) {
                        delete distanceQuery;
                        distanceQuery = nullptr;
                    }
                    for (auto& q : batch) {
                        auto it = results.find(q->getQueryID());
                        if (it != results.end()) {
                            delete it->second;
                        }
                        delete q;
                    }
                    batch.clear();
                    results.clear();
                    data_prepared = false;
                }
                current_query_type = cmd["queryType"];
                std::string dataset = cmd.value("dataset", "");
                std::string queryDataset = cmd.value("queryDataset", "");
                k = cmd.value("kValue", 5);
                // Extract format and geometry type from command
                fileFormat = cmd.value("fileFormat", "WKT");
                geometryType = cmd.value("geometryType", "POINT");
                queryFileFormat = cmd.value("queryFileFormat", "WKT");
                queryGeometryType = cmd.value("queryGeometryType", "POINT");
                if (!hec_initialized) {
                    response["status"] = "error";
                    response["message"] = "HEC not initialized";
                } 
                else if (current_query_type == "knnQuery") {
                    pointsPath = dataset;
                    pointDatasetID = hec::prepareDataset(pointsPath, fileFormat, geometryType, false);
                    start = hec::time::getTime();
                    ret = hec::partition({pointDatasetID});
                    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
                    queriesPath = queryDataset;
                    batch = hec::loadKNNQueriesFromFile(queriesPath, queryFileFormat, pointDatasetID, k);
                    data_prepared = true;
                    response["status"] = "success";
                } 
                else if (current_query_type == "rangeQuery") {
                    pointsPath = dataset;
                    pointDatasetID = hec::prepareDataset(pointsPath, fileFormat, geometryType, false);
                    start = hec::time::getTime();
                    ret = hec::partition({pointDatasetID});
                    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
                    queriesPath = queryDataset;
                    batch = hec::loadRangeQueriesFromFile(queriesPath, queryFileFormat, pointDatasetID, hec::QR_COUNT);
                    data_prepared = true;
                    response["status"] = "success";
                } 
                else if (current_query_type == "spatialJoins") {
                    std::string leftDataset = cmd.value("leftDataset", "");
                    std::string rightDataset = cmd.value("rightDataset", "");
                    
                    std::string leftFileFormat = cmd.value("leftFileFormat", "WKT");
                    std::string leftGeometryType = cmd.value("leftGeometryType", "POINT");
                    std::string rightFileFormat = cmd.value("rightFileFormat", "WKT");
                    std::string rightGeometryType = cmd.value("rightGeometryType", "POINT");

                                       
                    pointsPath = leftDataset;
                    pointDatasetID = hec::prepareDataset(pointsPath, leftFileFormat, leftGeometryType, false);
                    secondPointDatasetID = hec::prepareDataset(rightDataset, rightFileFormat, rightGeometryType, false);
                    
                    double start = hec::time::getTime();
                    int ret = hec::partition({pointDatasetID, secondPointDatasetID});
                    
                    start = hec::time::getTime();
                    ret = hec::buildIndex({pointDatasetID, secondPointDatasetID}, hec::IT_UNIFORM_GRID);
                    distanceQuery = new hec::DistanceJoinQuery(pointDatasetID, secondPointDatasetID, 0, hec::QR_COLLECT, 0.001);
                    data_prepared = true;
                    response["status"] = "success";
                }
            } 

            else if (action == "execute") {
                if (!data_prepared) {
                    response["status"] = "error";
                    response["message"] = "Data not prepared";
                } 
                else if (current_query_type == "knnQuery") {
                    start = hec::time::getTime();
                    results = hec::query(batch, hec::Q_KNN);
                    total_time = hec::time::getTime() - start;
                    response["status"] = "success";
                    response["executionTime"] = total_time;
                    std::string timestamp = std::to_string(std::time(nullptr));         
                    json results_json;
                    results_json["timestamp"] = timestamp;
                    results_json["executionTime"] = total_time;
                    results_json["queryType"] = current_query_type;
                    json results_array = json::array();
                    for (const auto& [queryID, resultPtr] : results) {
                        if (resultPtr) {
                            json result_item;
                            result_item["queryID"] = resultPtr->getQueryID();
                            result_item["queryType"] = resultPtr->getQueryType();
                            result_item["resultType"] = resultPtr->getResultType();
                            result_item["resultCount"] = resultPtr->getResultCount();
                            result_item["neighbors"] = resultPtr->getResultList();
                            results_array.push_back(result_item);
                        }
                    }
                    
                    results_json["results"] = results_array;
                    results_json["totalResults"] = results.size();
                    response["resultsData"] = results_json;
                } 
                else if (current_query_type == "rangeQuery") {
                    start = hec::time::getTime();
                    results = hec::query(batch, hec::Q_RANGE);
                    total_time = hec::time::getTime() - start;
                
                    response["status"] = "success";
                    response["executionTime"] = total_time;
                    
                    try {
                        std::string timestamp = std::to_string(std::time(nullptr));
                        
                        json results_json;
                        results_json["timestamp"] = timestamp;
                        results_json["executionTime"] = total_time;
                        results_json["queryType"] = current_query_type;
                        
                        json results_array = json::array();
                        for (const auto& [queryID, resultPtr] : results) {
                            if (resultPtr) {
                                json result_item;
                                result_item["queryID"] = resultPtr->getQueryID();
                                result_item["queryType"] = resultPtr->getQueryType();
                                result_item["resultType"] = resultPtr->getResultType();
                                result_item["resultCount"] = resultPtr->getResultCount();
                                results_array.push_back(result_item);
                            }
                        }
                        
                        results_json["results"] = results_array;
                        results_json["totalResults"] = results.size();
                        response["resultsData"] = results_json;
                    } 
                    catch (const std::exception& e) {
                        std::cerr << "ERROR building results: " << e.what() << std::endl;
                        response["resultsError"] = e.what();
                    }
                } 
                else if (current_query_type == "spatialJoins") {
                    start = hec::time::getTime();
                    hec::QResultBase* result = hec::query(distanceQuery);
                    double total_time = hec::time::getTime() - start;          
                    response["status"] = "success";
                    response["executionTime"] = total_time;
                    std::string timestamp = std::to_string(std::time(nullptr));
                    
                    json results_json;
                    results_json["timestamp"] = timestamp;
                    results_json["executionTime"] = total_time;
                    results_json["queryType"] = current_query_type;
                    
                    if (result) {
                        json result_item;
                        result_item["queryID"] = result->getQueryID();
                        result_item["queryType"] = result->getQueryType();
                        result_item["resultType"] = result->getResultType();
                        result_item["resultCount"] = result->getResultCount();
                        result_item["joinResults"] = result->getResultList();
                        
                        results_json["results"] = json::array({ result_item });
                        results_json["totalResults"] = 1;
                    } else {
                        results_json["results"] = json::array();
                        results_json["totalResults"] = 0;
                    }
                    
                    response["resultsData"] = results_json;
                } 
                else {
                    response["status"] = "error";
                    response["message"] = "Unknown query type";
                }
            } 
            else if (action == "clear") {
                if (hec_initialized) {
                    for (auto& q : batch) {
                        auto it = results.find(q->getQueryID());
                        if (it != results.end()) {
                            delete it->second;
                        }
                        delete q;
                    }
                    batch.clear();
                    results.clear();
                    
                    // Unload both datasets if they were loaded
                    if (pointDatasetID >= 0) {
                        hec::unloadDataset(pointDatasetID);
                        pointDatasetID = -1;
                    }
                    if (secondPointDatasetID >= 0) {
                        hec::unloadDataset(secondPointDatasetID);
                        secondPointDatasetID = -1;
                    }
                    
                    // Clean up distance query if it exists
                    if (distanceQuery != nullptr) {
                        delete distanceQuery;
                        distanceQuery = nullptr;
                    }
                    
                    current_query_type = "";
                    data_prepared = false;
                }
                response["status"] = "clear";
            }
            
            else if (action == "terminate") {
                if (hec_initialized) {
                    for (auto& q : batch) {
                        auto it = results.find(q->getQueryID());
                        if (it != results.end()) {
                            delete it->second;
                        }
                        delete q;
                    }
                    batch.clear();
                    results.clear();
                    
                    // Unload datasets if they were loaded
                    if (pointDatasetID >= 0) {
                        hec::unloadDataset(pointDatasetID);
                        pointDatasetID = -1;
                    }
                    if (secondPointDatasetID >= 0) {
                        hec::unloadDataset(secondPointDatasetID);
                        secondPointDatasetID = -1;
                    }
                    
                    // Clean up distance query if it exists
                    if (distanceQuery != nullptr) {
                        delete distanceQuery;
                        distanceQuery = nullptr;
                    }
                    
                    data_prepared = false;
                    current_query_type = "";
                    hec::finalize();
                    hec_initialized = false;
                }
                response["status"] = "terminated";
                std::cout << "C++: " << response.dump() << std::endl;
                std::cout.flush();
                break;
            }            else {
                response["status"] = "error";
                response["message"] = "Unknown action";
            }
            std::cout << "C++: " << response.dump() << std::endl;
            std::cout.flush();

        } catch (const std::exception& e) {
            json err;
            err["status"] = "error";
            err["message"] = e.what();
            std::cout << "C++: " << err.dump() << std::endl;
            std::cout.flush();
        }
    }
    return 0;
}
