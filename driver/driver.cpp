#include <Hecatoncheir.h>

#include <fstream>
#include <sstream>
#include <unistd.h>

void spatialJoinScenario() {
    // prepare
    std::string landmarkPath = "/home/hec/datasets/T1_lo48.tsv";
    int landmarkID = hec::prepareDataset(landmarkPath, "WKT", "POLYGON", false);
    std::string waterPath = "/home/hec/datasets/T2_lo48.tsv";
    int waterID = hec::prepareDataset(waterPath, "WKT", "POLYGON", false);
    // partition
    double start = hec::time::getTime();
    int ret = hec::partition({landmarkID, waterID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // index
    start = hec::time::getTime();
    ret = hec::buildIndex({landmarkID, waterID}, hec::IT_TWO_LAYER);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
    // query
    hec::PredicateJoinQuery joinQuery(landmarkID, waterID, 0, hec::Q_INTERSECTION_JOIN, hec::QR_COUNT);
    start = hec::time::getTime();
    hec::QResultBase* result = hec::query(&joinQuery);
    double total_time = hec::time::getTime() - start;
    // results
    size_t resCount = result->getResultCount();
    printf("Results: %ld\n", resCount);
    printf("Query finished in %0.2f seconds.\n", total_time);
    delete result;

    // // unload
    ret = hec::unloadDataset(landmarkID);
    ret = hec::unloadDataset(waterID);
}

void spatialJoinScenario2() {
    // prepare
    std::string path1 = "/home/hec/datasets/OSM/O5_lakes.tsv";
    int id1 = hec::prepareDataset(path1, "WKT", "POLYGON", false);
    std::string path2 = "/home/hec/datasets/OSM/O6_parks.tsv";
    int id2 = hec::prepareDataset(path2, "WKT", "POLYGON", false);
    // partition
    double start = hec::time::getTime();
    int ret = hec::partition({id1, id2});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // index
    start = hec::time::getTime();
    ret = hec::buildIndex({id1, id2}, hec::IT_TWO_LAYER);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
    // query
    hec::PredicateJoinQuery joinQuery(id1, id2, 0, hec::Q_INTERSECTION_JOIN, hec::QR_COUNT);
    start = hec::time::getTime();
    hec::QResultBase* result = hec::query(&joinQuery);
    double total_time = hec::time::getTime() - start;
    // results
    size_t resCount = result->getResultCount();
    printf("Results: %ld\n", resCount);
    printf("Query finished in %0.2f seconds.\n", total_time);
    delete result;

    // unload
    ret = hec::unloadDataset(id1);
    ret = hec::unloadDataset(id2);
}

void singleKNNScenario() {
    // prepare
    std::string pointsPath = "/home/hec/datasets/T2_lo48_points.tsv";
    int pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);
    // partition
    double start = hec::time::getTime();
    int ret = hec::partition({pointDatasetID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // index
    start = hec::time::getTime();
    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
    // query
    hec::KNNQuery kNNQuery(pointDatasetID, 0, "POINT (-87.905635 32.897007)", 5);
    start = hec::time::getTime();
    // results
    hec::QResultBase* result = hec::query(&kNNQuery);
    double total_time = hec::time::getTime() - start;
    std::vector<size_t> res = result->getResultList();
    printf("Results:\n");
    for (auto &it : res) {
        printf(" %ld,", it);
    }
    delete result;
    printf("\n");
    printf("Query finished in %0.5f seconds.\n", total_time);
    // unload
    ret = hec::unloadDataset(pointDatasetID);
}

void batchKNNScenario() {
    /**
     * Batch KNN QUERIES
     */
    // prepare
    // std::string pointsPath = "/home/hec/datasets/T2_lo48_points.tsv";
    std::string pointsPath = "/home/hec/datasets/OSM/O3_points.wkt";
    int pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);
    // partition
    double start = hec::time::getTime();
    int ret = hec::partition({pointDatasetID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // index
    start = hec::time::getTime();
    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
    // queries batch
    std::string queriesPath = "/home/hec/datasets/USA_queries/NN_queries.wkt";
    int k = 5;
    std::vector<hec::Query *> batch = hec::loadKNNQueriesFromFile(queriesPath, "WKT", pointDatasetID, k);
    double total_time = 0;
    start = hec::time::getTime();
    std::unordered_map<int, std::unique_ptr<hec::QResultBase>> results = hec::query(batch, hec::Q_KNN);
    total_time += hec::time::getTime() - start;
    // results
    for (auto &it : batch) {
        // std::vector<size_t> ids = results[it->getQueryID()]->getResultList();
        // printf("Query %d results for k=%d:\n", it->getQueryID(), k);
        // for (auto &id : ids) {
        //     printf(" %ld", id);
        // }
        // printf("\n");
        delete it;
    }
    batch.clear();
    results.clear();
    printf("Query finished in %0.5f seconds.\n", total_time);
    // unload
    ret = hec::unloadDataset(pointDatasetID);
}

void batchRangeScenario() {
    // prepare
    std::string pointsPath = "/home/hec/datasets/T2_lo48_points.tsv";
    int pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);
    // partition
    double start = hec::time::getTime();
    int ret = hec::partition({pointDatasetID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // index
    start = hec::time::getTime();
    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
    // query
    std::string queriesPath = "/home/hec/datasets/USA_queries/USA_c0.01_n10000_polygons.wkt";
    std::vector<hec::Query *> batch = hec::loadRangeQueriesFromFile(queriesPath, "WKT", pointDatasetID, hec::QR_COUNT);
    double total_time = 0;
    start = hec::time::getTime();
    std::unordered_map<int, std::unique_ptr<hec::QResultBase>> results = hec::query(batch, hec::Q_RANGE);
    total_time += hec::time::getTime() - start;
    printf("Query %d results: %ld\n", 5, results[5]->getResultCount());
    for (auto &it : batch) {
        // printf("Query %d results: %ld\n", it->getQueryID(), results[it->getQueryID()]->getResultCount());
        delete it;
    }
    results.clear();
    batch.clear();
    printf("Query finished in %0.5f seconds.\n", total_time);
    // unload
    ret = hec::unloadDataset(pointDatasetID);
}

void distanceJoinScenario() {
    // prepare
    std::string pathR = "/home/hec/datasets/T2_lo48_points.tsv";
    int RID = hec::prepareDataset(pathR, "WKT", "POINT", false);
    std::string pathS = "/home/hec/datasets/USA_queries/NN_queries.wkt";
    int SID = hec::prepareDataset(pathS, "WKT", "POINT", false);
    // partition
    double start = hec::time::getTime();
    int ret = hec::partition({RID, SID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // index
    start = hec::time::getTime();
    ret = hec::buildIndex({RID, SID}, hec::IT_UNIFORM_GRID);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
    // query
    hec::DistanceJoinQuery distanceQuery(RID, SID, 0, hec::QR_COLLECT, 0.001);
    start = hec::time::getTime();
    hec::QResultBase* result = hec::query(&distanceQuery);
    double total_time = hec::time::getTime() - start;
    // results
    std::vector<size_t> results = result->getResultList();
    // for (int i=0; i<results.size(); i+=2) {
    //     printf("%ld,%ld\n", results[i], results[i+1]);
    // }
    printf("Results: %ld\n", results.size()/2);

    printf("Query finished in %0.5f seconds.\n", total_time);
    delete result;

    // unload
    ret = hec::unloadDataset(RID);
    ret = hec::unloadDataset(SID);
}

int main(int argc, char* argv[]) {
    /** Your hosts list. It is mandatory to define each node as:
     * <hostname>:1 
    */
    std::vector<std::string> hosts = {"vm1:1", "vm3:1", "vm5:1", "vm7:1", "vm9:1", "vm2:1", "vm4:1", "vm6:1", "vm8:1", "vm10:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm3:1", "vm5:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm3:1"};
    // std::vector<std::string> hosts = {"vm1:1"};

    /**
     * INIT
     * Initialize Hecatoncheir using hec::init()
     * This method must be used before any other Hecatoncheir calls.
     */
    hec::init(hosts.size(), hosts);

    int RUNS = 1;
    for (int i=0; i<RUNS; i++) {
        // printf("Run %d: Running Join Scenario...\n", i);
        // spatialJoinScenario();
        // printf("Run %d: Running OSM Lakes-Parks Join Scenario...\n", i);
        // spatialJoinScenario2();
        // printf("Run %d: Running batch KNN Scenario...\n", i);
        // batchKNNScenario();
        // printf("Run %d: Running batch range Scenario...\n", i);
        // batchRangeScenario();
        printf("Run %d: Running Distance Scenario...\n", i);
        distanceJoinScenario();
    }
    
    /**
     * Don't forget to terminate Hecatoncheir when you are done!
     */
    hec::finalize();

    return 0;
}
