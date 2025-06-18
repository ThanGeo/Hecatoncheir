#include <Hecatoncheir.h>

#include <fstream>
#include <sstream>
#include <unistd.h>

int main(int argc, char* argv[]) {
    /** Your hosts list. It is mandatory to define each node as:
     * <hostname>:1 
    */
    std::vector<std::string> hosts = {"vm1:1", "vm3:1", "vm5:1", "vm7:1", "vm9:1", "vm2:1", "vm4:1", "vm6:1", "vm8:1", "vm10:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1"};
    // std::vector<std::string> hosts = {"vm1:1"};

    /**
     * INIT
     * Initialize Hecatoncheir using hec::init()
     * This method must be used before any other Hecatoncheir calls.
     */
    hec::init(hosts.size(), hosts);


    /** 
     * Prepare data
     * You can prepare your data using hec::prepareDataset()
     * and then partition it using hec::partition();
     */
    std::string pointsPath = "/home/hec/datasets/T2_lo48_points.tsv";
    int pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);

    // std::string landmarkPath = "/home/hec/datasets/T1_lo48.tsv";
    // int landmarkID = hec::prepareDataset(landmarkPath, "WKT", "POLYGON", false);
    // std::string waterPath = "/home/hec/datasets/T2_lo48.tsv";
    // int waterID = hec::prepareDataset(waterPath, "WKT", "POLYGON", false);
    // std::string roadsPath = "/home/hec/datasets/T8_lo48.tsv";
    // int roadsID = hec::prepareDataset(roadsPath, "WKT", "LINESTRING", false);


    /** 
     * Partition data
     * 
     */
    // double start = hec::time::getTime();
    // int ret = hec::partition({landmarkID, waterID});
    // // int ret = hec::partition({pointDatasetID});
    // printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);

    double start = hec::time::getTime();
    int ret = hec::partition({pointDatasetID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);

    /** 
     * Index data
     * 
     */
    start = hec::time::getTime();
    // ret = hec::buildIndex({landmarkID, waterID}, hec::IT_TWO_LAYER);
    // ret = hec::buildIndex({roadsID}, hec::IT_TWO_LAYER);
    ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);

    // start = hec::time::getTime();
    // ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
    // printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);


    /**
     * JOIN QUERIES
     */
    // hec::JoinQuery joinQuery(landmarkID, waterID, 0, hec::Q_FIND_RELATION_JOIN, hec::QR_COLLECT);
    // start = hec::time::getTime();
    // hec::QResultBase* result = hec::query(&joinQuery);
    // double total_time = hec::time::getTime() - start;
    // std::vector<std::vector<size_t>> res = result->getTopologyResultList();
    // for (int i=0; i<res.size(); i++) {
    //     printf("Relation %d count: %ld\n", i, res[i].size()/2);
    // }
    // printf("Query finished in %0.2f seconds.\n", total_time);


    // std::string queriesPath = "/home/hec/datasets/USA_queries/USA_c0.01_n10000_polygons.wkt";
    // // std::string queriesPath = "/home/hec/thanasis/Hecatoncheir/test_query.wkt";
    // std::vector<hec::Query *> batch = hec::loadRangeQueriesFromFile(queriesPath, "WKT", pointDatasetID, hec::QR_COUNT);
    // int RUNS = 1;
    // double total_time = 0;
    // for (int i=0; i<RUNS; i++) {
    //     start = hec::time::getTime();
    //     std::unordered_map<int, hec::QResultBase *> results = hec::query(batch);
    //     total_time += hec::time::getTime() - start;
    //     for (auto &it : batch) {
    //         printf("Query %d results: %ld\n", it->getQueryID(), results[it->getQueryID()]->getResultCount());
    //         delete results[it->getQueryID()];
    //     }
    //     results.clear();
    // }
    // printf("Query finished in %0.2f seconds.\n", total_time/(double)RUNS);
    
    // for (int i=0; i<batch.size(); i++) {
    //     std::vector<size_t> ids = results[i]->getResultList();
    //     printf("Query %d results: %ld\n", i, ids.size());
    //     for (auto &id: ids) {
    //         printf(" %ld", id);
    //     }
    //     printf("\n");
    // }
    // for (int i=0; i<batch.size(); i++) {
    //     printf("Query %d results: %ld\n", i, results[i]->getResultCount());
    // }

    // for (auto &it : batch) {
    //     // delete results[it->getQueryID()];
    //     delete it;
    // }

    /**
     * KNN QUERIES
     */

    // hec::KNNQuery kNNQuery(pointDatasetID, 0, "POINT (-87.905635 32.897007)", 5);
    // start = hec::time::getTime();
    // hec::QResultBase* result = hec::query(&kNNQuery);
    // double end = hec::time::getTime() - start;
    // std::vector<size_t> res = result->getResultList();
    // printf("Results:\n");
    // for (auto &it : res) {
    //     printf(" %ld,", it);
    // }
    // printf("\n");

    // printf("Query finished in %0.5f seconds.\n", end);

    /**
     * Batch KNN QUERIES
     */
    std::string queriesPath = "/home/hec/datasets/USA_queries/NN_queries.wkt";
    int k = 5;
    std::vector<hec::Query *> batch = hec::loadKNNQueriesFromFile(queriesPath, "WKT", pointDatasetID, k);
    int RUNS = 1;
    double total_time = 0;
    for (int i=0; i<RUNS; i++) {
        start = hec::time::getTime();
        std::vector<hec::Query *> temp = {batch[0]};
        std::unordered_map<int, hec::QResultBase *> results = hec::query(temp);
        total_time += hec::time::getTime() - start;
        for (auto &it : temp) {
            std::vector<size_t> ids = results[it->getQueryID()]->getResultList();
            printf("Query %d results for k=%d:\n", it->getQueryID(), k);
            for (auto &id : ids) {
                printf(" %ld", id);
            }
            printf("\n");
            delete results[it->getQueryID()];
        }
        results.clear();
    }
    printf("Query finished in %0.5f seconds.\n", total_time/(double)RUNS);

    /**
     * Don't forget to terminate Hecatoncheir when you are done!
     */
    hec::finalize();

    return 0;
}
