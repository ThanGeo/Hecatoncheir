#include <Hecatoncheir.h>

#include <fstream>
#include <sstream>

int main(int argc, char* argv[]) {
    /** Your hosts list. It is mandatory to define each node as:
     * <hostname>:1 
    */
    // std::vector<std::string> hosts = {"vm1:1", "vm3:1", "vm5:1", "vm7:1", "vm9:1", "vm2:1", "vm4:1", "vm6:1", "vm8:1", "vm10:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1"};
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1"};
    std::vector<std::string> hosts = {"vm1:1"};

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
    // std::string pointsPath = "/home/hec/datasets/T2_lo48_points.tsv";
    // int pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);

    std::string landmarkPath = "/home/hec/datasets/T1_lo48.tsv";
    int landmarkID = hec::prepareDataset(landmarkPath, "WKT", "POLYGON", false);
    // std::string waterPath = "/home/hec/datasets/T2_lo48.tsv";
    // int waterID = hec::prepareDataset(waterPath, "WKT", "POLYGON", false);
    // std::string roadsPath = "/home/hec/datasets/T8_lo48.tsv";
    // int roadsID = hec::prepareDataset(roadsPath, "WKT", "LINESTRING", false);


    /** 
     * Partition data
     * 
     */
    double start = hec::time::getTime();
    // int ret = hec::partition({landmarkID, waterID});
    int ret = hec::partition({landmarkID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);

    // double start = hec::time::getTime();
    // int ret = hec::partition({pointDatasetID});
    // printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);

    /** 
     * Index data
     * 
     */
    start = hec::time::getTime();
    ret = hec::buildIndex({landmarkID}, hec::IT_TWO_LAYER);
    // ret = hec::buildIndex({waterID}, hec::IT_TWO_LAYER);
    printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);

    // start = hec::time::getTime();
    // ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
    // printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);


    /** 
     * Define a query and evaluate it
     * 
     */
    // hec::JoinQuery joinQuery(landmarkID, waterID, 0, hec::Q_FIND_RELATION_JOIN, hec::QR_COLLECT);
    // start = hec::time::getTime();
    // hec::QResultBase* result = hec::query(&joinQuery);
    // printf("Query finished in %0.2f seconds.\n", hec::time::getTime() - start);
    // std::vector<std::vector<size_t>> res = result->getTopologyResultList();
    // for (int i=0; i<res.size(); i++) {
    //     printf("Relation %d count: %ld\n", i, res[i].size()/2);
        
    // }


    start = hec::time::getTime();
    std::string queriesPath = "/home/hec/datasets/USA_queries/USA_c0.01_n10000_polygons.wkt";
    // std::string queriesPath = "/home/hec/thanasis/Hecatoncheir/test_query.wkt";
    std::vector<hec::Query *> batch = hec::loadQueriesFromFile(queriesPath, "WKT", landmarkID, hec::QR_COLLECT);
    std::unordered_map<int, hec::QResultBase *> results = hec::query(batch);
    printf("Query finished in %0.2f seconds.\n", hec::time::getTime() - start);
    for (int i=0; i<batch.size(); i++) {
        std::vector<size_t> ids = results[i]->getResultList();
        printf("Query %d results: %ld\n", i, ids.size());
        for (auto &id: ids) {
            printf(" %ld", id);
        }
        printf("\n");
    }

    for (auto &it : batch) {
        delete results[it->getQueryID()];
        delete it;
    }

    /**
     * Don't forget to terminate Hecatoncheir when you are done!
     */
    hec::finalize();

    return 0;
}
