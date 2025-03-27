#include <Hecatoncheir.h>

#include <fstream>
#include <sstream>

int main(int argc, char* argv[]) {
    /** Your hosts list. It is mandatory to define each node as:
     * <hostname>:1 
    */
    // std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1", "vm5:1", "vm6:1", "vm9:1", "vm10:1"};
    std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1"};
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
    // std::string pointsPath = "/home/hec/datasets/T2_lo48_points.tsv";
    // int pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);
    // std::string polygonsPath = "/home/hec/datasets/T2_lo48.tsv";
    // int pointDatasetID = hec::prepareDataset(polygonsPath, "WKT", "POLYGON", false);

    std::string waterPath = "/home/hec/datasets/T2_lo48.tsv";
    int waterID = hec::prepareDataset(waterPath, "WKT", "POLYGON", false);
    std::string roadsPath = "/home/hec/datasets/T8_lo48.tsv";
    int roadsID = hec::prepareDataset(roadsPath, "WKT", "LINESTRING", false);


    /** 
     * Partition data
     * 
     */
    double start = hec::time::getTime();
    int ret = hec::partition({waterID, roadsID});
    printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);

    /** 
     * Index data
     * 
     */
    ret = hec::buildIndex({waterID}, hec::IT_TWO_LAYER);
    ret = hec::buildIndex({roadsID}, hec::IT_TWO_LAYER);


    /** 
     * Define a query and evaluate it
     * 
     */
    hec::JoinQuery joinQuery(waterID, roadsID, 0, hec::Q_INTERSECTION_JOIN, hec::QR_COUNT);
    
    start = hec::time::getTime();
    hec::QueryResult result = hec::query(&joinQuery);
    printf("Query finished in %0.2f seconds.\n", hec::time::getTime() - start);

    result.print();

    /**
     * Don't forget to terminate Hecatoncheir when you are done!
     */
    hec::finalize();

    return 0;
}
