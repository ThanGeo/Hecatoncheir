#include <Hecatoncheir.h>

int main(int argc, char* argv[]) {
    /** Your hosts list. It is mandatory to define each node as:
     * <hostname>:1 
    */
    std::vector<std::string> hosts = {"vm1:1", "vm2:1", "vm3:1", "vm4:1", "vm5:1", "vm6:1", "vm7:1", "vm8:1", "vm9:1", "vm10:1"};

    /**
     * INIT
     * Initialize Hecatoncheir using hec::init()
     * This method must be used before any other Hecatoncheir calls.
     */
    hec::init(hosts.size(), hosts);


    /** 
     * Prepare data
     * You can prepare your data using hec::prepareDataset()
     * specify the following:
     * * path
     * * file type
     * * spatial data type
     * * true/false for persistence (under development)
     */
    int datasetID = hec::prepareDataset("path_to_dataset.wkt", "WKT", "POLYGON", false);


    /** 
     * Partition data
     * To partition your dataset you only need to call hec::partition();
     * with the dataset IDs you want to partition across the system.
     */
    int ret = hec::partition({datasetID});


    /** 
     * Index data
     * you can index your partitioned datasets by specifying their IDs and the Index type to use.
     */
    ret = hec::buildIndex({datasetID}, hec::IT_TWO_LAYER);


    /** 
     * Define a query and evaluate it/
     * Range queries can be loaded from the disk. Specify the query filepath, filetype 
     * and the dataset ID on which the queries will be evaluated on.
     * You can either count the results or collect the result ids.
     * Join queries require two dataset IDs instead and no query file.
     */
    std::vector<hec::Query *> batch = hec::loadQueriesFromFile("path_to_query.wkt", "WKT", datasetID, hec::QR_COUNT);
    std::unordered_map<int, hec::QResultBase *> results = hec::query(batch);

    /**
     * Don't forget to terminate Hecatoncheir when you are done!
     */
    hec::finalize();

    return 0;
}
