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
     * and then partition it using hec::partition();
     */



    /** 
     * Partition data
     * 
     */



    /** 
     * Index data
     * 
     */



    /** 
     * Define a query and evaluate it
     * 
     */



    /**
     * Don't forget to terminate Hecatoncheir when you are done!
     */
    hec::finalize();

    return 0;
}
