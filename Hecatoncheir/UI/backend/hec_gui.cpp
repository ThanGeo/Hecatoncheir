#include "hec_gui.h"

vector<string> hosts;
string pointsPath;
int pointDatasetID;
int ret;
string queriesPath;
vector<hec::Query *> batch;
unordered_map<int, hec::QResultBase *> results;
int k;
double start;
double total_time;

void printUsage() {
    cout << "Commands:" << endl;
    cout << "  init <host1> <host2> ...          Initialize HEC cluster" << endl;
    cout << "  prepare --queryType <type> [options]  Prepare data for queries" << endl;
    cout << "  execute                           Execute queries" << endl;
    cout << "  terminate                         Terminate HEC cluster" << endl;
    cout << endl;
    cout << "Prepare options:" << endl;
    cout << "  --queryType rangeQuery|knnQuery|spatialJoins" << endl;
    cout << "  --dataset <path>                  Dataset file path" << endl;
    cout << "  --queryDataset <path>             Query dataset file path" << endl;
    cout << "  --leftDataset <path>              Left dataset file path (for spatial joins)" << endl;
    cout << "  --rightDataset <path>             Right dataset file path (for spatial joins)" << endl;
    cout << "  --spatialDataType <type>          Spatial data type" << endl;
    cout << "  --querySetType <type>             Query set type" << endl;
    cout << "  --kValue <value>                  K value for KNN queries" << endl;
    cout << "  --predicate <predicate>           Predicate for spatial joins" << endl;
}

vector<string> parseArguments(int argc, char* argv[]) {
    vector<string> args;
    for (int i = 0; i < argc; ++i) {
        args.push_back(argv[i]);
    }
    return args;
}

string getArgumentValue(const vector<string>& args, const string& flag) {
    for (size_t i = 0; i < args.size() - 1; ++i) {
        if (args[i] == flag) {
            return args[i + 1];
        }
    }
    return "";
}

int handleInitCommand(const vector<string>& args) {
    cout << "Initializing HEC cluster..." << endl;
    
    for (size_t i = 2; i < args.size(); ++i) {
        string temp_name = args[i];
        hosts.push_back(temp_name + ":1");
        cout << "Added host: " << hosts.back() << endl;
    }
    
    hec::init(hosts.size(), hosts);
    cout << "HEC initialized with " << hosts.size() << " hosts" << endl;
    return 0;
}

int handlePrepareCommand(const vector<string>& args) {
    cout << "Preparing data..." << endl;
    
    string queryType = getArgumentValue(args, "--queryType");
    string dataset = getArgumentValue(args, "--dataset");
    string queryDataset = getArgumentValue(args, "--queryDataset");
    string leftDataset = getArgumentValue(args, "--leftDataset");
    string rightDataset = getArgumentValue(args, "--rightDataset");
    string spatialDataType = getArgumentValue(args, "--spatialDataType");
    string querySetType = getArgumentValue(args, "--querySetType");
    string kValue = getArgumentValue(args, "--kValue");
    string predicate = getArgumentValue(args, "--predicate");

    cout << "Query Type: " << queryType << endl;
    
    if (queryType == "knnQuery") {
        pointsPath = dataset;
        pointDatasetID = hec::prepareDataset(pointsPath, "WKT", "POINT", false);
        
        // Partition
        start = hec::time::getTime();
        ret = hec::partition({pointDatasetID});
        printf("Partitioning finished in %0.2f seconds.\n", hec::time::getTime() - start);
        
        // Index
        start = hec::time::getTime();
        ret = hec::buildIndex({pointDatasetID}, hec::IT_UNIFORM_GRID);
        printf("Indexes built in %0.2f seconds.\n", hec::time::getTime() - start);
        
        // Load queries
        queriesPath = queryDataset;
        k = kValue.empty() ? 5 : stoi(kValue); // default k=5
        batch = hec::loadKNNQueriesFromFile(queriesPath, "WKT", pointDatasetID, k);
        
        cout << "KNN preparation completed for k=" << k << endl;
    }
    else if (queryType == "rangeQuery") {
        cout << "Range query preparation completed" << endl;
    }
    else if (queryType == "spatialJoins") {
        cout << "Spatial joins preparation completed" << endl;
    }
    else {
        cerr << "Unknown query type: " << queryType << endl;
        return 1;
    }
    
    return 0;
}

int handleExecuteCommand(const vector<string>& args) {
    cout << "Executing queries..." << endl;
    
    total_time = 0;
    start = hec::time::getTime();
    results = hec::query(batch, hec::Q_KNN);
    total_time += hec::time::getTime() - start;
    
    printf("Query execution finished in %0.2f seconds.\n", total_time);
    
    // Cleanup - Do we really want it?
    for (auto &it : batch) {
        delete results[it->getQueryID()];
        delete it;
    }
    
    batch.clear();
    results.clear();
    
    // Unload dataset
    ret = hec::unloadDataset(pointDatasetID);
    
    cout << "Execution completed successfully" << endl;
    return 0;
}

int handleTerminateCommand(const vector<string>& args) {
    cout << "Terminating HEC cluster..." << endl;
    
    hec::finalize();
    cout << "HEC cluster terminated" << endl;
    return 0;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printUsage();
        return 1;
    }

    vector<string> args = parseArguments(argc, argv);
    string command = args[1];

    try {
        if (command == "init") {
            return handleInitCommand(args);
        } else if (command == "prepare") {
            return handlePrepareCommand(args);
        } else if (command == "execute") {
            return handleExecuteCommand(args);
        } else if (command == "terminate") {
            return handleTerminateCommand(args);
        } else {
            cerr << "Unknown command: " << command << endl;
            printUsage();
            return 1;
        }
    } catch (const exception& e) {
        cerr << "Error executing command '" << command << "': " << e.what() << endl;
        return 1;
    }

    return 0;
}