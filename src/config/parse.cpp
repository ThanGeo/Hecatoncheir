#include "config/parse.h"

namespace parser
{
    std::unordered_map<std::string, PartitioningTypeE> partitioningTypeStrToIntMap = {{"RR",PARTITIONING_ROUND_ROBIN}};

    // property tree var
    static boost::property_tree::ptree system_config_pt;
    static boost::property_tree::ptree dataset_config_pt;

    /**
     * @brief parses the selected actions and puts them in proper order inside the configuration
     * 
     * @param actionsStmt 
     * @return DB_STATUS 
     */
    static DB_STATUS parseActions(ActionsStatementT *actionsStmt) {
        // partitioning always first
        if (actionsStmt->performPartitioning) {
            ActionT action(ACTION_PERFORM_PARTITIONING);
            g_config.actions.emplace_back(action);
        }
        // approximation
        if (actionsStmt->createAPRIL) {
            ActionT action(ACTION_CREATE_APRIL);
            g_config.actions.emplace_back(action);
        }

        // verification always last
        if (actionsStmt->performVerification) {
            ActionT action(ACTION_PERFORM_VERIFICATION);
            g_config.actions.emplace_back(action);
        }

        return DBERR_OK;
    }

    /**
     * @brief adjust the number of partitions based on how many nodes are there 
     * to improve load balancing
     * 
     */
    static int adjustPartitions(int partitionsPerDimension) {
        int newPartitionsPerDimension = partitionsPerDimension;
        int modCells = partitionsPerDimension % g_world_size;
        int halfWorldSize = g_world_size / 2;
        if (modCells != halfWorldSize) {
            newPartitionsPerDimension += halfWorldSize - modCells + 1;
        }
        return newPartitionsPerDimension;
    }

    static DB_STATUS parsePartitioningOptions() {
        std::string partitioningTypeStr = system_config_pt.get<std::string>("Partitioning.type");
        int partitionsPerDimension = system_config_pt.get<int>("Partitioning.partitionsPerDimension");
        std::string assignmentFuncStr = system_config_pt.get<std::string>("Partitioning.assignmentFunc");

        // partitioning type
        auto it = partitioningTypeStrToIntMap.find(partitioningTypeStr);
        if(it == partitioningTypeStrToIntMap.end()) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Unkown partitioning type in config file. Type:", partitioningTypeStr);
            return DBERR_INVALID_PARAMETER;
        }
        g_config.partitioningInfo.type = it->second;

        // partitions per dimension
        if (partitionsPerDimension < g_world_size) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Partitions per dimension are not at least as many as the processes:", partitionsPerDimension);
            return DBERR_INVALID_PARAMETER;
        }
        g_config.partitioningInfo.partitionsPerDimension = partitionsPerDimension;

        // node assignment to cells
        if (assignmentFuncStr == "OP") {
            // optimized, adjust the partitions per num automatically to improve load balancing
            g_config.partitioningInfo.partitionsPerDimension = adjustPartitions(partitionsPerDimension);
            logger::log_task("Adjusted partitions per dimension to", g_config.partitioningInfo.partitionsPerDimension);
            // partitioning::printPartitionAssignment();
        } else if(assignmentFuncStr == "ST") {
            // standard, do nothing extra on the grid
        } else {
            logger::log_error(DBERR_INVALID_PARAMETER, "Unknown enumeration function string:", assignmentFuncStr);
            return DBERR_INVALID_PARAMETER;
        }


        return DBERR_OK;
    }

    static DB_STATUS parseDatasetOptions(QueryStatementT *queryStmt) {
        // check if datasets.ini file exists
        if (!verifyFileExists(g_config.dirPaths.datasetsConfigPath)) {
            logger::log_error(DBERR_MISSING_FILE, "Dataset configuration file 'dataset.ini' missing from Database directory.");
            return DBERR_MISSING_FILE;
        }

        // if not specified
        if (queryStmt->datasetNicknameR == "" || queryStmt->datasetNicknameS == "") {
            // run default scenario (dev only)
            queryStmt->datasetPathR = dataset_config_pt.get<std::string>("T1NA.path");
            queryStmt->datasetPathS = dataset_config_pt.get<std::string>("T2NA.path");
            queryStmt->datasetNicknameR = "T1NA";
            queryStmt->datasetNicknameS = "T2NA";
        } else {
            queryStmt->datasetPathR = dataset_config_pt.get<std::string>(queryStmt->datasetNicknameR+".path");
            queryStmt->datasetPathS = dataset_config_pt.get<std::string>(queryStmt->datasetNicknameS+".path");
        }

        // dataset count (todo, make more flexible)
        queryStmt->datasetCount = 2;
        
        // dataset data types
        spatial_lib::DataTypeE datatypeR = spatial_lib::dataTypeTextToInt(dataset_config_pt.get<std::string>(queryStmt->datasetNicknameR+".datatype"));
        if (datatypeR == spatial_lib::DT_INVALID) {
            logger::log_error(DBERR_UNKNOWN_DATATYPE, "Unknown data type for dataset R.");
            return DBERR_UNKNOWN_DATATYPE;
        }
        spatial_lib::DataTypeE datatypeS = spatial_lib::dataTypeTextToInt(dataset_config_pt.get<std::string>(queryStmt->datasetNicknameS+".datatype"));
        if (datatypeS == spatial_lib::DT_INVALID) {
            logger::log_error(DBERR_UNKNOWN_DATATYPE, "Unknown data type for dataset S.");
            return DBERR_UNKNOWN_DATATYPE;
        }
        queryStmt->datatypeR = datatypeR;
        queryStmt->datatypeS = datatypeS;
        // datatype combination
        if(datatypeR == spatial_lib::DT_POLYGON && datatypeS == spatial_lib::DT_POLYGON) {
            queryStmt->datasetTypeCombination = spatial_lib::POLYGON_POLYGON;
        } else {
            logger::log_error(DBERR_UNSUPPORTED_DATATYPE_COMBINATION, "Dataset data type combination not yet supported.");
            return DBERR_UNSUPPORTED_DATATYPE_COMBINATION;
        }
        
        // offset maps
        queryStmt->offsetMapPathR = dataset_config_pt.get<std::string>(queryStmt->datasetNicknameR+".offsetMapPath");
        if (queryStmt->offsetMapPathR == "") {
            logger::log_error(DBERR_MISSING_FILE, "Missing offset map path for dataset R.");
            return DBERR_MISSING_FILE;
        }
        queryStmt->offsetMapPathS = dataset_config_pt.get<std::string>(queryStmt->datasetNicknameS+".offsetMapPath");
        if (queryStmt->offsetMapPathS == "") {
            logger::log_error(DBERR_MISSING_FILE, "Missing offset map path for dataset S.");
            return DBERR_MISSING_FILE;
        }

        // hardcoded bounds
        double xMinR = std::numeric_limits<int>::max();
        double yMinR = std::numeric_limits<int>::max();
        double xMaxR = -std::numeric_limits<int>::min();
        double yMaxR = -std::numeric_limits<int>::max();
        if(dataset_config_pt.get<int>(queryStmt->datasetNicknameR+".bounds")) {
            xMinR = dataset_config_pt.get<double>(queryStmt->datasetNicknameR+".xMin");
            yMinR = dataset_config_pt.get<double>(queryStmt->datasetNicknameR+".yMin");
            xMaxR = dataset_config_pt.get<double>(queryStmt->datasetNicknameR+".xMax");
            yMaxR = dataset_config_pt.get<double>(queryStmt->datasetNicknameR+".yMax");
            queryStmt->boundsSet = true;
        }
        double xMinS = std::numeric_limits<int>::max();
        double yMinS = std::numeric_limits<int>::max();
        double xMaxS = -std::numeric_limits<int>::min();
        double yMaxS = -std::numeric_limits<int>::max();
        if(dataset_config_pt.get<int>(queryStmt->datasetNicknameS+".bounds")) {
            xMinS = dataset_config_pt.get<double>(queryStmt->datasetNicknameS+".xMin");
            yMinS = dataset_config_pt.get<double>(queryStmt->datasetNicknameS+".yMin");
            xMaxS = dataset_config_pt.get<double>(queryStmt->datasetNicknameS+".xMax");
            yMaxS = dataset_config_pt.get<double>(queryStmt->datasetNicknameS+".yMax");
            queryStmt->boundsSet = true;
        }
        // if they have different hardcoded bounds, assign as global the outermost ones (min of min, max of max)
        queryStmt->xMinGlobal = std::min(xMinR, xMinS);
        queryStmt->yMinGlobal = std::min(yMinR, yMinS);
        queryStmt->xMaxGlobal = std::max(xMaxR, xMaxS);
        queryStmt->yMaxGlobal = std::max(yMaxR, yMaxS);

        return DBERR_OK;
    }

    /**
     * @brief passes the configuration on to the sysOps return object
    */
    static DB_STATUS parseConfigurationOptions(SystemOptionsStatementT &sysOpsStmt, SystemOptionsT &sysOps) {
        sysOps.nodeCount = sysOpsStmt.nodeCount;
        sysOps.nodefilePath = sysOpsStmt.nodefilePath;
        sysOps.setupType = sysOpsStmt.setupType;


        return DBERR_OK;
    }

    /**
     * @brief parse system related options from config file
    */
    static DB_STATUS loadSetupOptions(SystemOptionsStatementT &sysOpsStmt) {
        sysOpsStmt.setupType = (SystemSetupTypeE) system_config_pt.get<int>("Environment.type");
        sysOpsStmt.nodefilePath = system_config_pt.get<std::string>("Environment.nodefilePath");
        sysOpsStmt.nodeCount = system_config_pt.get<int>("Environment.nodeCount");

        return DBERR_OK;
    }

    DB_STATUS parse(int argc, char *argv[], SystemOptionsT &sysOps) {
        char c;
        SystemOptionsStatementT sysOpsStmt;
        QueryStatementT queryStmt;
        ActionsStatementT actionsStmt;

        // check If config files exist
        if (!verifyFileExists(g_config.dirPaths.configFilePath)) {
            logger::log_error(DBERR_MISSING_FILE, "Configuration file 'config.ini' missing from Database directory.");
            return DBERR_MISSING_FILE;
        }
        if (!verifyFileExists(g_config.dirPaths.datasetsConfigPath)) {
            logger::log_error(DBERR_MISSING_FILE, "Configuration file 'datasets.ini' missing from Database directory.");
            return DBERR_MISSING_FILE;
        }
        // parse configuration files
        boost::property_tree::ini_parser::read_ini(g_config.dirPaths.configFilePath, system_config_pt);
        boost::property_tree::ini_parser::read_ini(g_config.dirPaths.datasetsConfigPath, dataset_config_pt);

        // setup options
        DB_STATUS ret = loadSetupOptions(sysOpsStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // after config file has been loaded, parse cmd arguments and overwrite any selected options
        while ((c = getopt(argc, argv, "t:s:m:pcf:q:R:S:ev:z?")) != -1)
        {
            switch (c)
            {
                // case 'q':
                //     // Query Type
                //     queryStmt.queryType = std::string(optarg);
                //     break;
                case 'R':
                    // Dataset R path
                    queryStmt.datasetNicknameR = std::string(optarg);
                    queryStmt.datasetCount++;
                    break;
                case 'S':
                    // Dataset S path
                    queryStmt.datasetNicknameS = std::string(optarg);
                    queryStmt.datasetCount++;
                    break;
                case 't':
                    sysOpsStmt.setupType = (SystemSetupTypeE) atoi(optarg);
                    break;
                case 'p':
                    actionsStmt.performPartitioning = true;
                    break;
                default:
                    logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unkown cmd argument.");
                    exit(-1);
                    break;
            }
        }

        // verify setup (TODO)

        // set configuration options
        ret = parseConfigurationOptions(sysOpsStmt, sysOps);
        if (ret != DBERR_OK) {
            return ret;
        }

        // parse partitioning options
        ret = parsePartitioningOptions();
        if (ret != DBERR_OK) {
            return ret;
        }

        // parse dataset options
        ret = parseDatasetOptions(&queryStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // parse actions
        ret = parseActions(&actionsStmt);
        if (ret != DBERR_OK) {
            return ret;
        }


        return DBERR_OK;
    }
}
