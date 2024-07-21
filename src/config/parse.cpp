#include "config/parse.h"

namespace parser
{
    // property tree var
    static boost::property_tree::ptree system_config_pt;
    static boost::property_tree::ptree dataset_config_pt;

    // query support map
    QuerySupportMap g_querySupportMap = {
        {Q_INTERSECT, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_INSIDE, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_COVERED_BY, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_COVERS, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_CONTAINS, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_DISJOINT, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_EQUAL, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_MEET, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
        {Q_FIND_RELATION, {
            {{DT_POLYGON, DT_LINESTRING}, true},
            {{DT_POLYGON, DT_POLYGON}, true},
            {{DT_LINESTRING, DT_POLYGON}, true}
        }},
    };


    static int systemSetupTypeStrToInt(std::string &str) {
        if (str == "LOCAL") {
            return SYS_LOCAL_MACHINE;
        } else if (str == "CLUSTER") {
            return SYS_CLUSTER;
        }
        return -1;
    }

    static DB_STATUS verifyDatatypeCombinationForQueryType(QueryTypeE queryType) {
        // get number of datasets
        int numberOfDatasets = g_config.datasetInfo.getNumberOfDatasets();
        // find if query type is supported
        auto queryIT = g_querySupportMap.find(queryType);
        if (queryIT != g_querySupportMap.end()) {
            DataTypeE dataTypeR = g_config.datasetInfo.getDatasetR()->dataType;
            // todo: for queries with one dataset input, handle this accordingly
            DataTypeE dataTypeS = g_config.datasetInfo.getDatasetS()->dataType;
            const auto& allowedCombinations = queryIT->second;
            auto dataTypesPair = std::make_pair(dataTypeR, dataTypeS);
            auto datatypesIT = allowedCombinations.find(dataTypesPair);
            if (datatypesIT != allowedCombinations.end()) {
                // query data types combination supported
                return DBERR_OK;
            } else {
                logger::log_error(DBERR_QUERY_INVALID_TYPE, "Data type combination unsupported for query", mapping::queryTypeIntToStr(g_config.queryInfo.type), "combination:", mapping::dataTypeIntToStr(dataTypeR), "and", mapping::dataTypeIntToStr(dataTypeS));
                return DBERR_QUERY_INVALID_TYPE;
            }
        }
        // error for query type
        logger::log_error(DBERR_QUERY_INVALID_TYPE, "Query type unsupported. Query code:", queryType);
        return DBERR_QUERY_INVALID_TYPE;
    }

    static DB_STATUS loadAPRILconfig() {
        ApproximationInfoT approxInfo(AT_APRIL);

        int N = system_config_pt.get<int>("APRIL.N");
        if (N < 10 || N > 16) {
            logger::log_error(DBERR_INVALID_PARAMETER, "APRIL granularity must be in range [10,16]. N:", N);
            return DBERR_INVALID_PARAMETER;
        }
        approxInfo.aprilConfig.setN(N);
        
        int compression = system_config_pt.get<int>("APRIL.compression");
        if (compression != 0 && compression != 1) {
            logger::log_error(DBERR_INVALID_PARAMETER, "APRIL compression must be set to either 0 (disabled) or 1 (enabled). compression:", compression);
            return DBERR_INVALID_PARAMETER;
        }
        approxInfo.aprilConfig.compression = compression;

        int partitions = system_config_pt.get<int>("APRIL.partitions");
        if (partitions < 1 || partitions > 32) {
            logger::log_error(DBERR_INVALID_PARAMETER, "APRIL partitions must be in range [1,32]. partitions:", partitions);
            return DBERR_INVALID_PARAMETER;
        }
        approxInfo.aprilConfig.partitions = partitions;

        // set to global config
        g_config.approximationInfo = approxInfo;

        return DBERR_OK;
    }

    /**
     * @brief parses the selected actions and puts them in proper order inside the configuration
     * 
     * @param actionsStmt 
     * @return DB_STATUS 
     */
    static DB_STATUS parseActions(ActionsStatementT *actionsStmt) {
        DB_STATUS ret = DBERR_OK;
        // partitioning always before issuing the load datasets action
        if (actionsStmt->performPartitioning) {
            ActionT action(ACTION_PERFORM_PARTITIONING);
            g_config.actions.emplace_back(action);
        }
        // load datasets action
        if (actionsStmt->loadDatasets) {
            ActionT action(ACTION_LOAD_DATASETS);
            g_config.actions.emplace_back(action);
        }
        // create APRIL
        for (int i=0; i<actionsStmt->createApproximations.size(); i++) {
            ActionT action;
            ret = statement::getCreateApproximationAction(actionsStmt->createApproximations.at(i), action);
            if (ret != DBERR_OK) {
                return ret;
            }
            g_config.actions.emplace_back(action);
            // load APRIL configuration from configuration file
            ret = loadAPRILconfig();
            if (ret != DBERR_OK) {
                logger::log_error(DBERR_CONFIG_FILE, "Failed to load APRIL config from system configuration file");
                return DBERR_CONFIG_FILE;
            }
        }
        // load approximations (after any creation and after the datasets have been loaded)
        if (actionsStmt->loadDatasets) {
            ActionT action(ACTION_LOAD_APRIL);
            g_config.actions.emplace_back(action);
        }
        // queries
        if (g_config.queryInfo.type != Q_NONE) {
            ActionT action(ACTION_QUERY);
            g_config.actions.emplace_back(action);
        }
        // verification always last (todo)
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
        
        if (modCells != 1) {
            newPartitionsPerDimension += modCells + 1;
        }
        return newPartitionsPerDimension;
    }

    static DB_STATUS parsePartitioningOptions() {
        std::string partitioningTypeStr = system_config_pt.get<std::string>("Partitioning.type");
        int partitionsPerDimension = system_config_pt.get<int>("Partitioning.partitionsPerDimension");
        std::string assignmentFuncStr = system_config_pt.get<std::string>("Partitioning.assignmentFunc");
        int batchSize = system_config_pt.get<int>("Partitioning.batchSize");
        std::string dataDirPath = system_config_pt.get<std::string>("Partitioning.path");

        // set node directory paths
        g_config.dirPaths.setNodeDataDirectories(dataDirPath);

        // partitioning type
        DB_STATUS ret = statement::getPartitioningType(partitioningTypeStr, g_config.partitioningInfo.type);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Unkown partitioning type in config file");
            return ret;
        }

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
            // logger::log_task("Adjusted partitions per dimension to", g_config.partitioningInfo.partitionsPerDimension);
        } else if(assignmentFuncStr == "ST") {
            // standard, do nothing extra on the grid
        } else {
            logger::log_error(DBERR_INVALID_PARAMETER, "Unknown enumeration function string:", assignmentFuncStr);
            return DBERR_INVALID_PARAMETER;
        }
        // partitioning::printPartitionAssignment();

        // batch size
        if (batchSize <= 0) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Batch size needs to be a positive number. Batch Size: ", batchSize);
            return DBERR_INVALID_PARAMETER;
        }
        g_config.partitioningInfo.batchSize = batchSize;

        return DBERR_OK;
    }

    static DB_STATUS verifyDatasetOptions(DatasetStatementT *datasetStmt) {
        if (datasetStmt->datasetCount == 1 && datasetStmt->datasetNicknameR == "") {
            logger::log_error(DBERR_INVALID_PARAMETER, "If only one dataset is specified, the '-R' argument must be used instead of '-S'");
            return DBERR_INVALID_PARAMETER;
        }

        if (datasetStmt->datasetCount > 2) {
            logger::log_error(DBERR_FEATURE_UNSUPPORTED, "System doesn't support more than 2 datasets at a time.");
            return DBERR_FEATURE_UNSUPPORTED;
        }

        if (datasetStmt->datasetCount > 0) {
            // R dataset
            // file type
            FileTypeE fileTypeR = mapping::fileTypeTextToInt(datasetStmt->filetypeR);
            if (fileTypeR == FT_INVALID) {
                logger::log_error(DBERR_INVALID_FILETYPE, "Unkown file type of dataset R:", datasetStmt->filetypeR);
                return DBERR_INVALID_FILETYPE;
            }

            // verify dataset R path
            if (!verifyFilepath(datasetStmt->datasetPathR)) {
                logger::log_error(DBERR_MISSING_FILE, "Dataset R invalid path:", datasetStmt->datasetPathR);
                return DBERR_MISSING_FILE;
            }

            if (datasetStmt->datasetCount > 1) {
                // S dataset
                // file type
                FileTypeE fileTypeS = mapping::fileTypeTextToInt(datasetStmt->filetypeS);
                if (fileTypeS == FT_INVALID) {
                    logger::log_error(DBERR_INVALID_FILETYPE, "Unkown file type of dataset S:", datasetStmt->filetypeS);
                    return DBERR_INVALID_FILETYPE;
                }
                // verify dataset S path
                if (!verifyFilepath(datasetStmt->datasetPathS)) {
                    logger::log_error(DBERR_MISSING_FILE, "Dataset S invalid path:", datasetStmt->datasetPathS);
                    return DBERR_MISSING_FILE;
                }
            }
        }


        return DBERR_OK;
    }

    static DB_STATUS parseDatasetOptions(DatasetStatementT *datasetStmt) {
        // check if datasets.ini file exists
        if (!verifyFilepath(g_config.dirPaths.datasetsConfigPath)) {
            logger::log_error(DBERR_MISSING_FILE, "Dataset configuration file 'dataset.ini' missing from Database directory.");
            return DBERR_MISSING_FILE;
        }

        // if no argument is passed, read config
        if (datasetStmt->datasetNicknameR != "") {
            datasetStmt->datasetPathR = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".path");
            // dataset data type
            DataTypeE datatypeR = mapping::dataTypeTextToInt(dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".datatype"));
            if (datatypeR == DT_INVALID) {
                logger::log_error(DBERR_INVALID_DATATYPE, "Unknown data type for dataset R.");
                return DBERR_INVALID_DATATYPE;
            }
            datasetStmt->datatypeR = datatypeR;
            // file type
            datasetStmt->filetypeR = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".filetype");
        }
        if (datasetStmt->datasetNicknameS != "") {
            datasetStmt->datasetPathS = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".path");
            // dataset data type
            DataTypeE datatypeS = mapping::dataTypeTextToInt(dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".datatype"));
            if (datatypeS == DT_INVALID) {
                logger::log_error(DBERR_INVALID_DATATYPE, "Unknown data type for dataset S.");
                return DBERR_INVALID_DATATYPE;
            }
            datasetStmt->datatypeS = datatypeS;

            // file type
            datasetStmt->filetypeS = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".filetype");
        }
        if (datasetStmt->datasetCount > 0) {
            // hardcoded bounds
            double xMinR = std::numeric_limits<int>::max();
            double yMinR = std::numeric_limits<int>::max();
            double xMaxR = -std::numeric_limits<int>::max();
            double yMaxR = -std::numeric_limits<int>::max();
            if(dataset_config_pt.get<int>(datasetStmt->datasetNicknameR+".bounds")) {
                // hardcoded bounds
                xMinR = dataset_config_pt.get<double>(datasetStmt->datasetNicknameR+".xMin");
                yMinR = dataset_config_pt.get<double>(datasetStmt->datasetNicknameR+".yMin");
                xMaxR = dataset_config_pt.get<double>(datasetStmt->datasetNicknameR+".xMax");
                yMaxR = dataset_config_pt.get<double>(datasetStmt->datasetNicknameR+".yMax");
                datasetStmt->boundsSet = true;
            }
            if (datasetStmt->datasetCount == 2) {
                double xMinS = std::numeric_limits<int>::max();
                double yMinS = std::numeric_limits<int>::max();
                double xMaxS = -std::numeric_limits<int>::max();
                double yMaxS = -std::numeric_limits<int>::max();
                if(dataset_config_pt.get<int>(datasetStmt->datasetNicknameS+".bounds")) {
                    xMinS = dataset_config_pt.get<double>(datasetStmt->datasetNicknameS+".xMin");
                    yMinS = dataset_config_pt.get<double>(datasetStmt->datasetNicknameS+".yMin");
                    xMaxS = dataset_config_pt.get<double>(datasetStmt->datasetNicknameS+".xMax");
                    yMaxS = dataset_config_pt.get<double>(datasetStmt->datasetNicknameS+".yMax");
                    datasetStmt->boundsSet = true;
                }
                // if they have different hardcoded bounds, assign as global the outermost ones (min of min, max of max)
                datasetStmt->xMinGlobal = std::min(xMinR, xMinS);
                datasetStmt->yMinGlobal = std::min(yMinR, yMinS);
                datasetStmt->xMaxGlobal = std::max(xMaxR, xMaxS);
                datasetStmt->yMaxGlobal = std::max(yMaxR, yMaxS);
            } else {
                // only one dataset (R)
                datasetStmt->xMinGlobal = xMinR;
                datasetStmt->yMinGlobal = yMinR;
                datasetStmt->xMaxGlobal = xMaxR;
                datasetStmt->yMaxGlobal = yMaxR;
            }
        }

        // verify 
        DB_STATUS ret = verifyDatasetOptions(datasetStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // set to config
        ret = configure::setDatasetInfo(datasetStmt);
        if (ret != DBERR_OK) {
            return ret;
        }
        
        return DBERR_OK;
    }

    static DB_STATUS parseQueryOptions(QueryStatementT *queryStmt) {
        if (queryStmt->queryType == "") {
            // no query was selected
            return DBERR_OK;
        }
        // get query type int
        QueryTypeE queryType = mapping::queryTypeStrToInt(queryStmt->queryType);
        if (queryType == -1) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Invalid query type:", queryStmt->queryType);
            return DBERR_INVALID_PARAMETER;
        }
        // verify query validity with input datasets
        DB_STATUS ret = verifyDatatypeCombinationForQueryType(queryType);
        if (ret != DBERR_OK) {
            return ret;
        }

        // set to configuration
        g_config.queryInfo.type = (QueryTypeE) queryType;
        
        return DBERR_OK;    
    }

    /**
     * @brief passes the configuration on to the sysOps return object
    */
    static DB_STATUS parseConfigurationOptions(SystemOptionsStatementT &sysOpsStmt) {
        // if not set by user, parse from config file
        if (sysOpsStmt.setupType == "") {
            sysOpsStmt.setupType = system_config_pt.get<std::string>("Environment.type");
        }
        if (sysOpsStmt.nodefilePath == "") {
            sysOpsStmt.nodefilePath = system_config_pt.get<std::string>("Environment.nodefilePath");
        }
        if (sysOpsStmt.nodeCount == -1) {
            sysOpsStmt.nodeCount = system_config_pt.get<int>("Environment.nodeCount");
        }
        // load pipeline configuration
        g_config.queryInfo.MBRFilter = system_config_pt.get<int>("Pipeline.MBRFilter");
        if (g_config.queryInfo.MBRFilter != 0 && g_config.queryInfo.MBRFilter != 1) {
            logger::log_error(DBERR_CONFIG_FILE, "MBRFilter setting in configuration file must be 0 or 1");
            return DBERR_CONFIG_FILE;
        }
        g_config.queryInfo.IntermediateFilter = system_config_pt.get<int>("Pipeline.IFilter");
        if (g_config.queryInfo.MBRFilter != 0 && g_config.queryInfo.MBRFilter != 1) {
            logger::log_error(DBERR_CONFIG_FILE, "IFilter setting in configuration file must be 0 or 1");
            return DBERR_CONFIG_FILE;
        }
        g_config.queryInfo.Refinement = system_config_pt.get<int>("Pipeline.Refinement");
        if (g_config.queryInfo.MBRFilter != 0 && g_config.queryInfo.MBRFilter != 1) {
            logger::log_error(DBERR_CONFIG_FILE, "Refinement setting in configuration file must be 0 or 1");
            return DBERR_CONFIG_FILE;
        }
        // set to configuration
        g_config.options.nodeCount = sysOpsStmt.nodeCount;
        g_config.options.nodefilePath = sysOpsStmt.nodefilePath;
        if (sysOpsStmt.setupType != "") {
            // user specified system type, overwrite the loaded type from config.ini
            int setupType = systemSetupTypeStrToInt(sysOpsStmt.setupType);
            if (setupType == -1) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Unknown system setup type:", sysOpsStmt.setupType);
                return DBERR_INVALID_PARAMETER;
            }
            g_config.options.setupType = (SystemSetupTypeE) setupType;
        }
        return DBERR_OK;
    }

    DB_STATUS parse(int argc, char *argv[]) {
        char c;
        SettingsStatementT settingsStmt;
        DB_STATUS ret = DBERR_OK;

        // after config file has been loaded, parse cmd arguments and overwrite any selected options
        while ((c = getopt(argc, argv, "a:t:sm:pc:f:q:R:S:ev:z?")) != -1)
        {
            switch (c)
            {
                case 'q':
                    // Query Type
                    settingsStmt.queryStmt.queryType = std::string(optarg);
                    break;
                case 'c':
                    // user configuration file
                    settingsStmt.configFilePath = std::string(optarg);
                    break;
                case 'R':
                    // Dataset R path
                    settingsStmt.datasetStmt.datasetNicknameR = std::string(optarg);
                    settingsStmt.datasetStmt.datasetCount++;
                    settingsStmt.actionsStmt.loadDatasets = true;
                    break;
                case 'S':
                    // Dataset S path
                    settingsStmt.datasetStmt.datasetNicknameS = std::string(optarg);
                    settingsStmt.datasetStmt.datasetCount++;
                    settingsStmt.actionsStmt.loadDatasets = true;
                    break;
                case 't':
                    // system type: LOCAL (1 machine) or CLUSTER (many machines)
                    settingsStmt.sysOpsStmt.setupType = std::string(optarg);
                    break;
                case 'p':
                    // perform the partitioning of the input datasets
                    settingsStmt.actionsStmt.performPartitioning = true;
                    break;
                case 'a':
                    // order the nodes to create the selected approximation for the input datasets
                    settingsStmt.actionsStmt.createApproximations.emplace_back(std::string(optarg));
                    break;
                default:
                    logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unkown cmd argument.");
                    return DBERR_UNKNOWN_ARGUMENT;
            }
        }

        // check If config files exist
        if (!verifyFilepath(settingsStmt.configFilePath)) {
            logger::log_error(DBERR_MISSING_FILE, "Configuration file missing from Database directory. Path:", settingsStmt.configFilePath);
            return DBERR_MISSING_FILE;
        } else {
            // set config file
            g_config.dirPaths.configFilePath = settingsStmt.configFilePath;
        }
        if (!verifyFilepath(g_config.dirPaths.datasetsConfigPath)) {
            logger::log_error(DBERR_MISSING_FILE, "Configuration file 'datasets.ini' missing from Database directory.");
            return DBERR_MISSING_FILE;
        }

        // parse configuration files
        boost::property_tree::ini_parser::read_ini(g_config.dirPaths.configFilePath, system_config_pt);
        boost::property_tree::ini_parser::read_ini(g_config.dirPaths.datasetsConfigPath, dataset_config_pt);

        // configuration options
        ret = parseConfigurationOptions(settingsStmt.sysOpsStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // partitioning options
        ret = parsePartitioningOptions();
        if (ret != DBERR_OK) {
            return ret;
        }

        // dataset options
        ret = parseDatasetOptions(&settingsStmt.datasetStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // query options
        ret = parseQueryOptions(&settingsStmt.queryStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // actions
        ret = parseActions(&settingsStmt.actionsStmt);
        if (ret != DBERR_OK) {
            return ret;
        }


        return DBERR_OK;
    }
}
