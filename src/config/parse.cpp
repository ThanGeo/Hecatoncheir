#include "config/parse.h"

namespace parser
{
    // property tree var
    static boost::property_tree::ptree system_config_pt;
    static boost::property_tree::ptree dataset_config_pt;

    static int systemSetupTypeStrToInt(std::string &str) {
        if (str == "LOCAL") {
            return SYS_LOCAL_MACHINE;
        } else if (str == "CLUSTER") {
            return SYS_CLUSTER;
        }
        return -1;
    }

    static DB_STATUS verifyDatatypeCombinationForQueryType(int queryType) {
        // get number of datasets
        int numberOfDatasets = g_config.datasetInfo.getNumberOfDatasets();
        // get dataset data types combination
        spatial_lib::DatatypeCombinationE datatypeCombination = g_config.datasetInfo.getDatatypeCombination();
        if (datatypeCombination == spatial_lib::DC_INVALID_COMBINATION) {
            logger::log_error(DBERR_UNSUPPORTED_DATATYPE_COMBINATION, "Unsupported data type combination:", datatypeCombination);
            return DBERR_UNSUPPORTED_DATATYPE_COMBINATION;
        }

        switch (queryType) {
            // range query
            case spatial_lib::Q_RANGE:
                // requires dataset R to be queries and dataset S to be data
                if (numberOfDatasets != 2) {
                    logger::log_error(DBERR_QUERY_INVALID_INPUT, "Range query requires two datasets as input: R for the queries, S for the data to be queried.");
                    return DBERR_QUERY_INVALID_INPUT;
                }
                // queries must be polygon or rectangle
                if (g_config.datasetInfo.getDatasetR()->dataType != spatial_lib::DT_RECTANGLE && g_config.datasetInfo.getDatasetR()->dataType != spatial_lib::DT_POLYGON) {
                    logger::log_error(DBERR_QUERY_INVALID_INPUT, "Query dataset (R) must contain either RECTANGLE or POLYGON objects");
                    return DBERR_QUERY_INVALID_INPUT;
                }
                // currently supported queries
                switch (datatypeCombination) {
                    default:
                        logger::log_error(DBERR_UNSUPPORTED_DATATYPE_COMBINATION, "Datatype combination", spatial_lib::mapping::datatypeCombinationIntToStr(datatypeCombination), " for Range queries currently unsupported");
                        return DBERR_UNSUPPORTED_DATATYPE_COMBINATION;
                }
                
                break;
            // joins with topological predicate
            case spatial_lib::Q_FIND_RELATION:
            case spatial_lib::Q_INTERSECT:
            case spatial_lib::Q_INSIDE:
            case spatial_lib::Q_CONTAINS:
            case spatial_lib::Q_COVERED_BY:
            case spatial_lib::Q_COVERS:
            case spatial_lib::Q_DISJOINT:
            case spatial_lib::Q_EQUAL:
            case spatial_lib::Q_MEET:
                // require two datasets
                if (numberOfDatasets != 2) {
                    logger::log_error(DBERR_QUERY_INVALID_INPUT, "Joins require two datasets as input.");
                    return DBERR_QUERY_INVALID_INPUT;
                }
                // datatype combination support
                if (datatypeCombination != spatial_lib::DC_POLYGON_POLYGON) {
                    logger::log_error(DBERR_UNSUPPORTED_DATATYPE_COMBINATION, "Currently only polygon-polygon joins are supported. Input:", spatial_lib::mapping::datatypeCombinationIntToStr(datatypeCombination));
                    return DBERR_UNSUPPORTED_DATATYPE_COMBINATION;
                }
                break;
        }

        return DBERR_OK;
    }

    static DB_STATUS loadAPRILconfig() {
        ApproximationInfoT approxInfo(spatial_lib::AT_APRIL);

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
        // partitioning always first
        if (actionsStmt->performPartitioning) {
            ActionT action(ACTION_PERFORM_PARTITIONING);
            g_config.actions.emplace_back(action);
        }
        // approximations
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

        // queries
        if (g_config.queryInfo.type != spatial_lib::Q_NONE) {
            ActionT action(ACTION_QUERY);
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
            spatial_lib::FileTypeE fileTypeR = spatial_lib::mapping::fileTypeTextToInt(datasetStmt->filetypeR);
            if (fileTypeR == spatial_lib::FT_INVALID) {
                logger::log_error(DBERR_INVALID_FILETYPE, "Unkown file type of dataset R:", datasetStmt->filetypeR);
                return DBERR_INVALID_FILETYPE;
            }

            // verify dataset R path + offset
            if (!verifyFilepath(datasetStmt->datasetPathR)) {
                logger::log_error(DBERR_MISSING_FILE, "Dataset R invalid path:", datasetStmt->datasetPathR);
                return DBERR_MISSING_FILE;
            }

            if (datasetStmt->datasetCount > 1) {
                // S dataset
                // file type
                spatial_lib::FileTypeE fileTypeS = spatial_lib::mapping::fileTypeTextToInt(datasetStmt->filetypeS);
                if (fileTypeS == spatial_lib::FT_INVALID) {
                    logger::log_error(DBERR_INVALID_FILETYPE, "Unkown file type of dataset S:", datasetStmt->filetypeS);
                    return DBERR_INVALID_FILETYPE;
                }
                // verify dataset S path + offset
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
            spatial_lib::DataTypeE datatypeR = spatial_lib::mapping::dataTypeTextToInt(dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".datatype"));
            if (datatypeR == spatial_lib::DT_INVALID) {
                logger::log_error(DBERR_INVALID_DATATYPE, "Unknown data type for dataset R.");
                return DBERR_INVALID_DATATYPE;
            }
            datasetStmt->datatypeR = datatypeR;

            // file type
            datasetStmt->filetypeR = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".filetype");

            // offset map
            datasetStmt->offsetMapPathR = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".offsetMapPath");
            if (datasetStmt->offsetMapPathR == "") {
                logger::log_error(DBERR_MISSING_FILE, "Missing offset map path for dataset R.");
                return DBERR_MISSING_FILE;
            }
            
        }
        if (datasetStmt->datasetNicknameS != "") {
            datasetStmt->datasetPathS = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".path");
            // dataset data type
            spatial_lib::DataTypeE datatypeS = spatial_lib::mapping::dataTypeTextToInt(dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".datatype"));
            if (datatypeS == spatial_lib::DT_INVALID) {
                logger::log_error(DBERR_INVALID_DATATYPE, "Unknown data type for dataset S.");
                return DBERR_INVALID_DATATYPE;
            }
            datasetStmt->datatypeS = datatypeS;

            // file type
            datasetStmt->filetypeS = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".filetype");

            // offset map
            datasetStmt->offsetMapPathS = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".offsetMapPath");
            if (datasetStmt->offsetMapPathS == "") {
                logger::log_error(DBERR_MISSING_FILE, "Missing offset map path for dataset S.");
                return DBERR_MISSING_FILE;
            }
            
        }
        
        // datatype combination
        if (datasetStmt->datasetCount == 2) {
            if(datasetStmt->datatypeR == spatial_lib::DT_POLYGON && datasetStmt->datatypeS == spatial_lib::DT_POLYGON) {
                datasetStmt->DatatypeCombination = spatial_lib::DC_POLYGON_POLYGON;
            } else {
                logger::log_error(DBERR_UNSUPPORTED_DATATYPE_COMBINATION, "Dataset data type combination not yet supported.");
                return DBERR_UNSUPPORTED_DATATYPE_COMBINATION;
            }
        }
        if (datasetStmt->datasetCount > 0) {
            // hardcoded bounds
            double xMinR = std::numeric_limits<int>::max();
            double yMinR = std::numeric_limits<int>::max();
            double xMaxR = -std::numeric_limits<int>::max();
            double yMaxR = -std::numeric_limits<int>::max();
            if(dataset_config_pt.get<int>(datasetStmt->datasetNicknameR+".bounds")) {
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
        int queryType = spatial_lib::mapping::queryTypeStrToInt(queryStmt->queryType);
        if (queryType == -1) {
            return DBERR_INVALID_PARAMETER;
        }
        // verify query validity with input datasets
        DB_STATUS ret = verifyDatatypeCombinationForQueryType(queryType);
        if (ret != DBERR_OK) {
            return ret;
        }

        // set to configuration
        g_config.queryInfo.type = (spatial_lib::QueryTypeE) queryType;
        
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
                    break;
                case 'S':
                    // Dataset S path
                    settingsStmt.datasetStmt.datasetNicknameS = std::string(optarg);
                    settingsStmt.datasetStmt.datasetCount++;
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
