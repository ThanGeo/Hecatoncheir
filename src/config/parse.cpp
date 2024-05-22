#include "config/parse.h"

namespace parser
{
    std::unordered_map<std::string, PartitioningTypeE> partitioningTypeStrToIntMap = {{"RR",PARTITIONING_ROUND_ROBIN}};
    std::unordered_map<std::string, spatial_lib::FileTypeE> fileTypeStrToIntMap = {{"BINARY",spatial_lib::FT_BINARY},
                                                                                    {"CSV",spatial_lib::FT_CSV},
                                                                                    {"WKT",spatial_lib::FT_WKT},};

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
            if (datasetStmt->filetypeR != "BINARY") {
                logger::log_error(DBERR_INVALID_PARAMETER, "Unkown file type of dataset R:", datasetStmt->filetypeR);
                return DBERR_INVALID_PARAMETER;
            }

            if (datasetStmt->datasetCount > 1) {
                if (datasetStmt->filetypeS != "BINARY") {
                    logger::log_error(DBERR_INVALID_PARAMETER, "Unkown file type of dataset S:", datasetStmt->filetypeS);
                    return DBERR_INVALID_PARAMETER;
                }
            }
        }

        return DBERR_OK;
    }

    static DB_STATUS parseDatasetOptions(DatasetStatementT *datasetStmt) {
        // check if datasets.ini file exists
        if (!verifyFileExists(g_config.dirPaths.datasetsConfigPath)) {
            logger::log_error(DBERR_MISSING_FILE, "Dataset configuration file 'dataset.ini' missing from Database directory.");
            return DBERR_MISSING_FILE;
        }

        // if no argument is passed, read config
        if (datasetStmt->datasetNicknameR != "") {
            datasetStmt->datasetPathR = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".path");
            // dataset data type
            spatial_lib::DataTypeE datatypeR = spatial_lib::dataTypeTextToInt(dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".datatype"));
            if (datatypeR == spatial_lib::DT_INVALID) {
                logger::log_error(DBERR_UNKNOWN_DATATYPE, "Unknown data type for dataset R.");
                return DBERR_UNKNOWN_DATATYPE;
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
            spatial_lib::DataTypeE datatypeS = spatial_lib::dataTypeTextToInt(dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameS+".datatype"));
            if (datatypeS == spatial_lib::DT_INVALID) {
                logger::log_error(DBERR_UNKNOWN_DATATYPE, "Unknown data type for dataset S.");
                return DBERR_UNKNOWN_DATATYPE;
            }
            datasetStmt->datatypeS = datatypeS;

            // file type
            datasetStmt->filetypeR = dataset_config_pt.get<std::string>(datasetStmt->datasetNicknameR+".filetype");

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
                datasetStmt->datasetTypeCombination = spatial_lib::POLYGON_POLYGON;
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
        
        logger::log_success("Setup", datasetStmt->datasetCount,"datasets");

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
        SettingsStatementT settingsStmt;

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
        DB_STATUS ret = loadSetupOptions(settingsStmt.sysOpsStmt);
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
                    settingsStmt.datasetStmt.datasetNicknameR = std::string(optarg);
                    settingsStmt.datasetStmt.datasetCount++;
                    break;
                case 'S':
                    // Dataset S path
                    settingsStmt.datasetStmt.datasetNicknameS = std::string(optarg);
                    settingsStmt.datasetStmt.datasetCount++;
                    break;
                case 't':
                    settingsStmt.sysOpsStmt.setupType = (SystemSetupTypeE) atoi(optarg);
                    break;
                case 'p':
                    settingsStmt.actionsStmt.performPartitioning = true;
                    break;
                default:
                    logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unkown cmd argument.");
                    exit(-1);
                    break;
            }
        }

        // verify setup (TODO)

        // set configuration options
        ret = parseConfigurationOptions(settingsStmt.sysOpsStmt, sysOps);
        if (ret != DBERR_OK) {
            return ret;
        }

        // parse partitioning options
        ret = parsePartitioningOptions();
        if (ret != DBERR_OK) {
            return ret;
        }

        // parse dataset options
        ret = parseDatasetOptions(&settingsStmt.datasetStmt);
        if (ret != DBERR_OK) {
            return ret;
        }

        // parse query options

        // parse actions
        ret = parseActions(&settingsStmt.actionsStmt);
        if (ret != DBERR_OK) {
            return ret;
        }


        return DBERR_OK;
    }
}
