#include "config/parse.h"

namespace parser
{

    // property tree var
    static boost::property_tree::ptree system_config_pt;
    static boost::property_tree::ptree dataset_config_pt;

    static DB_STATUS verifySetup() {

    }

    /**
     * @brief passes the configuration on to the sysOps return object
    */
    static DB_STATUS passConfigurationOptions(SystemOptionsStatementT &sysOpsStmt, SystemOptionsT &sysOps) {
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

        // check If config files exist
        if (!verifyFileExists(g_config.dirPaths.configFilePath)) {
            log::log_error(DBERR_MISSING_FILE, "Configuration file 'config.ini' missing from Database directory. Please refer to the README file.");
            return DBERR_MISSING_FILE;
        }
        if (!verifyFileExists(g_config.dirPaths.datasetsConfigPath)) {
            log::log_error(DBERR_MISSING_FILE, "Configuration file 'datasets.ini' missing from Database directory. Please refer to the README file.");
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
        while ((c = getopt(argc, argv, "t:s:m:p:cf:q:R:S:ev:z?")) != -1)
        {
            switch (c)
            {
                // case 'q':
                //     // Query Type
                //     queryStmt.queryType = std::string(optarg);
                //     break;
                // case 'R':
                //     // Dataset R path
                //     queryStmt.datasetNicknameR = std::string(optarg);
                //     queryStmt.datasetCount++;
                //     break;
                // case 'S':
                //     // Dataset S path
                //     queryStmt.datasetNicknameS = std::string(optarg);
                //     queryStmt.datasetCount++;
                //     break;
                case 't':
                    sysOpsStmt.setupType = (SystemSetupTypeE) atoi(optarg);
                    break;
                default:
                    log::log_error(DBERR_UNKNOWN_ARGUMENT, "Unkown cmd argument.");
                    exit(-1);
                    break;
            }
        }

        // verify setup (TODO)

        // set final configuration options
        ret = passConfigurationOptions(sysOpsStmt, sysOps);
        if (ret != DBERR_OK) {
            return ret;
        }

        return DBERR_OK;
    }
}
