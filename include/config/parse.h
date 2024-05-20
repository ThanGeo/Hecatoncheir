#ifndef D_PARSE_H
#define D_PARSE_H


#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>

#include "SpatialLib.h"
#include "def.h"
#include "containers.h"
#include "env/partitioning.h"

#include <unordered_map>




namespace parser
{
    extern std::unordered_map<std::string, PartitioningTypeE> partitioningTypeStrToIntMap;

    typedef struct ActionsStatement {
        bool performPartitioning = false;
        bool createAPRIL = false;
        bool performVerification = false;
    }ActionsStatementT;

    typedef struct QueryStatement {
        std::string queryType = "";
        std::string datasetPathR = "", datasetPathS = "";
        std::string datasetNicknameR = "", datasetNicknameS = "";
        std::string offsetMapPathR = "", offsetMapPathS = "";
        int datasetTypeCombination = -1;
        int datasetCount = 0;
        spatial_lib::DataTypeE datatypeR, datatypeS;
        bool boundsSet = false;
        double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;
    }QueryStatementT;

    typedef struct SystemOptionsStatement {
        SystemSetupTypeE setupType = SYS_LOCAL_MACHINE;
        std::string nodefilePath;
        uint nodeCount;
    } SystemOptionsStatementT;

    /**
     * @brief Load config options and parse any cmd arguments.
     * @return fills the sysOps object with data
    */
    DB_STATUS parse(int argc, char *argv[], SystemOptionsT &sysOps);
}

#endif