#ifndef D_CONFIG_STATEMENT
#define D_CONFIG_STATEMENT

#include "containers.h"

typedef struct ActionsStatement {
    bool performPartitioning = false;
    std::vector<std::string> createApproximations;
    bool performVerification = false;
    bool loadDatasets = false;
}ActionsStatementT;

typedef struct DatasetStatement {
    std::string queryType = "";
    std::string datasetPathR = "", datasetPathS = "";
    std::string datasetNicknameR = "", datasetNicknameS = "";
    std::string filetypeR,filetypeS;
    int datasetCount = 0;
    DataTypeE datatypeR, datatypeS;
    bool boundsSet = false;
    double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;
} DatasetStatementT;

typedef struct QueryStatement {
    std::string queryType = "";
}QueryStatementT;

typedef struct SystemOptionsStatement {
    std::string setupType = "";
    std::string nodefilePath = "";
    int nodeCount = -1;
} SystemOptionsStatementT;

typedef struct SettingsStatement {
    std::string configFilePath;
    ActionsStatementT actionsStmt;
    QueryStatementT queryStmt;
    SystemOptionsStatementT sysOpsStmt;
    DatasetStatementT datasetStmt;
} SettingsStatementT;

namespace statement
{
    DB_STATUS getPartitioningType(std::string &typeStr, PartitioningTypeE &type);

    DB_STATUS getFiletype(std::string &filetypeStr, FileTypeE &filetype);

    DB_STATUS getCreateApproximationAction(std::string &approximationStr, Action &actionType);
}

#endif