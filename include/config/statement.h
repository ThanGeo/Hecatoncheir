#ifndef D_CONFIG_STATEMENT
#define D_CONFIG_STATEMENT

#include "def.h"


typedef struct ActionsStatement {
    bool performPartitioning = false;
    bool createAPRIL = false;
    bool performVerification = false;
}ActionsStatementT;

typedef struct DatasetStatement {
    std::string queryType = "";
    std::string datasetPathR = "", datasetPathS = "";
    std::string datasetNicknameR = "", datasetNicknameS = "";
    std::string offsetMapPathR = "", offsetMapPathS = "";
    std::string filetypeR,filetypeS;
    int datasetTypeCombination = -1;
    int datasetCount = 0;
    spatial_lib::DataTypeE datatypeR, datatypeS;
    bool boundsSet = false;
    double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;
} DatasetStatementT;

typedef struct QueryStatement {
    /* TODO */
    std::string queryType = "";
    int datasetTypeCombination = -1;
    int datasetCount = 0;
    spatial_lib::DataTypeE datatypeR, datatypeS;
    bool boundsSet = false;
}QueryStatementT;

typedef struct SystemOptionsStatement {
    std::string setupType = "";
    std::string nodefilePath = "";
    uint nodeCount;
} SystemOptionsStatementT;

typedef struct SettingsStatement {
    ActionsStatementT actionsStmt;
    QueryStatementT queryStmt;
    SystemOptionsStatementT sysOpsStmt;
    DatasetStatementT datasetStmt;
} SettingsStatementT;

#endif