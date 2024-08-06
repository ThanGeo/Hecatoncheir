#ifndef D_CONFIG_STATEMENT
#define D_CONFIG_STATEMENT

#include "containers.h"

/** @brief Statement containing the requested actions, based on the user and system configuration. */
struct ActionsStatement {
    bool performPartitioning = false;
    std::vector<std::string> createApproximations;
    bool performVerification = false;
    bool loadDatasets = false;
};

/** @brief Statement containing all the dataset related parameters. */
struct DatasetStatement {
    std::string queryType = "";
    std::string datasetPathR = "", datasetPathS = "";
    std::string datasetNicknameR = "", datasetNicknameS = "";
    std::string filetypeR,filetypeS;
    int datasetCount = 0;
    DataType datatypeR, datatypeS;
    bool boundsSet = false;
    double xMinGlobal, yMinGlobal, xMaxGlobal, yMaxGlobal;
};

/** @brief Statement containing the query info. */
struct QueryStatement {
    std::string queryType = "";
};

/** @brief Statement containing the distributed system options. */
struct SystemOptionsStatement {
    std::string setupType = "";
    std::string nodefilePath = "";
    int nodeCount = -1;
};

/** @brief Statement containing all system and user specified parameters for runtime. */
struct SettingsStatement {
    std::string configFilePath;
    ActionsStatement actionsStmt;
    QueryStatement queryStmt;
    SystemOptionsStatement sysOpsStmt;
    DatasetStatement datasetStmt;
};

/** @brief Statement related methods. */
namespace statement
{
    /** @brief Sets the partitioning type based on the user specification.
     * @param[in] typeStr A string describing the partitioning method.
     * @param[out] type The returned partitioning type system parameter.
    */
    DB_STATUS getPartitioningType(std::string &typeStr, PartitioningType &type);

    /** @brief Sets the input data file type.
     * @param[in] filetypeStr A string describing the file type.
     * @param[out] filetype The returned file type system parameter.
    */
    DB_STATUS getFiletype(std::string &filetypeStr, FileType &filetype);

    /** @brief Sets the create approximation action (if any).
     * @param[in] approximationStr A string describing the approximation type to create.
     * @param[out] fileactionTypetype The returned create approximation type system parameter.
    */
    DB_STATUS getCreateApproximationAction(std::string &approximationStr, Action &actionType);
}

#endif