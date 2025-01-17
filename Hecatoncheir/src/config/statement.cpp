#include "config/statement.h"

namespace statement
{
    DB_STATUS getPartitioningType(std::string &typeStr, PartitioningType &type) {
        if (typeStr == "RR"){
            type = PARTITIONING_ROUND_ROBIN;
        } else if (typeStr == "TWOGRID") {
            type = PARTITIONING_TWO_GRID;
        } else {
            // unknown type
            logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unknown partitioning type request:", typeStr);
        }
        return DBERR_OK;
    }

    DB_STATUS getFiletype(std::string &filetypeStr, FileType &filetype) {
        if (filetypeStr == "CSV") {
            filetype = FT_CSV;
        } else if (filetypeStr == "WKT") {
            filetype = FT_WKT;
        } else {
            // unknown type
            logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unknown file type request:", filetypeStr);
        }
        return DBERR_OK;
    }

    DB_STATUS getCreateApproximationAction(std::string &approximationStr, Action &action) {
        if (approximationStr == "APRIL") {
            action.type = ACTION_CREATE_APRIL;
        } else {
            // unknown type
            logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unknown type of create approximation request:", approximationStr);
        }
        return DBERR_OK;
    }
}