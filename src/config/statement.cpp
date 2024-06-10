#include "config/statement.h"

namespace statement
{
    DB_STATUS getPartitioningType(std::string &typeStr, PartitioningTypeE &type) {
        if (typeStr == "RR"){
            type = PARTITIONING_ROUND_ROBIN;
        } else {
            // unknown type
            logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unknown partitioning type request:", typeStr);
        }
        return DBERR_OK;
    }

    DB_STATUS getFiletype(std::string &filetypeStr, spatial_lib::FileTypeE &filetype) {
        if (filetypeStr == "BINARY") {
            filetype = spatial_lib::FT_BINARY;
        } else if (filetypeStr == "CSV") {
            filetype = spatial_lib::FT_CSV;
        } else if (filetypeStr == "WKT") {
            filetype = spatial_lib::FT_WKT;
        } else {
            // unknown type
            logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unknown file type request:", filetypeStr);
        }
        return DBERR_OK;
    }

    DB_STATUS getCreateApproximationAction(std::string &approximationStr, ActionTypeE &actionType) {
        if (approximationStr == "APRIL") {
            actionType = ACTION_CREATE_APRIL;
        } else {
            // unknown type
            logger::log_error(DBERR_UNKNOWN_ARGUMENT, "Unknown type of create approximation request:", approximationStr);
        }
        return DBERR_OK;
    }
}