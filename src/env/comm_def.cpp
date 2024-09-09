#include "env/comm_def.h"

MsgType dataTypeToMsgType(DataType dataType) {
    switch (dataType) {
        case DT_POLYGON:
            return MSG_BATCH_POLYGON;
        case DT_POINT:
            return MSG_BATCH_POINT;
        case DT_LINESTRING:
            return MSG_BATCH_LINESTRING;
        default:
            logger::log_error(DBERR_INVALID_DATATYPE, "Unknown data type with code:", dataType);
            return MSG_INVALID;
    }
}
DataType msgTypeToDataType(MsgType msgType) {
    switch (msgType) {
        case MSG_BATCH_POLYGON:
            return DT_POLYGON;
        case MSG_BATCH_LINESTRING:
            return DT_LINESTRING;
        case MSG_BATCH_POINT:
            return DT_POINT;
        default:
            logger::log_error(DBERR_INVALID_DATATYPE, "Unknown msg type with code:", msgType);
            return DT_INVALID;
    }
}