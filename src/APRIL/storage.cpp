#include "APRIL/storage.h"

namespace APRIL
{
    namespace writer
    {
        DB_STATUS saveAPRIL(FILE* pFileALL, FILE* pFileFULL, uint recID, uint sectionID, spatial_lib::AprilDataT* aprilData) {
            // buffered write
            int buf[3];
            buf[0] = recID;
            buf[1] = sectionID;
            buf[2] = aprilData->numIntervalsALL;
            // ALL intervals
            size_t elementsWritten = fwrite(buf, sizeof(int), 3, pFileALL);
            if (elementsWritten != 3) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID, sectionID and numIntervalsALL failed for polygon with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            elementsWritten = fwrite(&aprilData->intervalsALL.data()[0], sizeof(uint), aprilData->numIntervalsALL * 2, pFileALL);
            if (elementsWritten != aprilData->numIntervalsALL * 2) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing ALL intervals failed for polygon with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            // FULL intervals (if any)
            if(aprilData->numIntervalsFULL > 0){
                buf[2] = aprilData->numIntervalsFULL;
                elementsWritten = fwrite(buf, sizeof(int), 3, pFileFULL);
                if (elementsWritten != 3) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID, sectionID and numIntervalsFULL failed for polygon with ID", recID);
                    return DBERR_DISK_WRITE_FAILED;
                }
                elementsWritten = fwrite(&aprilData->intervalsFULL.data()[0], sizeof(uint), aprilData->numIntervalsFULL * 2, pFileFULL);
                if (elementsWritten != aprilData->numIntervalsFULL * 2) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing FULL intervals failed for polygon with ID", recID);
                    return DBERR_DISK_WRITE_FAILED;
                }
            }

            return DBERR_OK;
        }
    }

}