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

    namespace reader
    {
        static DB_STATUS loadIntervals(std::string &intervalFilePath, spatial_lib::DatasetT &dataset, bool ALL) {
            DB_STATUS ret = DBERR_OK;
            int buf[3];
            int polygonCount, recID, sectionID, numIntervals, totalValues;
            std::vector<uint> intervals;
            // open interval file
            FILE* pFile = fopen(intervalFilePath.c_str(), "rb");
            if (pFile == NULL) {
                logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", intervalFilePath);
                return DBERR_MISSING_FILE;
            }
            // read polygon count
            size_t elementsRead = fread(&polygonCount, sizeof(int), 1, pFile);
            if (elementsRead != 1) {
                logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read polygon count");
                ret = DBERR_DISK_READ_FAILED;
                goto CLOSE_AND_EXIT;
            }
            // read APRIL for each polygon
            for (int i=0; i<polygonCount; i++) {
                elementsRead = fread(&buf, sizeof(int), 3, pFile);
                if (elementsRead != 3) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Read", elementsRead,"instead of",3,"for polygon number",i,"of",polygonCount);
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                recID = buf[0];
                sectionID = buf[1];
                numIntervals = buf[2];
                // X intervals are comprised of X*2 numbers [start,end)
                totalValues = numIntervals*2;
                intervals.resize(totalValues);
                elementsRead = fread(&intervals.data()[0], sizeof(uint), totalValues, pFile);
                if (elementsRead != totalValues) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the intervals");
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;

                }
                // store
                if (ALL) {
                    // add object to section map first
                    dataset.addObjectToSectionMap(sectionID, recID);
                }
                // add intervals
                dataset.addIntervalsToAprilData(sectionID, recID, numIntervals, intervals, ALL);
            }

CLOSE_AND_EXIT:
            fclose(pFile);
            return ret;
        }

        DB_STATUS loadAPRIL(spatial_lib::DatasetT &dataset) {
            // load ALL intervals
            DB_STATUS ret = loadIntervals(dataset.aprilConfig.ALL_intervals_path, dataset, true);
            if (ret != DBERR_OK) {
                return ret;
            }
            // load FULL intervals
            ret = loadIntervals(dataset.aprilConfig.FULL_intervals_path, dataset, false);
            if (ret != DBERR_OK) {
                return ret;
            }
            
            return ret;
        }
    }

}