#include "APRIL/storage.h"

namespace APRIL
{
    namespace writer
    {
        DB_STATUS saveAPRIL(FILE* pFile, size_t recID, uint sectionID, AprilData* aprilData) {
            // ALL intervals
            size_t elementsWritten = fwrite(&recID, sizeof(size_t), 1, pFile);
            if (elementsWritten != 1) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID failed for object with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            // buffered write for section id, numIntervalsALL and numIntervalsFull
            int buf[3];
            buf[0] = sectionID;
            buf[1] = aprilData->numIntervalsALL;
            buf[2] = aprilData->numIntervalsFULL;
            elementsWritten = fwrite(buf, sizeof(int), 3, pFile);
            if (elementsWritten != 3) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing sectionID and numIntervalsALL failed for object with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            // ALL intervals
            elementsWritten = fwrite(&aprilData->intervalsALL.data()[0], sizeof(uint32_t), aprilData->numIntervalsALL * 2, pFile);
            if (elementsWritten != aprilData->numIntervalsALL * 2) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing ALL intervals failed for object with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            // FULL intervals (if any)
            if(aprilData->numIntervalsFULL > 0){
                elementsWritten = fwrite(&aprilData->intervalsFULL.data()[0], sizeof(uint32_t), aprilData->numIntervalsFULL * 2, pFile);
                if (elementsWritten != aprilData->numIntervalsFULL * 2) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing FULL intervals failed for object with ID", recID);
                    return DBERR_DISK_WRITE_FAILED;
                }
            }
            return DBERR_OK;
        }

        DB_STATUS saveAPRIL(FILE* pFile, Dataset &dataset) {
            DB_STATUS ret = DBERR_OK;
            for (auto &secIT : dataset.sectionMap) {
                for (auto &objIT : secIT.second.aprilData) {
                    ret = saveAPRIL(pFile, objIT.first, secIT.first, &objIT.second);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Saving APRIL for object", objIT.first, "failed.");
                        return ret;
                    }
                }
            }
            return ret;
        }
    }

    namespace reader
    {
        static DB_STATUS loadIntervals(std::string &intervalFilePath, Dataset &dataset, bool ALL) {
            DB_STATUS ret = DBERR_OK;
            int buf[3];
            size_t objectCount;
            size_t recID;
            int sectionID, numIntervals, totalValues;
            std::vector<uint32_t> intervals;
            // open interval file
            FILE* pFile = fopen(intervalFilePath.c_str(), "rb");
            if (pFile == NULL) {
                logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", intervalFilePath);
                return DBERR_MISSING_FILE;
            }
            // read object count
            size_t elementsRead = fread(&objectCount, sizeof(size_t), 1, pFile);
            if (elementsRead != 1) {
                logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read object count");
                ret = DBERR_DISK_READ_FAILED;
                goto CLOSE_AND_EXIT;
            }
            // read APRIL for each object
            for (size_t i=0; i<objectCount; i++) {
                elementsRead = fread(&recID, sizeof(size_t), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the recID","for object number",i,"of",objectCount);
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                elementsRead = fread(&buf, sizeof(int), 2, pFile);
                if (elementsRead != 2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Read", elementsRead,"instead of",2,"for object number",i,"of",objectCount);
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                sectionID = buf[0];
                numIntervals = buf[1];
                // X intervals are comprised of X*2 numbers [start,end)
                totalValues = numIntervals*2;
                intervals.resize(totalValues);
                elementsRead = fread(&intervals.data()[0], sizeof(uint32_t), totalValues, pFile);
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
                ret = dataset.addIntervalsToAprilData(sectionID, recID, numIntervals, intervals, ALL);
                if(ret != DBERR_OK){
                    goto CLOSE_AND_EXIT;
                }

            }

CLOSE_AND_EXIT:
            fclose(pFile);
            return ret;
        }

        DB_STATUS loadAPRIL(Dataset* dataset) {
            DB_STATUS ret = DBERR_OK;
            int buf[3];
            size_t objectCount;
            size_t recID;
            int sectionID, totalValuesALL, totalValuesFULL;
            // open APRIL file
            FILE* pFile = fopen(dataset->aprilConfig.filepath.c_str(), "rb");
            if (pFile == NULL) {
                logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset->aprilConfig.filepath);
                return DBERR_MISSING_FILE;
            }
            // read object count
            size_t elementsRead = fread(&objectCount, sizeof(size_t), 1, pFile);
            if (elementsRead != 1) {
                logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read object count");
                ret = DBERR_DISK_READ_FAILED;
                goto CLOSE_AND_EXIT;
            }
            // read APRIL for each object
            for (size_t i=0; i<objectCount; i++) {
                // empty april data object
                AprilData aprilData;
                // read rec ID
                elementsRead = fread(&recID, sizeof(size_t), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the recID","for object number",i,"of",objectCount);
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                // read secID, numIntervalsALL and numIntervalsFULL
                elementsRead = fread(&buf, sizeof(int), 3, pFile);
                if (elementsRead != 3) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Read", elementsRead,"instead of",3,"for object number",i,"of",objectCount);
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                sectionID = buf[0];
                aprilData.numIntervalsALL = buf[1];
                aprilData.numIntervalsFULL = buf[2];
                // X intervals are comprised of X*2 values [start,end)
                totalValuesALL = aprilData.numIntervalsALL*2;
                aprilData.intervalsALL.resize(totalValuesALL);
                // read intervals ALL
                elementsRead = fread(&aprilData.intervalsALL.data()[0], sizeof(uint32_t), totalValuesALL, pFile);
                if (elementsRead != totalValuesALL) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the ALL intervals");
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;

                }
                // FULL intervals
                if (aprilData.numIntervalsFULL > 0) {
                    totalValuesFULL = aprilData.numIntervalsFULL*2;
                    aprilData.intervalsFULL.resize(totalValuesFULL);
                    // read intervals FULL
                    elementsRead = fread(&aprilData.intervalsFULL.data()[0], sizeof(uint32_t), totalValuesFULL, pFile);
                    if (elementsRead != totalValuesFULL) {
                        logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the FULL intervals");
                        ret = DBERR_DISK_READ_FAILED;
                        goto CLOSE_AND_EXIT;

                    }
                }
                // add APRIL to the dataset
                dataset->addAprilData(sectionID, recID, aprilData);
            }
CLOSE_AND_EXIT:
            fclose(pFile);
            return ret;
        }
    }

}