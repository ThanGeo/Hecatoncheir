#include "APRIL/storage.h"

namespace APRIL
{
    namespace writer
    {
        DB_STATUS saveAPRILForObject(FILE* pFileALL, FILE* pFileFULL, size_t recID, uint sectionID, AprilData* aprilData) {
            // ALL intervals
            size_t elementsWritten = fwrite(&recID, sizeof(size_t), 1, pFileALL);
            if (elementsWritten != 1) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID failed for object with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            // buffered write for section id and numIntervals
            int buf[2];
            buf[0] = sectionID;
            buf[1] = aprilData->numIntervalsALL;
            elementsWritten = fwrite(buf, sizeof(int), 2, pFileALL);
            if (elementsWritten != 2) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing sectionID and numIntervalsALL failed for object with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            elementsWritten = fwrite(&aprilData->intervalsALL.data()[0], sizeof(uint32_t), aprilData->numIntervalsALL * 2, pFileALL);
            if (elementsWritten != aprilData->numIntervalsALL * 2) {
                logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing ALL intervals failed for object with ID", recID);
                return DBERR_DISK_WRITE_FAILED;
            }
            // logger::log_success("Object:", recID, "saved", aprilData->numIntervalsALL * 2, "uint32_t elements for ALL intervals");
            // FULL intervals (if any)
            if(aprilData->numIntervalsFULL > 0){
                elementsWritten = fwrite(&recID, sizeof(size_t), 1, pFileFULL);
                if (elementsWritten != 1) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID failed for object with ID", recID);
                    return DBERR_DISK_WRITE_FAILED;
                }
                buf[1] = aprilData->numIntervalsFULL;
                elementsWritten = fwrite(buf, sizeof(int), 2, pFileFULL);
                if (elementsWritten != 2) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing sectionID and numIntervalsFULL failed for object with ID", recID);
                    return DBERR_DISK_WRITE_FAILED;
                }
                elementsWritten = fwrite(&aprilData->intervalsFULL.data()[0], sizeof(uint32_t), aprilData->numIntervalsFULL * 2, pFileFULL);
                if (elementsWritten != aprilData->numIntervalsFULL * 2) {
                    logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing FULL intervals failed for object with ID", recID);
                    return DBERR_DISK_WRITE_FAILED;
                }
            }

            return DBERR_OK;
        }

        DB_STATUS saveAPRILForDataset(FILE* pFileALL, FILE* pFileFULL, Dataset &dataset) {
            for (auto &secIT : dataset.sectionMap) {
                for (auto &objIT : secIT.second.aprilData) {
                    // ALL intervals
                    size_t elementsWritten = fwrite(&objIT.first, sizeof(size_t), 1, pFileALL);
                    if (elementsWritten != 1) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID failed for object with ID", objIT.first);
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    // buffered write for section id and numIntervals
                    int buf[2];
                    buf[0] = secIT.first;
                    buf[1] = objIT.second.numIntervalsALL;
                    elementsWritten = fwrite(buf, sizeof(int), 2, pFileALL);
                    if (elementsWritten != 2) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing sectionID and numIntervalsALL failed for object with ID", objIT.first);
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    elementsWritten = fwrite(&objIT.second.intervalsALL.data()[0], sizeof(uint32_t), objIT.second.numIntervalsALL * 2, pFileALL);
                    if (elementsWritten != objIT.second.numIntervalsALL * 2) {
                        logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing ALL intervals failed for object with ID", objIT.first);
                        return DBERR_DISK_WRITE_FAILED;
                    }
                    // logger::log_success("Object:", recID, "saved", aprilData->numIntervalsALL * 2, "uint32_t elements for ALL intervals");
                    // FULL intervals (if any)
                    if(objIT.second.numIntervalsFULL > 0){
                        elementsWritten = fwrite(&objIT.first, sizeof(size_t), 1, pFileFULL);
                        if (elementsWritten != 1) {
                            logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing recID failed for object with ID", objIT.first);
                            return DBERR_DISK_WRITE_FAILED;
                        }
                        buf[1] = objIT.second.numIntervalsFULL;
                        elementsWritten = fwrite(buf, sizeof(int), 2, pFileFULL);
                        if (elementsWritten != 2) {
                            logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing sectionID and numIntervalsFULL failed for object with ID", objIT.first);
                            return DBERR_DISK_WRITE_FAILED;
                        }
                        elementsWritten = fwrite(&objIT.second.intervalsFULL.data()[0], sizeof(uint32_t), objIT.second.numIntervalsFULL * 2, pFileFULL);
                        if (elementsWritten != objIT.second.numIntervalsFULL * 2) {
                            logger::log_error(DBERR_DISK_WRITE_FAILED, "Writing FULL intervals failed for object with ID", objIT.first);
                            return DBERR_DISK_WRITE_FAILED;
                        }
                    }
                }
            }
            return DBERR_OK;
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
                dataset.addIntervalsToAprilData(sectionID, recID, numIntervals, intervals, ALL);
            }

CLOSE_AND_EXIT:
            fclose(pFile);
            return ret;
        }

        DB_STATUS loadAPRIL(Dataset &dataset) {
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