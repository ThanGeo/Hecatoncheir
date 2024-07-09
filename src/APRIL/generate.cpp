#include "APRIL/generate.h"

namespace APRIL
{
    // static DB_STATUS rasterizeNextPolygon(FILE* pFile, int &recID, spatial_lib::AprilDataT &aprilData) {
    //     DB_STATUS ret = DBERR_OK;
    //     //read objects
    //     rasterizerlib::polygon2d rasterizerPolygon;
    //     // get next object to rasterize
    //     ret = storage::reader::partitionFile::readNextObjectForRasterization(pFile, recID, rasterizerPolygon);
    //     if (ret != DBERR_OK) {
    //         return ret;
    //     }
    //     // generate APRIL
    //     aprilData = rasterizerlib::generate(rasterizerPolygon, rasterizerlib::GT_APRIL);
    //     // at least 1 ALL interval is needed for each polygon, otherwise there is an error
    //     if (aprilData.numIntervalsALL == 0) {
    //         logger::log_error(DBERR_APRIL_CREATE, "Failed to generate APRIL for polygon with ID", recID);
    //         ret = DBERR_APRIL_CREATE;
    //         return ret;
    //     }
    //     return ret;
    // }

    static DB_STATUS rasterizeNextObject(FILE* pFile, int &recID, spatial_lib::AprilDataT &aprilData) {
        DB_STATUS ret = DBERR_OK;
        //read objects
        rasterizerlib::linestring2d rasterizerLinestring;
        // get next object to rasterize
        ret = storage::reader::partitionFile::readNextObjectForRasterization(pFile, recID, rasterizerLinestring);
        if (ret != DBERR_OK) {
            return ret;
        }
        // generate APRIL
        aprilData = rasterizerlib::generate(rasterizerLinestring);
        // at least 1 ALL interval is needed for each polygon, otherwise there is an error
        if (aprilData.numIntervalsALL == 0) {
            logger::log_error(DBERR_APRIL_CREATE, "Failed to generate APRIL for polygon with ID", recID);
            ret = DBERR_APRIL_CREATE;
            return ret;
        }
        return ret;
    }

    namespace generation
    {
        static spatial_lib::MbrT rasterMBR;

        static DB_STATUS setRasterBounds(double xMin, double yMin, double xMax, double yMax) {
            if (xMin >= xMax || yMin >= yMax) {
                logger::log_error(DBERR_INVALID_PARAMETER, "Invalid raster bounds (min larger than max). Bounds:", xMin, yMin, xMax, yMax);
                return DBERR_INVALID_PARAMETER;
            }
            rasterMBR.minPoint.x = xMin;
            rasterMBR.minPoint.y = yMin;
            rasterMBR.maxPoint.x = xMax;
            rasterMBR.maxPoint.y = yMax;
            return DBERR_OK;
        }
    }

    DB_STATUS create(spatial_lib::DatasetT &dataset) {
        DB_STATUS ret = DBERR_OK;
        int recID;
        int objectsInFullFile = 0;
        // init raster environment
        ret = generation::setRasterBounds(dataset.dataspaceInfo.xMinGlobal, dataset.dataspaceInfo.yMinGlobal, dataset.dataspaceInfo.xMaxGlobal, dataset.dataspaceInfo.yMaxGlobal);
        if (ret != DBERR_OK) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Setting raster bounds failed");
            return DBERR_INVALID_PARAMETER;
        }
        // open dataset file
        FILE* pFile = fopen(dataset.path.c_str(), "rb");
        if (pFile == NULL) {
            logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset.path);
            return DBERR_MISSING_FILE;
        }
        // generate approximation filepaths
        ret = storage::generateAPRILFilePath(dataset);
        if (ret != DBERR_OK) {
            return ret;
        }
        // open april files for writing
        FILE* pFileALL = fopen(dataset.aprilConfig.ALL_intervals_path.c_str(), "wb");
        if (pFileALL == NULL) {
            logger::log_error(DBERR_MISSING_FILE, "Couldnt open APRIL ALL file:", dataset.aprilConfig.ALL_intervals_path);
            return DBERR_MISSING_FILE;
        }
        FILE* pFileFULL = fopen(dataset.aprilConfig.FULL_intervals_path.c_str(), "wb");
        if (pFileFULL == NULL) {
            logger::log_error(DBERR_MISSING_FILE, "Couldnt open APRIL FULL file:", dataset.aprilConfig.FULL_intervals_path);
            return DBERR_MISSING_FILE;
        }
        // next read the dataset info
        ret = storage::reader::partitionFile::loadDatasetInfo(pFile, dataset);
        if (ret != DBERR_OK) {
            goto CLOSE_AND_EXIT;
        }
        // write dummy value for object count. 
        fwrite(&dataset.totalObjects, sizeof(int), 1, pFileALL);
        // will replace with the actual value in the end, because some objects may not have FULL intervals
        fwrite(&dataset.totalObjects, sizeof(int), 1, pFileFULL);
        //read objects
        for(int i=0; i<dataset.totalObjects; i++){
            spatial_lib::AprilDataT aprilData;
            // rasterize
            ret = rasterizeNextObject(pFile, recID, aprilData);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "APRIL creation failed for object number", i);
                goto CLOSE_AND_EXIT;
            }



            // save on disk
            ret = APRIL::writer::saveAPRIL(pFileALL, pFileFULL, recID, 0, &aprilData);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while saving APRIL on disk");
                goto CLOSE_AND_EXIT;
            }
            // count objects
            if (aprilData.numIntervalsFULL > 0) {
                objectsInFullFile += 1;
            }
        }
        // update value 
        ret = storage::writer::updateObjectCountInFile(pFileFULL, objectsInFullFile);
        if (ret != DBERR_OK) {
            logger::log_error(ret, "Couldn't update object count value in FULL intervals file");
            goto CLOSE_AND_EXIT;
        }
CLOSE_AND_EXIT:
        fclose(pFile);
        fclose(pFileALL);
        fclose(pFileFULL);
        return ret;
    }
}