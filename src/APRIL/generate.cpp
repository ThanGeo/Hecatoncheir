#include "APRIL/generate.h"

namespace APRIL
{

    DB_STATUS generate(spatial_lib::DatasetT &dataset) {
        DB_STATUS ret = DBERR_OK;
        int recID, vertexCount;
        std::vector<int> partitionIDs;
        double xMin, yMin, xMax, yMax;
        double coordLoadSpace[1000000];
        int objectsInFullFile = 0;
        // init rasterizer environment
        int rasterizerRet = rasterizerlib::init(dataset.dataspaceInfo.xMinGlobal, dataset.dataspaceInfo.yMinGlobal, dataset.dataspaceInfo.xMaxGlobal, dataset.dataspaceInfo.yMaxGlobal);
        if (!rasterizerRet) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Rasterizer init failed");
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

        // write dummy value for polygon count. 
        fwrite(&dataset.totalObjects, sizeof(int), 1, pFileALL);
        // will replace with the actual value in the end, because some objects may not have FULL intervals
        fwrite(&dataset.totalObjects, sizeof(int), 1, pFileFULL);
        //read polygons
        for(int i=0; i<dataset.totalObjects; i++){
            rasterizerlib::polygon2d rasterizerPolygon;
            // get next object to rasterize
            ret = storage::reader::partitionFile::readNextObjectForRasterization(pFile, recID, partitionIDs, rasterizerPolygon);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while reading polygon number",i,"from partition file");
                goto CLOSE_AND_EXIT;
            }
            // generate APRIL
            spatial_lib::AprilDataT aprilData = rasterizerlib::generate(rasterizerPolygon, rasterizerlib::GT_APRIL);
            // at least 1 ALL interval is needed for each polygon, otherwise there is an error
            if (aprilData.numIntervalsALL == 0) {
                logger::log_error(DBERR_APRIL_CREATE, "Failed to generate APRIL for polygon with ID", recID);
                ret = DBERR_APRIL_CREATE;
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