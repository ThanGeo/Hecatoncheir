#include "APRIL/generate.h"

namespace APRIL
{

    DB_STATUS generate(spatial_lib::DatasetT &dataset) {
        DB_STATUS ret = DBERR_OK;
        int polygonCount = 0, recID, vertexCount;
        std::vector<int> partitionIDs;
        double xMin, yMin, xMax, yMax;
        double coordLoadSpace[1000000];
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

        //first read the total polygon count of the dataset
        size_t elementsRead = fread(&polygonCount, sizeof(int), 1, pFile);
        if (elementsRead != 1) {
            ret = DBERR_DISK_READ_FAILED;
            goto CLOSE_AND_EXIT;
        }
        // write dummy value for polygon count. 
        // will replace with the actual value in the end
        fwrite(&polygonCount, sizeof(int), 1, pFileALL);
        fwrite(&polygonCount, sizeof(int), 1, pFileFULL);
        //read polygons
        for(int i=0; i<polygonCount; i++){
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
        }
CLOSE_AND_EXIT:
        fclose(pFile);
        fclose(pFileALL);
        fclose(pFileFULL);
        return ret;
    }
}