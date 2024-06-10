#include "APRIL/generate.h"

namespace APRIL
{

    DB_STATUS generate(spatial_lib::DatasetT &dataset) {
        DB_STATUS ret = DBERR_OK;
        int polygonCount = 0, recID, partitionID, vertexCount;
        double xMin, yMin, xMax, yMax;
        double coordLoadSpace[1000000];
        // init rasterizer environment
        int rasterizerRet = rasterizerlib::init(dataset.dataspaceInfo.xMinGlobal, dataset.dataspaceInfo.yMinGlobal, dataset.dataspaceInfo.xMaxGlobal, dataset.dataspaceInfo.yMaxGlobal);
        if (!rasterizerRet) {
            logger::log_error(DBERR_INVALID_PARAMETER, "Rasterizer init failed");
            return DBERR_INVALID_PARAMETER;
        }

        // open dataset file stream
        FILE* pFile = fopen(dataset.path.c_str(), "rb");
        if (pFile == NULL) {
            logger::log_error(DBERR_MISSING_FILE, "Couldnt open binary dataset file:", dataset.path);
            return DBERR_MISSING_FILE;
        }
        // generate approximation filepaths
        ret = storage::generateApproximationFilePath(dataset);
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
            return DBERR_DISK_READ_FAILED;
        }
        // write dummy value for polygon count. 
        // will replace with the actual value in the end
        fwrite(&polygonCount, sizeof(int), 1, pFileALL);
        fwrite(&polygonCount, sizeof(int), 1, pFileFULL);

        logger::log_success("Polygon count: ", polygonCount);

        //read polygons
        int lastRecID;
        for(int i=0; i<polygonCount; i++){
            rasterizerlib::polygon2d rasterizerPolygon;
            // get next object
            ret = storage::reader::partitionFile::readNextObjectForRasterization(pFile, recID, partitionID, rasterizerPolygon);
            if (ret != DBERR_OK) {
                logger::log_error(ret, "Failed while reading polygon number",i,"from partition file");
                return ret;
            }

            // generate april only if it hasn't already (partition files contain duplicates)
            if (recID != lastRecID) {
                // generate april
                spatial_lib::AprilDataT aprilData = rasterizerlib::generate(rasterizerPolygon, rasterizerlib::GT_APRIL);
                
                // logger::log_success("Generated APRIL for object id", recID);
            }


            // keep the object ID that was read last
            lastRecID == recID;
        }

        fclose(pFile);
        fclose(pFileALL);
        fclose(pFileFULL);

        return ret;
    }
}