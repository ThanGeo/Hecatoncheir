#include "storage/read.h"

namespace storage
{   
    namespace reader
    {

        namespace partitionFile
        {
            DB_STATUS readNextObjectForRasterization(FILE* pFile, int &recID, std::vector<int> &partitionIDs, rasterizerlib::polygon2d &rasterizerPolygon) {
                int intBuf[2];
                // read recID and partition count
                size_t elementsRead = fread(&intBuf, sizeof(int), 2, pFile);
                if (elementsRead != 2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the recID + partitionCount");
                    return DBERR_DISK_READ_FAILED;
                }
                // rec ID
                recID = intBuf[0];
                // partitionCount
                int partitionCount = intBuf[1];
                // read partition IDs
                partitionIDs.resize(partitionCount);
                elementsRead = fread(partitionIDs.data(), sizeof(int), partitionCount, pFile);
                if (elementsRead != partitionCount) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the partition IDs");
                    return DBERR_DISK_READ_FAILED;
                }
                // read vertex count
                elementsRead = fread(&intBuf, sizeof(int), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the vertex count");
                    return DBERR_DISK_READ_FAILED;
                }
                int vertexCount = intBuf[0];
                // read the points
                std::vector<double> coords(vertexCount*2);
                elementsRead = fread(coords.data(), sizeof(double), vertexCount*2, pFile);
                if (elementsRead != vertexCount*2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the vertices");
                    return DBERR_DISK_READ_FAILED;
                }
                // create the object
                rasterizerPolygon = rasterizerlib::createPolygon(coords);
                
                return DBERR_OK;
            }


            DB_STATUS readNextObjectMBR(FILE* pFile, spatial_lib::PolygonT &polygon) {
                int intBuf[2];
                // read recID and partition count
                size_t elementsRead = fread(&intBuf, sizeof(int), 2, pFile);
                if (elementsRead != 2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the recID + partitionCount");
                    return DBERR_DISK_READ_FAILED;
                }
                // rec ID
                polygon.recID = intBuf[0];
                // partitionCount
                int partitionCount = intBuf[1];
                // read partition IDs
                polygon.partitionIDs.resize(partitionCount);
                elementsRead = fread(polygon.partitionIDs.data(), sizeof(int), partitionCount, pFile);
                if (elementsRead != partitionCount) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the partition IDs");
                    return DBERR_DISK_READ_FAILED;
                }
                // read vertex count
                elementsRead = fread(&intBuf, sizeof(int), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the vertex count");
                    return DBERR_DISK_READ_FAILED;
                }
                int vertexCount = intBuf[0];
                // read the points
                std::vector<double> coords(vertexCount*2);
                elementsRead = fread(coords.data(), sizeof(double), vertexCount*2, pFile);
                if (elementsRead != vertexCount*2) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the vertices");
                    return DBERR_DISK_READ_FAILED;
                }
                // create MBR
                polygon.mbr.minPoint.x = std::numeric_limits<int>::max();
                polygon.mbr.minPoint.y = std::numeric_limits<int>::max();
                polygon.mbr.maxPoint.x = -std::numeric_limits<int>::max();
                polygon.mbr.maxPoint.y = -std::numeric_limits<int>::max();
                for (int i=0; i<coords.size(); i+=2) {
                    polygon.mbr.minPoint.x = std::min(polygon.mbr.minPoint.x, coords[i]);
                    polygon.mbr.minPoint.y = std::min(polygon.mbr.minPoint.y, coords[i+1]);
                    polygon.mbr.maxPoint.x = std::max(polygon.mbr.maxPoint.x, coords[i]);
                    polygon.mbr.maxPoint.y = std::max(polygon.mbr.maxPoint.y, coords[i+1]);
                }
                
                return DBERR_OK;
            }

            DB_STATUS loadDatasetInfo(FILE* pFile, spatial_lib::DatasetT &dataset) {
                DB_STATUS ret = DBERR_OK;
                int length;
                // polygon count
                size_t elementsRead = fread(&dataset.totalObjects, sizeof(int), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the polygon count");
                    return DBERR_DISK_READ_FAILED;
                }
                // read data type
                elementsRead = fread(&dataset.dataType, sizeof(spatial_lib::DataTypeE), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's datatype");
                    return DBERR_DISK_READ_FAILED;
                }
                // read nickname length + string
                elementsRead = 0;
                elementsRead += fread(&length, sizeof(int), 1, pFile);
                elementsRead += fread(dataset.nickname.data(), length * sizeof(char), length, pFile);
                if (elementsRead != length + 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's nickname");
                    return DBERR_DISK_READ_FAILED;
                }
                // dataspace MBR
                elementsRead = 0;
                elementsRead += fread(&dataset.dataspaceInfo.xMinGlobal, sizeof(double), 1, pFile);
                elementsRead += fread(&dataset.dataspaceInfo.yMinGlobal, sizeof(double), 1, pFile);
                elementsRead += fread(&dataset.dataspaceInfo.xMaxGlobal, sizeof(double), 1, pFile);
                elementsRead += fread(&dataset.dataspaceInfo.yMaxGlobal, sizeof(double), 1, pFile);
                if (elementsRead != 4) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's dataspace MBR");
                    return DBERR_DISK_READ_FAILED;
                }
                // logger::log_success("loaded dataset info:", dataset.totalObjects, dataset.dataType, dataset.nickname, dataset.dataspaceInfo.xMinGlobal, dataset.dataspaceInfo.yMinGlobal, dataset.dataspaceInfo.xMaxGlobal, dataset.dataspaceInfo.yMaxGlobal);
                return ret;
            }

            DB_STATUS loadDatasetMBRs(spatial_lib::DatasetT &dataset) {
                DB_STATUS ret = DBERR_OK;
                int length = 0;
                // open partition file
                FILE* pFile = fopen(dataset.path.c_str(), "rb");
                if (pFile == NULL) {
                    logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset.path);
                    return DBERR_MISSING_FILE;
                }
                // dataset info
                ret = loadDatasetInfo(pFile, dataset);
                if (ret != DBERR_OK) {
                    logger::log_error(ret, "Failed to load dataset info");
                    goto CLOSE_AND_EXIT;
                }
                // read MBRs
                for (int i=0; i<dataset.totalObjects; i++) {
                    spatial_lib::PolygonT polygon;
                    // read next polygon
                    ret = readNextObjectMBR(pFile, polygon);
                    if (ret != DBERR_OK) {
                        logger::log_error(ret, "Failed to read MBR for object number", i, "of", dataset.totalObjects);
                        goto CLOSE_AND_EXIT;
                    }
                    // store in dataset
                    dataset.addPolygon(polygon);
                }
CLOSE_AND_EXIT:
                fclose(pFile);
                return ret;
            }
        }


    }
}
