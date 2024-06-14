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

            DB_STATUS loadDatasetMBRs(spatial_lib::DatasetT &dataset) {
                DB_STATUS ret = DBERR_OK;
                int polygonCount = 0;
                int length = 0;
                // open partition file
                FILE* pFile = fopen(dataset.path.c_str(), "rb");
                if (pFile == NULL) {
                    logger::log_error(DBERR_MISSING_FILE, "Could not open partitioned dataset file from path:", dataset.path);
                    return DBERR_MISSING_FILE;
                }
                // polygon count
                size_t elementsRead = fread(&polygonCount, sizeof(int), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the polygon count");
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                // read data type
                elementsRead = fread(&dataset.dataType, sizeof(spatial_lib::DataTypeE), 1, pFile);
                if (elementsRead != 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's datatype");
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                // read nickname length + string
                elementsRead = 0;
                elementsRead += fread(&length, sizeof(int), 1, pFile);
                elementsRead += fread(dataset.nickname.data(), length * sizeof(char), length, pFile);
                if (elementsRead != length + 1) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's nickname");
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                // dataspace MBR
                elementsRead = 0;
                elementsRead += fread(&dataset.dataspaceInfo.xMinGlobal, sizeof(double), 1, pFile);
                elementsRead += fread(&dataset.dataspaceInfo.yMinGlobal, sizeof(double), 1, pFile);
                elementsRead += fread(&dataset.dataspaceInfo.xMaxGlobal, sizeof(double), 1, pFile);
                elementsRead += fread(&dataset.dataspaceInfo.yMaxGlobal, sizeof(double), 1, pFile);
                if (elementsRead != 4) {
                    logger::log_error(DBERR_DISK_READ_FAILED, "Couldn't read the dataset's dataspace MBR");
                    ret = DBERR_DISK_READ_FAILED;
                    goto CLOSE_AND_EXIT;
                }
                // read MBRs
                for (int i=0; i<polygonCount; i++) {
                    spatial_lib::PolygonT polygon;
                    // read next polygon
                    ret = readNextObjectMBR(pFile, polygon);
                    if (ret != DBERR_OK) {
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
