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
        }





        DB_STATUS loadDatasetMBRandAPRIL(spatial_lib::DatasetT &dataset) {
            


            return DBERR_OK;
        }

    }
}
