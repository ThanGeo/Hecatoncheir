#ifndef D_STORAGE_READ_H
#define D_STORAGE_READ_H

#include "def.h"
#include "env/partitioning.h"

namespace storage
{
    namespace reader
    {
        namespace partitionFile
        {
            /**
             * @brief reads the next object from an opened partition file and returns the rasterizer polygon object, that will be used in the rasterizer library
             *  to create april
             */
            // DB_STATUS readNextObjectForRasterization(FILE* pFile, int &recID, rasterizerlib::polygon2d &rasterizerPolygon);
            // DB_STATUS readNextObjectForRasterization(FILE* pFile, int &recID, rasterizerlib::linestring2d &rasterizerLinestring);

            template<typename T>
            DB_STATUS readNextObject(FILE* pFile, int &recID, spatial_lib::Shape<T> &object) {
                std::vector<int> partitions;
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
                partitions.resize(partitionCount*2);
                elementsRead = fread(partitions.data(), sizeof(int), partitionCount*2, pFile);
                if (elementsRead != partitionCount*2) {
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
                object.mbr.minPoint.x = std::numeric_limits<int>::max();
                object.mbr.minPoint.y = std::numeric_limits<int>::max();
                object.mbr.maxPoint.x = -std::numeric_limits<int>::max();
                object.mbr.maxPoint.y = -std::numeric_limits<int>::max();
                for (auto it = coords.begin(); it != coords.end(); it+=2) {
                    object.addPoint(*it, *(it+1));
                    object.mbr.minPoint.x = std::min(object.mbr.minPoint.x, *(it));
                    object.mbr.minPoint.y = std::min(object.mbr.minPoint.y, *(it+1));
                    object.mbr.maxPoint.x = std::max(object.mbr.maxPoint.x, *(it));
                    object.mbr.maxPoint.y = std::max(object.mbr.maxPoint.y, *(it+1));
                }
                return DBERR_OK;
            }

            /**
             * @brief reads the next object from an opened partition file and fills the polygon struct with the following information:
             * recID
             * MBR
             * partition IDs
             * @warning it does not load the actual geometry in memory
             */
            DB_STATUS readNextObjectMBR(FILE* pFile, spatial_lib::PolygonT &polygon);
            
            /**
             * @brief loads a dataset's MBRs in-memory, to use in query processing.
             * Creates the local index based on the objects' partitions
             */
            DB_STATUS loadDatasetMBRs(spatial_lib::DatasetT &dataset);

            /**
             * @brief loads a dataset (vector and MBR) in-memory, to use in query processing.
             * Creates the local index based on the objects' partitions
             */
            DB_STATUS loadDatasetComplete(spatial_lib::DatasetT &dataset);

            /**
             * loads dataset info from the partition file. The pfile pos must be in the begining
             * loads: polygonCount, dataset nickname, dataspace MBR
             */
            DB_STATUS loadDatasetInfo(FILE* pFile, spatial_lib::DatasetT &dataset);
        }


    }
}


#endif