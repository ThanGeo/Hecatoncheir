#include "storage/read.h"

namespace storage
{   
    namespace reader
    {
        double coordLoadSpace[1000000];

        namespace partitionFile
        {
            DB_STATUS readNextObjectForRasterization(FILE* pFile, int &recID, int &partitionID, rasterizerlib::polygon2d &rasterizerPolygon) {
                // int intBuf[3];
                // size_t elementsRead = fread(&intBuf, sizeof(int), 3, pFile);
                // if (elementsRead != 3) {
                //     return DBERR_DISK_READ_FAILED;
                // }
                // // object info
                // recID = intBuf[0];
                // partitionID = intBuf[1];
                // int vertexCount = intBuf[2];
                // // read the points
                // elementsRead = fread(&coordLoadSpace, sizeof(double), vertexCount*2, pFile);
                // if (elementsRead != vertexCount*2) {
                //     return DBERR_DISK_READ_FAILED;
                // }
                // // create the object
                // rasterizerPolygon = rasterizerlib::createPolygon(coordLoadSpace, vertexCount);
                
                return DBERR_OK;
            }
        }

    }
}
