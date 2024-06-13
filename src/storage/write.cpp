#include "storage/write.h"

namespace storage
{
    namespace writer
    {
        DB_STATUS appendDatasetInfoToPartitionFile(FILE* outFile, spatial_lib::DatasetT* dataset) {
            // datatype
            fwrite(&dataset->dataType, sizeof(spatial_lib::DataTypeE), 1, outFile);
            // nikcname length + string
            int length = dataset->nickname.length();
            fwrite(&length, sizeof(int), 1, outFile);
            fwrite(dataset->nickname.data(), length * sizeof(char), 1, outFile);
            // dataspace MBR
            fwrite(&dataset->dataspaceInfo.xMinGlobal, sizeof(double), 1, outFile);
            fwrite(&dataset->dataspaceInfo.yMinGlobal, sizeof(double), 1, outFile);
            fwrite(&dataset->dataspaceInfo.xMaxGlobal, sizeof(double), 1, outFile);
            fwrite(&dataset->dataspaceInfo.yMaxGlobal, sizeof(double), 1, outFile);

            return DBERR_OK;
        }

        DB_STATUS appendBatchToPartitionFile(FILE* outFile, GeometryBatchT* batch, spatial_lib::DatasetT* dataset) {
            // store each geometry
            for(auto &it: batch->geometries) {
                fwrite(&it.recID, sizeof(int), 1, outFile);
                fwrite(&it.partitionCount, sizeof(int), 1, outFile);
                fwrite(it.partitionIDs.data(), sizeof(int), it.partitionCount, outFile);
                fwrite(&it.vertexCount, sizeof(int), 1, outFile);
                fwrite(it.coords.data(), sizeof(double), it.coords.size(), outFile);
            }

            // append batch count to dataset count
            dataset->totalObjects += batch->objectCount;

            return DBERR_OK;
        }

        DB_STATUS updateObjectCountInPartitionFile(FILE* outFile, int objectCount) {
            // keep current position
            long currentPos = ftell(outFile);
            // go to begining of file
            fseek(outFile, 0, SEEK_SET);
            fwrite(&objectCount, sizeof(objectCount), 1, outFile);
            // return to the original position
            fseek(outFile, currentPos, SEEK_SET);
            return DBERR_OK;
        }


    }
}